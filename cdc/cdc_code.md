Question – CDC + Deduplication + SCD Type 2

Suppose your Oracle CDC extraction returns these records for the same customer in one five-minute batch:
Customer ID	City	Operation	Commit timestamp
101	Kolkata	INSERT	10:00
101	Delhi	UPDATE	10:01
101	Mumbai	UPDATE	10:02
101	Bangalore	UPDATE	10:03
101	Pune	UPDATE	10:04

Your target Snowflake table maintains SCD Type 2 history.

Question:

Would you use ROW_NUMBER() to retain only the latest record for CUSTOMER_ID 101, or would you process all five records?

Explain:
1. Which records would you retain?
2. How would you update the target table?
3. What should the final historical records look like?

_________________________________________________________________________________________________________________________________________________

BEGIN

    FOR rec IN (
        SELECT *
        FROM CDC_STAGING
        ORDER BY CUSTOMER_ID, COMMIT_TIMESTAMP
    )
    DO

        -- 1. Close the existing current record
        UPDATE CUSTOMER_HISTORY
        SET
            END_DATE = rec.COMMIT_TIMESTAMP,
            IS_CURRENT = FALSE
        WHERE CUSTOMER_ID = rec.CUSTOMER_ID
          AND IS_CURRENT = TRUE;

        -- 2. Insert a new record for INSERT or UPDATE
        IF (rec.OPERATION != 'DELETE') THEN

            INSERT INTO CUSTOMER_HISTORY (
                CUSTOMER_ID,
                CITY,
                START_DATE,
                END_DATE,
                IS_CURRENT,
                IS_DELETED,
                SOURCE_EVENT_ID
            )
            VALUES (
                rec.CUSTOMER_ID,
                rec.CITY,
                rec.COMMIT_TIMESTAMP,
                NULL,
                TRUE,
                FALSE,
                rec.EVENT_ID
            );

        END IF;

    END FOR;

END;
___________________________________________________________________________________________________________________________________________________________

# Question 2 – CDC + Deduplication + SCD Type 2

Correct answer: Process all five records, not just the latest record.

Because the target is an SCD Type 2 table, we must preserve every meaningful change to the customer's city.

If we use `ROW_NUMBER()` to retain only the latest event, we would keep Pune and lose the history of Kolkata, Delhi, Mumbai and Bangalore.

However, there is an important distinction:

- We can use `ROW_NUMBER()` to remove duplicate copies of the same CDC event.
- We must not use it to discard different legitimate changes belonging to the same customer.

## 1. Which records would you retain?

All five records, ordered by commit timestamp.

| Customer ID | City      | Operation | Timestamp |
| ----------- | --------- | --------- | --------- |
| 101         | Kolkata   | INSERT    | 10:00     |
| 101         | Delhi     | UPDATE    | 10:01     |
| 101         | Mumbai    | UPDATE    | 10:02     |
| 101         | Bangalore | UPDATE    | 10:03     |
| 101         | Pune      | UPDATE    | 10:04     |

For example, if the Kolkata event appears twice because of a retry, we remove the duplicate using its CDC event identity, such as source transaction/LSN and event sequence, or another stable unique event identifier.

We do not remove the Delhi, Mumbai or Bangalore events because they represent genuine changes.

## 2. How would I update the Snowflake target?

Assume the target table has these columns:

- `CUSTOMER_ID`
- `CITY`
- `START_DATE`
- `END_DATE`
- `IS_CURRENT`

The processing logic is:

1. Read CDC events in source commit order.
2. Deduplicate repeated copies of the same event.
3. For each event, find the customer's current record.
4. Expire that record by setting its `END_DATE` to the incoming event timestamp and `IS_CURRENT = FALSE`.
5. For INSERT or UPDATE, insert a new current version.
6. For DELETE, expire the current version but do not insert another active version.

### Snowflake target table

For a practical implementation, I would keep a `SOURCE_EVENT_ID` to identify which CDC event created each historical version, and an `IS_DELETED` flag to record deletes.

```
CREATE TABLE CUSTOMER_HISTORY (
    CUSTOMER_ID      NUMBER,
    CITY             VARCHAR(100),
    START_DATE       TIMESTAMP_NTZ,
    END_DATE         TIMESTAMP_NTZ,
    IS_CURRENT       BOOLEAN,
    IS_DELETED       BOOLEAN,
    SOURCE_EVENT_ID  VARCHAR(200)
);
```

The staging table is assumed to contain:

```
CUSTOMER_ID
CITY
OPERATION
COMMIT_TIMESTAMP
EVENT_ID
EVENT_SEQUENCE
```

`EVENT_ID` must uniquely identify a source change and remain the same if that event is delivered again. `EVENT_SEQUENCE` provides ordering when two events have the same commit timestamp.

### Processing SQL

First, deduplicate by event identity, not by customer ID. Then process the events in source order.

```
BEGIN TRANSACTION;

BEGIN
    FOR rec IN (
        SELECT
            CUSTOMER_ID,
            CITY,
            OPERATION,
            COMMIT_TIMESTAMP,
            EVENT_ID
        FROM CDC_STAGING
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY EVENT_ID
            ORDER BY EVENT_SEQUENCE DESC
        ) = 1
        ORDER BY CUSTOMER_ID, COMMIT_TIMESTAMP, EVENT_SEQUENCE
    )
    DO

        -- Skip events already processed
        IF (
            NOT EXISTS (
                SELECT 1
                FROM CUSTOMER_HISTORY
                WHERE SOURCE_EVENT_ID = rec.EVENT_ID
            )
        ) THEN

            -- Expire the existing current version
            UPDATE CUSTOMER_HISTORY
            SET
                END_DATE = rec.COMMIT_TIMESTAMP,
                IS_CURRENT = FALSE
            WHERE CUSTOMER_ID = rec.CUSTOMER_ID
              AND IS_CURRENT = TRUE;

            -- Insert the new version
            -- A DELETE creates a deletion marker (tombstone)
            INSERT INTO CUSTOMER_HISTORY (
                CUSTOMER_ID,
                CITY,
                START_DATE,
                END_DATE,
                IS_CURRENT,
                IS_DELETED,
                SOURCE_EVENT_ID
            )
            VALUES (
                rec.CUSTOMER_ID,
                rec.CITY,
                rec.COMMIT_TIMESTAMP,
                NULL,
                TRUE,
                IFF(rec.OPERATION = 'DELETE', TRUE, FALSE),
                rec.EVENT_ID
            );

        END IF;

    END FOR;

    COMMIT;

EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        RAISE;
END;
```

Why insert a deletion marker?

When a customer is deleted, the previous version is expired and a new version with `IS_DELETED = TRUE` records that the customer is deleted. This is a common soft-delete approach to maintaining deletion history.

A query for active customers would filter:

```
SELECT *
FROM CUSTOMER_HISTORY
WHERE IS_CURRENT = TRUE
  AND IS_DELETED = FALSE;
```

This example assumes the batch is processed in source order, the target starts with at most one current version per customer, and the events are not late-arriving. Production deployments also need to serialize or otherwise coordinate concurrent processing for the same customer, and handle late events using source ordering rather than blindly applying arrival order.

Snowflake Scripting variable syntax correction: Inside SQL statements, reference loop variables using `:rec.COLUMN_NAME`. The processing block above should use this syntax in its `WHERE`, `SET` and `VALUES` clauses.

## 3. What should the final historical records look like?

For the exact five records in your question, the expected result is:

| Customer ID | City      | Start | End   | Current | Deleted |
| ----------- | --------- | ----- | ----- | ------- | ------- |
| 101         | Kolkata   | 10:00 | 10:01 | FALSE   | FALSE   |
| 101         | Delhi     | 10:01 | 10:02 | FALSE   | FALSE   |
| 101         | Mumbai    | 10:02 | 10:03 | FALSE   | FALSE   |
| 101         | Bangalore | 10:03 | 10:04 | FALSE   | FALSE   |
| 101         | Pune      | 10:04 | NULL  | TRUE    | FALSE   |

There are five versions, four expired versions and one current version.

If a DELETE arrives at 10:05, the result becomes:

| Customer ID | City      | Start | End   | Current | Deleted |
| ----------- | --------- | ----- | ----- | ------- | ------- |
| 101         | Kolkata   | 10:00 | 10:01 | FALSE   | FALSE   |
| 101         | Delhi     | 10:01 | 10:02 | FALSE   | FALSE   |
| 101         | Mumbai    | 10:02 | 10:03 | FALSE   | FALSE   |
| 101         | Bangalore | 10:03 | 10:04 | FALSE   | FALSE   |
| 101         | Pune      | 10:04 | 10:05 | FALSE   | FALSE   |
| 101         | Pune      | 10:05 | NULL  | TRUE    | TRUE    |

The last row is a deletion marker, not an active customer.

## 4. What would I tell the interviewer?

> I would process all five CDC events because the target maintains SCD Type 2 history. I would not use ROW_NUMBER to retain only the latest record per customer, because that would discard intermediate changes.
>
> I would use event-level deduplication based on a stable source event identifier, then process the events in commit order.
>
> For every INSERT or UPDATE, I would expire the existing current record and insert a new version. For DELETE, I would expire the current version and record the deletion using a deletion marker, depending on the target's SCD design.
>
> I would also make the processing idempotent so that retrying a batch does not create duplicate historical records.

### 5. Important concepts to remember

| Concept                     | Purpose                                 |
| --------------------------- | --------------------------------------- |
| CDC event deduplication     | Prevent processing the same event twice |
| Commit timestamp + sequence | Preserve source change order            |
| SCD Type 2                  | Preserve historical versions            |
| `START_DATE`                | When a version became valid             |
| `END_DATE`                  | When a version stopped being valid      |
| `IS_CURRENT`                | Identify the latest version             |
| `IS_DELETED`                | Identify a deleted entity               |
| Idempotency                 | Safely retry failed batches             |

The most important distinction for your interview:

`ROW_NUMBER() PARTITION BY CUSTOMER_ID` to keep only the latest event is appropriate for building a current-state snapshot. It is not appropriate for preserving all five changes in an SCD Type 2 history table.

And remember: the SQL above illustrates the processing approach. A production implementation must additionally coordinate concurrent writes, handle late-arriving events, and ensure reliable event identity and ordering.

____________________________________________________________________________________________________________________________________________________________


A normal MERGE is not enough for SCD Type 2 when multiple changes for the same customer arrive in one batch.
Why?
Suppose the batch contains:
- 10:00 – Kolkata (INSERT)
- 10:01 – Delhi (UPDATE)
- 10:02 – Mumbai (UPDATE)
- 10:03 – Bangalore (UPDATE)
- 10:04 – Pune (UPDATE)
If we use ROW_NUMBER() to keep only the latest record and then MERGE, we will retain only Pune. The intermediate history will be lost.

For this example, the simpler approach is to process every event sequentially.
Why did I use INSERT instead of MERGE?
Because SCD Type 2 requires two operations for every change:
1. UPDATE the previous current record to expire it.
2. INSERT a new historical version.
A normal MERGE can perform both actions for a matched or unmatched row, but it becomes more complicated when multiple source events match the same customer.
What should you say in an interview?
MERGE can be used for SCD Type 2 implementation, but I would not directly MERGE multiple CDC events for the same customer without first handling their ordering and history. I need to expire the previous version and insert a new version for every meaningful change. For multiple changes in one batch, I would process the events in source order or use a carefully designed set-based SCD Type 2 transformation.

Remember: MERGE is a SQL operation. SCD Type 2 is a data-modelling and processing strategy. They are not competing approaches.