Let's solve the complete example
Starting target
CUSTOMER_ID	CITY	VALID_FROM	VALID_TO	IS_CURRENT
101	Kolkata	10:00	NULL	TRUE

Incoming CDC events
CUSTOMER_ID	CITY	UPDATED_AT
101	Bangalore	10:02
101	Mumbai	10:03
101	Pune	10:04

9.1 Calculate the validity periods
We use LEAD() on the incoming events.
SELECT
    CUSTOMER_ID,
    CITY,
    UPDATED_AT AS VALID_FROM,

    LEAD(UPDATED_AT) OVER (
        PARTITION BY CUSTOMER_ID
        ORDER BY UPDATED_AT
    ) AS VALID_TO

FROM CUSTOMER_CDC_STAGE;


Result:
CITY	VALID_FROM	VALID_TO
Bangalore	10:02	10:03
Mumbai	10:03	10:04
Pune	10:04	NULL

This calculates the correct validity periods for the incoming versions.
9.2 What happens to Kolkata?
Kolkata is the existing current record.

Its VALID_TO must become 10:02, because Bangalore starts at 10:02.

We expire Kolkata:
UPDATE DIM_CUSTOMER
SET
    VALID_TO = '10:02',
    IS_CURRENT = FALSE
WHERE CUSTOMER_ID = 101
  AND IS_CURRENT = TRUE;


9.3 Insert the incoming versions
We insert Bangalore, Mumbai and Pune with their calculated validity periods.

The final result:
CUSTOMER_ID	CITY	VALID_FROM	VALID_TO	IS_CURRENT
101	Kolkata	10:00	10:02	FALSE
101	Bangalore	10:02	10:03	FALSE
101	Mumbai	10:03	10:04	FALSE
101	Pune	10:04	NULL	TRUE

Expected SCD Type 2 result
Step 10 — Generalise the SQL (without hardcoding 10:02)
In an actual pipeline, we don't know beforehand that the first incoming event will be at 10:02.

We calculate it dynamically.
A. Find the first incoming event for each customer
SELECT
    CUSTOMER_ID,
    MIN(UPDATED_AT) AS FIRST_CHANGE_TIME
FROM CUSTOMER_CDC_STAGE
GROUP BY CUSTOMER_ID;


For our example:
CUSTOMER_ID	FIRST_CHANGE_TIME
101	10:02

B. Expire the existing current record
UPDATE DIM_CUSTOMER T
SET
    VALID_TO = S.FIRST_CHANGE_TIME,
    IS_CURRENT = FALSE
FROM (
    SELECT
        CUSTOMER_ID,
        MIN(UPDATED_AT) AS FIRST_CHANGE_TIME
    FROM CUSTOMER_CDC_STAGE
    GROUP BY CUSTOMER_ID
) S
WHERE T.CUSTOMER_ID = S.CUSTOMER_ID
  AND T.IS_CURRENT = TRUE;


Now Kolkata expires at 10:02.
C. Insert all incoming versions
INSERT INTO DIM_CUSTOMER
(
    CUSTOMER_ID,
    CITY,
    VALID_FROM,
    VALID_TO,
    IS_CURRENT
)
SELECT
    CUSTOMER_ID,
    CITY,
    UPDATED_AT AS VALID_FROM,

    LEAD(UPDATED_AT) OVER (
        PARTITION BY CUSTOMER_ID
        ORDER BY UPDATED_AT
    ) AS VALID_TO,

    CASE
        WHEN LEAD(UPDATED_AT) OVER (
            PARTITION BY CUSTOMER_ID
            ORDER BY UPDATED_AT
        ) IS NULL THEN TRUE
        ELSE FALSE
    END AS IS_CURRENT

FROM CUSTOMER_CDC_STAGE;


This is a simplified implementation for a batch containing new, chronological events. It assumes the incoming events are later than the existing current version.

Production requirements still to add:
- Deduplicate CDC events using a stable event identifier such as source LSN/sequence.
- Prevent previously processed events from being inserted again.
- Handle equal timestamps with a deterministic source sequence.
- Execute expiry and insertion atomically.
- Handle late-arriving events separately.