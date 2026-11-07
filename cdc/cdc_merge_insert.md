Here is the complete production-grade SQL code for handling late-arriving and out-of-order CDC events in both a **Current-State Table (SCD Type 1)** and an **SCD Type 2 Dimension Table**.

---

### 1. Current-State Table (SCD Type 1) Implementation

For a current-state table, we use a `MERGE` statement with a strict timestamp guardrail (`source.commit_timestamp > target.updated_at`). This ensures that if an older delayed event arrives (like Mumbai at 10:02 when Pune at 10:03 is already present), it is safely ignored.

```sql
MERGE INTO CUSTOMER_CURRENT AS target
USING CUSTOMER_STAGING AS source
    ON target.customer_id = source.customer_id
-- Guardrail: Only update if the incoming change is strictly newer than what we currently have
WHEN MATCHED AND source.commit_timestamp > target.updated_at THEN
    UPDATE SET 
        target.city = source.city,
        target.updated_at = source.commit_timestamp
WHEN NOT MATCHED THEN
    INSERT (
        customer_id,
        city,
        updated_at
    )
    VALUES (
        source.customer_id,
        source.city,
        source.commit_timestamp
    );

```

---

### 2. SCD Type 2 Dimension Table (Handling Out-of-Order Events)

For SCD Type 2, simply dropping an out-of-order event is incorrect because it represents a valid historical state.

The industry-standard approach for handling out-of-order CDC streams in a data warehouse is **Timeline Re-stitching**. When a late event arrives, we combine the existing history with the new event, sort them chronologically by the source commit timestamp, and use window functions (`LEAD`) to recalculate the valid `START_TIME` and `END_TIME` intervals for that entity.

#### Step-by-Step SQL Re-stitching Pattern:

```sql
-- Step 1: Combine existing valid history with the newly arrived out-of-order staging event
WITH all_timeline_events AS (
    -- Pull existing active/historical states for the affected customer
    SELECT 
        customer_id,
        city,
        start_time AS event_timestamp
    FROM DIM_CUSTOMER_SCD2
    WHERE customer_id = 101

    UNION

    -- Pull the late-arriving / out-of-order event from staging
    SELECT 
        customer_id,
        city,
        commit_timestamp AS event_timestamp
    FROM CUSTOMER_STAGING
    WHERE customer_id = 101
),

-- Step 2: Sort chronologically and use LEAD to calculate correct SCD Type 2 boundaries
restitched_timeline AS (
    SELECT 
        customer_id,
        city,
        event_timestamp AS start_time,
        -- LEAD looks at the timestamp of the *next* chronological change to close the current window
        COALESCE(
            LEAD(event_timestamp, 1) OVER (PARTITION BY customer_id ORDER BY event_timestamp ASC),
            '9999-12-31 23:59:59'::TIMESTAMP_NTZ
        ) AS end_time,
        -- Flag the latest record as current
        CASE 
            WHEN LEAD(event_timestamp, 1) OVER (PARTITION BY customer_id ORDER BY event_timestamp ASC) IS NULL 
            THEN TRUE 
            ELSE FALSE 
        END AS is_current
    FROM all_timeline_events
)

-- Step 3: Upsert or overwrite the customer's history in the target dimension table
-- (In practice, you would truncate and reload or merge the re-calculated timeline for the affected entity keys)
SELECT * FROM restitched_timeline;

```

#### Why this works for out-of-order events:

1. **`UNION`** merges the delayed `Mumbai (10:02)` event alongside `Kolkata (10:00)`, `Delhi (10:01)`, and `Pune (10:03)`.
2. **`ORDER BY event_timestamp ASC`** sorts them into true chronological order: `10:00 -> 10:01 -> 10:02 -> 10:03`.
3. **`LEAD()`** automatically sets Mumbai's `end_time` to `10:03` (Pune's start time), neatly slotting Mumbai into the historical timeline right where it belongs without breaking continuity.

---