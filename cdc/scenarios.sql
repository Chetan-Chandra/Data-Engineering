-- 1. Create a temporary table with sample login dates
CREATE TABLE #TestLogins (
    customer_id INT,
    login_date DATE
);

-- 2. Insert test data (Customer 1 has a 3-day streak; Customer 2 has a gap)
INSERT INTO #TestLogins (customer_id, login_date) VALUES
(1, '2026-06-01'),
(1, '2026-06-02'),
(1, '2026-06-03'), 
(2, '2026-06-01'),
(2, '2026-06-03'); 

-- 3. Run the Gaps-and-Islands Query (T-SQL Syntax)
WITH 
d AS (
    SELECT DISTINCT customer_id, login_date 
    FROM #TestLogins
),
g AS (
    SELECT customer_id, login_date,
           DATEADD(day, -ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY login_date), login_date) AS grp
    FROM d
)
SELECT customer_id, MIN(login_date) AS streak_start, MAX(login_date) AS streak_end, COUNT(*) AS days
FROM g 
GROUP BY customer_id, grp 
HAVING COUNT(*) >= 3;

--__________________________________________________________________________________________________________________________________________

WITH 
d AS (
    SELECT DISTINCT CustomerKey AS customer_id, CAST(OrderDate AS DATE) AS login_date
    FROM dbo.FactInternetSales
),
g AS (
    SELECT customer_id, login_date,
           -- T-SQL syntax for DATEADD uses unquoted 'day'
           DATEADD(day, -ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY login_date), login_date) AS grp
    FROM d
)
SELECT customer_id, MIN(login_date) AS streak_start, MAX(login_date) AS streak_end, COUNT(*) AS days
FROM g 
GROUP BY customer_id, grp 
HAVING COUNT(*) >= 3
ORDER BY days DESC;

-- 4. Clean up
DROP TABLE #TestLogins;
--_________________________________________________________________________________________________________________________________________________

-- 1. Create a temporary table for user clicks
CREATE TABLE #UserClicks (
    user_id INT,
    click_time DATETIME2
);

-- 2. Insert dummy click data for User 101 and User 102
-- User 101 has TWO distinct sessions (with a 45-minute gap in the middle)
-- User 102 has ONE session (all clicks are within a few minutes of each other)
INSERT INTO #UserClicks (user_id, click_time) VALUES
-- User 101 Session 1
(101, '2026-06-01 10:00:00'),
(101, '2026-06-01 10:05:00'), -- +5 mins (Same session)
(101, '2026-06-01 10:15:00'), -- +10 mins (Same session)
-- 45-minute gap here! This should trigger a new session:
(101, '2026-06-01 11:00:00'), -- +45 mins from previous click (New Session!)
(101, '2026-06-01 11:02:00'), -- +2 mins (Same session)

-- User 102 Session 1
(102, '2026-06-01 14:00:00'),
(102, '2026-06-01 14:10:00'), -- +10 mins (Same session)
(102, '2026-06-01 14:20:00'); -- +10 mins (Same session)

---------------------------------------------------------
-- 3. The Sessionization Query (T-SQL Adapted Version)
---------------------------------------------------------
WITH time_diffs AS (
    SELECT 
        user_id,
        click_time,
        -- Get the timestamp of the previous click for the same user
        LAG(click_time) OVER (PARTITION BY user_id ORDER BY click_time) AS prev_click
    FROM #UserClicks
),
flagged_sessions AS (
    SELECT 
        user_id,
        click_time,
        -- T-SQL uses DATEDIFF(minute, start, end). If gap > 30 mins or first row, flag as 1.
        CASE 
            WHEN prev_click IS NULL OR DATEDIFF(minute, prev_click, click_time) > 30 THEN 1 
            ELSE 0 
        END AS is_new_session
    FROM time_diffs
),
session_numbered AS (
    SELECT 
        user_id,
        click_time,
        -- Cumulative sum of flags creates an incrementing session ID per user
        SUM(is_new_session) OVER (
            PARTITION BY user_id 
            ORDER BY click_time 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS session_id
    FROM flagged_sessions
)
SELECT 
    user_id,
    session_id,
    MIN(click_time) AS session_start,
    MAX(click_time) AS session_end,
    COUNT(*) AS click_count
FROM session_numbered
GROUP BY user_id, session_id;

-- 4. Clean up temporary table
DROP TABLE #UserClicks;


SELECT 
        user_id,
        click_time,
        -- Cumulative sum of flags creates an incrementing session ID per user
        SUM(is_new_session) OVER (
            PARTITION BY user_id 
            ORDER BY click_time 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS session_id
    FROM (
SELECT 
        user_id,
        click_time,
        -- T-SQL uses DATEDIFF(minute, start, end). If gap > 30 mins or first row, flag as 1.
        CASE 
            WHEN prev_click IS NULL OR DATEDIFF(minute, prev_click, click_time) > 30 THEN 1 
            ELSE 0 
        END AS is_new_session
    FROM (SELECT 
        user_id,
        click_time,
        -- Get the timestamp of the previous click for the same user
        LAG(click_time) OVER (PARTITION BY user_id ORDER BY click_time) AS prev_click
    FROM #UserClicks) as time_diffs) as session_id;
--______________________________________________________________________________________________________________________________________

-- 1. Create a temporary SCD Type 2 dimension table
CREATE TABLE #DIM_CUSTOMER_SCD2 (
    customer_id INT,
    city VARCHAR(50),
    valid_from DATE,
    valid_to DATE,
    is_current BIT
);

-- 2. Insert the INITIAL flawed state:
-- We originally recorded that Customer 101 lived in New York starting June 1st.
INSERT INTO #DIM_CUSTOMER_SCD2 (customer_id, city, valid_from, valid_to, is_current) VALUES
(101, 'New York', '2026-06-01', NULL, 1);

PRINT '--- INITIAL TABLE STATE (Flawed History) ---';
SELECT 
    customer_id, 
    city, 
    valid_from, 
    ISNULL(CAST(valid_to AS VARCHAR), 'Present') AS valid_to, 
    is_current 
FROM #DIM_CUSTOMER_SCD2;

--- INITIAL TABLE STATE (Flawed History) ---
customer_id		  city		valid_from		valid_to	is_current
	101			New York	2026-06-01		Present			1

---------------------------------------------------------
-- 3. THE RETROACTIVE CORRECTION (Late-Arriving Data)
-- We discover: Customer 101 was actually in Chicago from June 1st to June 10th,
-- and only moved to New York on June 10th.
---------------------------------------------------------

-- Step A: Update the existing New York record so its valid_from is pushed to June 10th,
-- because New York wasn't active until June 10th!
UPDATE #DIM_CUSTOMER_SCD2
SET valid_from = '2026-06-10'
WHERE customer_id = 101 
  AND city = 'New York';

--- AFTER UPDATE TABLE STATE (Fixed History) ---
customer_id		  city		valid_from		valid_to	is_current
	101			New York	2026-06-10		Present			1

-- Step B: Insert the retroactive Chicago record into the past slot (June 1st to June 10th)
INSERT INTO #DIM_CUSTOMER_SCD2 (customer_id, city, valid_from, valid_to, is_current)
VALUES (
    101, 
    'Chicago', 
    '2026-06-01', -- Started on June 1st
    '2026-06-10', -- Ended when New York took over on June 10th
    0             -- Historical record, not current
);

---------------------------------------------------------
-- 4. VERIFY FINAL TABLE STATE
---------------------------------------------------------
PRINT '--- FINAL SPLIT TIMELINE STATE ---';
SELECT 
    customer_id,
    city,
    valid_from,
    ISNULL(CAST(valid_to AS VARCHAR), 'Present') AS valid_to,
    is_current
FROM #DIM_CUSTOMER_SCD2
ORDER BY valid_from ASC;

--- FINAL SPLIT TIMELINE STATE ---
customer_id		 city		valid_from		valid_to	is_current
	101			Chicago		2026-06-01		2026-06-10		0
	101			New York	2026-06-10		Present			1

-- Clean up
DROP TABLE #DIM_CUSTOMER_SCD2;