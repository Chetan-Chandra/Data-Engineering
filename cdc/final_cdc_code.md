###################################   dbt incremental model — dim_customer.sql  ######################################################
__________________________________________________________________________________________________________
Source configuration — sources.yml:
__________________________________________________________________________________________________________
version: 2

sources:
  - name: cdc_raw
    database: CDC_DB
    schema: CDC_RAW

    tables:
      - name: customer_cdc_stage
      
__________________________________________________________________________________________________________
models/marts/dim_customer.sql:
__________________________________________________________________________________________________________

{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'EVENT_ID',
        on_schema_change = 'sync_all_columns'
    )
}}

-- =========================================================
-- STEP 1: Deduplicate incoming CDC events
-- =========================================================

WITH DEDUPLICATED_STAGE AS (

    SELECT *
    FROM {{ source('cdc_raw', 'customer_cdc_stage') }}

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY EVENT_ID
        ORDER BY UPDATED_AT DESC
    ) = 1

),


-- =========================================================
-- STEP 2: Identify genuinely new events
-- =========================================================

NEW_EVENTS AS (

    SELECT S.*
    FROM DEDUPLICATED_STAGE S

    {% if is_incremental() %}

    WHERE NOT EXISTS (

        SELECT 1
        FROM {{ this }} T
        WHERE T.EVENT_ID = S.EVENT_ID

    )

    {% endif %}

),


-- =========================================================
-- STEP 3: Identify affected customers
-- =========================================================

AFFECTED_CUSTOMERS AS (

    SELECT DISTINCT CUSTOMER_ID
    FROM NEW_EVENTS

),


-- =========================================================
-- STEP 4: Combine existing history and new events
-- =========================================================

ALL_EVENTS AS (

    {% if is_incremental() %}

    -- Existing history for affected customers

    SELECT
        T.CUSTOMER_ID,
        T.CITY,
        T.VALID_FROM AS EVENT_TIME,
        T.EVENT_ID

    FROM {{ this }} T

    INNER JOIN AFFECTED_CUSTOMERS A
        ON T.CUSTOMER_ID = A.CUSTOMER_ID

    UNION ALL

    {% endif %}

    -- New incoming events

    SELECT
        CUSTOMER_ID,
        CITY,
        UPDATED_AT AS EVENT_TIME,
        EVENT_ID

    FROM NEW_EVENTS

),


-- =========================================================
-- STEP 5: Reconstruct complete SCD Type 2 timeline
-- =========================================================

CALCULATED_TIMELINE AS (

    SELECT
        CUSTOMER_ID,
        CITY,

        EVENT_TIME AS VALID_FROM,

        LEAD(EVENT_TIME) OVER (
            PARTITION BY CUSTOMER_ID
            ORDER BY EVENT_TIME, EVENT_ID
        ) AS VALID_TO,

        EVENT_ID

    FROM ALL_EVENTS

)


-- =========================================================
-- STEP 6: Return records for dbt MERGE
-- =========================================================

SELECT
    CUSTOMER_ID,
    CITY,
    VALID_FROM,
    VALID_TO,
    (VALID_TO IS NULL) AS IS_CURRENT,
    EVENT_ID

FROM CALCULATED_TIMELINE



###################################   Snowflake  ################################################################
-- =========================================================
-- STEP 1: Remove duplicate events from staging
-- =========================================================

CREATE OR REPLACE TEMPORARY TABLE DEDUPLICATED_STAGE AS
SELECT *
FROM CDC_DB.CDC_RAW.CUSTOMER_CDC_STAGE
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY EVENT_ID
    ORDER BY UPDATED_AT DESC
) = 1;


-- =========================================================
-- STEP 2: Identify genuinely new events
-- =========================================================

CREATE OR REPLACE TEMPORARY TABLE NEW_EVENTS AS
SELECT S.*
FROM DEDUPLICATED_STAGE S
WHERE NOT EXISTS (
    SELECT 1
    FROM CDC_DB.CDC_RAW.DIM_CUSTOMER T
    WHERE T.EVENT_ID = S.EVENT_ID
);


-- =========================================================
-- STEP 3: Combine existing history and new events
-- =========================================================

CREATE OR REPLACE TEMPORARY TABLE ALL_EVENTS AS

SELECT
    CUSTOMER_ID,
    CITY,
    VALID_FROM AS EVENT_TIME,
    EVENT_ID
FROM CDC_DB.CDC_RAW.DIM_CUSTOMER

UNION ALL

SELECT
    CUSTOMER_ID,
    CITY,
    UPDATED_AT AS EVENT_TIME,
    EVENT_ID
FROM NEW_EVENTS;


-- =========================================================
-- STEP 4: Reconstruct the complete timeline using LEAD()
-- =========================================================

CREATE OR REPLACE TEMPORARY TABLE CALCULATED_TIMELINE AS

SELECT
    CUSTOMER_ID,
    CITY,
    EVENT_TIME AS VALID_FROM,

    LEAD(EVENT_TIME) OVER (
        PARTITION BY CUSTOMER_ID
        ORDER BY EVENT_TIME, EVENT_ID
    ) AS VALID_TO,

    EVENT_ID

FROM ALL_EVENTS;


-- =========================================================
-- STEP 5: Begin transaction for target-table DML
-- =========================================================

BEGIN TRANSACTION;


-- =========================================================
-- STEP 6: Update existing historical records
-- =========================================================

UPDATE CDC_DB.CDC_RAW.DIM_CUSTOMER T

SET
    VALID_TO = C.VALID_TO,
    IS_CURRENT = (C.VALID_TO IS NULL)

FROM CALCULATED_TIMELINE C

WHERE T.EVENT_ID = C.EVENT_ID;


-- =========================================================
-- STEP 7: Insert genuinely new events
-- =========================================================

INSERT INTO CDC_DB.CDC_RAW.DIM_CUSTOMER
(
    CUSTOMER_ID,
    CITY,
    VALID_FROM,
    VALID_TO,
    IS_CURRENT,
    EVENT_ID
)

SELECT
    C.CUSTOMER_ID,
    C.CITY,
    C.VALID_FROM,
    C.VALID_TO,
    (C.VALID_TO IS NULL),
    C.EVENT_ID

FROM CALCULATED_TIMELINE C

INNER JOIN NEW_EVENTS N
    ON C.EVENT_ID = N.EVENT_ID;


-- =========================================================
-- STEP 8: Commit transaction
-- =========================================================

COMMIT;



###################################   Bigquery  ################################################################
BEGIN TRANSACTION;

-- Step 1: Remove duplicate events from staging
CREATE TEMP TABLE DEDUPLICATED_STAGE AS
SELECT *
FROM `electric-tesla-507710-k2.cdc_raw.customer_cdc_stage`
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY EVENT_ID
    ORDER BY UPDATED_AT DESC
) = 1;


-- Step 2: Identify genuinely new events
CREATE TEMP TABLE NEW_EVENTS AS
SELECT S.*
FROM DEDUPLICATED_STAGE S
WHERE NOT EXISTS (
    SELECT 1
    FROM `electric-tesla-507710-k2.cdc_raw.dim_customer` T
    WHERE T.EVENT_ID = S.EVENT_ID
);


-- Step 3: Combine existing history and new events
CREATE TEMP TABLE ALL_EVENTS AS
SELECT
    CUSTOMER_ID,
    CITY,
    VALID_FROM AS EVENT_TIME,
    EVENT_ID
FROM `electric-tesla-507710-k2.cdc_raw.dim_customer`

UNION ALL

SELECT
    CUSTOMER_ID,
    CITY,
    UPDATED_AT AS EVENT_TIME,
    EVENT_ID
FROM NEW_EVENTS;


-- Step 4: Reconstruct the complete timeline
CREATE TEMP TABLE CALCULATED_TIMELINE AS
SELECT
    CUSTOMER_ID,
    CITY,
    EVENT_TIME AS VALID_FROM,

    LEAD(EVENT_TIME) OVER (
        PARTITION BY CUSTOMER_ID
        ORDER BY EVENT_TIME, EVENT_ID
    ) AS VALID_TO,

    EVENT_ID
FROM ALL_EVENTS;


-- Step 5: Update existing historical records
UPDATE `electric-tesla-507710-k2.cdc_raw.dim_customer` T
SET
    VALID_TO = C.VALID_TO,
    IS_CURRENT = C.VALID_TO IS NULL
FROM CALCULATED_TIMELINE C
WHERE T.EVENT_ID = C.EVENT_ID;


-- Step 6: Insert only genuinely new events
INSERT INTO `electric-tesla-507710-k2.cdc_raw.dim_customer`
(
    CUSTOMER_ID,
    CITY,
    VALID_FROM,
    VALID_TO,
    IS_CURRENT,
    EVENT_ID
)
SELECT
    C.CUSTOMER_ID,
    C.CITY,
    C.VALID_FROM,
    C.VALID_TO,
    C.VALID_TO IS NULL,
    C.EVENT_ID
FROM CALCULATED_TIMELINE C
JOIN NEW_EVENTS N
    ON C.EVENT_ID = N.EVENT_ID;

COMMIT TRANSACTION;