Your existing SOP is already a strong foundation. However, to make it genuinely suitable for a Senior Data Engineer / Lead Data Engineer (L2/L3) working in an enterprise production environment, I would expand it in several important areas.

The biggest architectural improvement is to establish a proper multi-layer Snowflake data processing architecture rather than treating RAW, STAGING, CORE and MARTS as interchangeable concepts.

I would also make AWS S3 and Google Cloud Storage (GCS) interchangeable ingestion-storage options, so the same engineering framework can support AWS-based, GCP-based and hybrid enterprise environments.

# Major improvements in the revised SOP

1. Expanded enterprise architecture

Major architectural improvement- GCS and S3 as alternative raw object-storage platforms.
- Snowflake multi-layer architecture.
- Separate ingestion, processing, modeling and consumption responsibilities.
- Tableau and Power BI integration.

2. Additional Snowflake engineering

- Advanced SQL and window functions.
- Dynamic Tables, Streams and Tasks.
- Snowflake stored procedures and Snowpark.
- Advanced incremental loading.
- Query Profile and cost optimization.
- Data sharing and governance.

3. Advanced CDC and data modeling

- Transaction log ordering.
- Event deduplication.
- SCD Type 1, 2 and 6.
- Late-arriving dimensions.
- Fact table corrections.
- Hard deletes and tombstones.
- Historical timeline reconstruction.

4. Production engineering

- Idempotency.
- Watermark management.
- Data contracts.
- Schema evolution.
- Dead-letter processing.
- Reconciliation and observability.
- Disaster recovery.

5. Advanced development

- Python ingestion framework.
- PySpark optimization.
- Kafka streaming.
- Airflow orchestration.
- dbt CI/CD.
- Terraform and deployment automation.

Below is the expanded master SOP, organized as a technical implementation handbook.

Writing

# SENIOR DATA ENGINEER – ENTERPRISE MASTER SOP

## Production Implementation, Architecture, Coding, Optimization and Interview Handbook

**Technology Stack:** Snowflake | dbt Core | Advanced SQL | Python | PySpark | Apache Spark | Apache Kafka | Airflow | AWS S3 | Google Cloud Storage | AWS Glue | GCP Data Services | CDC | Tableau | Power BI | Terraform | GitHub Actions

**Target Role:** Senior Data Engineer / Lead Data Engineer / Data Platform Engineer

**Experience Level:** Intermediate to Advanced / L2 / L3

**Document Purpose:** Enterprise production implementation handbook, architecture reference, coding guide, troubleshooting manual and interview preparation reference.

---

# PART 1 – ENTERPRISE DATA ENGINEERING ARCHITECTURE

## 1.1 Overall Enterprise Architecture

The proposed enterprise architecture supports two object-storage alternatives:

- AWS S3 for AWS-based environments.
- Google Cloud Storage (GCS) for GCP-based environments.

Both can serve as durable raw-data storage before data is ingested into Snowflake.

The downstream Snowflake processing architecture remains substantially the same.

### Reference architecture

text

```text
                 SOURCE SYSTEMS
  Oracle | SQL Server | PostgreSQL | MySQL
      ERP | CRM | SaaS | REST APIs | Files
                       |
              +--------+--------+
              |                 |
          BATCH INGESTION     CDC INGESTION
              |                 |
          Airflow/API       DMS/Debezium
              |                 |
              |               Kafka
              |                 |
              +--------+--------+
                       |
              OBJECT STORAGE LAYER
                       |
              +--------+--------+
              |                 |
           AWS S3              GCS
              |                 |
              +--------+--------+
                       |
              SNOWFLAKE INGESTION
                       |
              COPY INTO / SNOWPIPE
                       |
              SNOWFLAKE RAW LAYER
                       |
              SNOWFLAKE LANDING
                       |
              SNOWFLAKE PREPARE
                       |
             SNOWFLAKE NORMALIZED
                       |
             SNOWFLAKE SCHEMATIZED
                       |
             SNOWFLAKE MARKETPLACE
                       |
                SEMANTIC LAYER
                       |
              +--------+--------+
              |                 |
            TABLEAU          POWER BI
```

## 1.2 Important: Understanding the Snowflake Layers

The proposed layer names are useful as an enterprise naming convention. They should be implemented as clearly defined schemas with separate responsibilities.

| Layer | Purpose | Typical processing |
| --- | --- | --- |
| RAW | Preserve ingested source data | Minimal modification |
| LANDING | Standardize ingestion structure | Metadata, file and schema validation |
| PREPARE | Clean and validate records | Null checks, deduplication, type casting |
| NORMALIZED | Standardize business entities | Conformed entities, relationships, reusable models |
| SCHEMATIZED | Apply dimensional and analytical modeling | Facts, dimensions, SCD |
| MARKETPLACE | Publish business-ready datasets | Aggregates, data marts, governed datasets |
| SEMANTIC | Business definitions and measures | KPI definitions, calculations, relationships |
| BI | Reporting and visualization | Tableau, Power BI |

**Important architectural distinction:**

RAW and LANDING are not necessarily separate physical copies of the same data.

A recommended implementation is:

- RAW: Immutable source payloads and ingestion metadata.
- LANDING: Validated, typed and consistently structured source records.
- PREPARE: Cleaned and deduplicated records.
- NORMALIZED: Conformed business entities.
- SCHEMATIZED: Facts, dimensions and historical models.
- MARKETPLACE: Business-facing analytical datasets.
- SEMANTIC: Business metrics and consumption definitions.

Depending on volume and governance requirements, some layers can be views or incremental tables rather than separate copies.

## 1.3 Recommended Snowflake Database Structure

text

```text
ENTERPRISE_DW
│
├── RAW
│   ├── RAW_ORACLE_CUSTOMERS
│   ├── RAW_ORACLE_ORDERS
│   ├── RAW_KAFKA_EVENTS
│   └── RAW_API_RESPONSES
│
├── LANDING
│   ├── LND_CUSTOMERS
│   ├── LND_ORDERS
│   └── LND_CDC_EVENTS
│
├── PREPARE
│   ├── PREP_CUSTOMERS
│   ├── PREP_ORDERS
│   └── PREP_CDC_EVENTS
│
├── NORMALIZED
│   ├── CUSTOMER
│   ├── PRODUCT
│   ├── ORDER_HEADER
│   └── ORDER_DETAIL
│
├── SCHEMATIZED
│   ├── DIM_CUSTOMER
│   ├── DIM_PRODUCT
│   ├── DIM_DATE
│   ├── FACT_ORDERS
│   └── FACT_PAYMENTS
│
├── MARKETPLACE
│   ├── MART_SALES
│   ├── MART_CUSTOMER_360
│   └── MART_FINANCE
│
├── SEMANTIC
│   ├── VW_SALES_KPI
│   └── VW_CUSTOMER_METRICS
│
└── AUDIT
    ├── ETL_AUDIT_LOG
    ├── ETL_WATERMARK
    ├── REJECTED_RECORDS
    └── RECONCILIATION_LOG
```

### Schema creation

sql

```sql
CREATE DATABASE IF NOT EXISTS ENTERPRISE_DW;

CREATE SCHEMA IF NOT EXISTS ENTERPRISE_DW.RAW;
CREATE SCHEMA IF NOT EXISTS ENTERPRISE_DW.LANDING;
CREATE SCHEMA IF NOT EXISTS ENTERPRISE_DW.PREPARE;
CREATE SCHEMA IF NOT EXISTS ENTERPRISE_DW.NORMALIZED;
CREATE SCHEMA IF NOT EXISTS ENTERPRISE_DW.SCHEMATIZED;
CREATE SCHEMA IF NOT EXISTS ENTERPRISE_DW.MARKETPLACE;
CREATE SCHEMA IF NOT EXISTS ENTERPRISE_DW.SEMANTIC;
CREATE SCHEMA IF NOT EXISTS ENTERPRISE_DW.AUDIT;
```

## 1.4 AWS S3 vs Google Cloud Storage

Both platforms provide durable object storage.

| Feature | AWS S3 | Google Cloud Storage |
| --- | --- | --- |
| Storage | Object storage | Object storage |
| Storage path | s3:// | gs:// |
| Security | IAM and bucket policies | IAM and bucket permissions |
| Encryption | SSE-S3 / SSE-KMS | Google-managed / CMEK |
| Notifications | S3 Event Notifications | Pub/Sub notifications |
| ETL integration | Glue, EMR | Dataflow, Dataproc, Data Fusion |
| Snowflake integration | S3 storage integration | GCS storage integration |
| Data format | Parquet, JSON, CSV, Avro | Parquet, JSON, CSV, Avro |

The engineering principles remain the same.

### Option A: AWS S3 architecture

text

```text
Oracle / APIs / Kafka
        |
        v
AWS DMS / Airflow
        |
        v
Amazon S3
        |
        v
Snowflake External Stage
        |
        v
COPY INTO / Snowpipe
        |
        v
Snowflake RAW
```

Example storage:

text

```text
s3://enterprise-data/
    bronze/
    cdc/
    rejected/
    manifests/
    checkpoints/
```

### Option B: GCS architecture

text

```text
Oracle / APIs / Kafka
        |
        v
Airflow / Datastream / Dataflow
        |
        v
Google Cloud Storage
        |
        v
Snowflake External Stage
        |
        v
COPY INTO / Snowpipe
        |
        v
Snowflake RAW
```

Example storage:

text

```text
gs://enterprise-data/
    bronze/
    cdc/
    rejected/
    manifests/
    checkpoints/
```

### Recommendation

Use S3 when the enterprise primarily operates on AWS.

Use GCS when the enterprise primarily operates on Google Cloud.

Use either as the landing/object-storage platform when Snowflake is the central analytical warehouse.

A hybrid architecture is also possible, but cross-cloud networking, security, egress costs and operational complexity should be evaluated before implementing it.

---

# PART 2 – GCS AND S3 TO SNOWFLAKE IMPLEMENTATION

## 2.1 GCS Bucket Setup

Example bucket:

text

```text
gs://enterprise-data-lake/
```

Recommended folder structure:

text

```text
enterprise-data-lake/
│
├── bronze/
│   ├── customers/
│   ├── orders/
│   └── payments/
│
├── cdc/
│   ├── customers/
│   └── orders/
│
├── rejected/
│
├── manifests/
│
└── archive/
```

Recommended partitioning:

text

```text
bronze/orders/
    ingestion_date=2026-11-09/
        part-00001.parquet
        part-00002.parquet
```

Use ingestion date for operational partitioning and retain source event time as a separate column.

Do not partition every dataset by a high-cardinality identifier such as customer ID.

## 2.2 Configure GCS Permissions

Create a dedicated service identity for Snowflake external storage access.

The service identity should have only the permissions required for the bucket and prefix.

Typical responsibilities include:

- Listing permitted objects.
- Reading permitted objects.
- Accessing the required bucket metadata.
- Writing only if the chosen workflow requires it.

The exact IAM roles depend on the integration and Snowflake's generated service identity.

Avoid granting project-wide administrator permissions.

## 2.3 Create Snowflake GCS Storage Integration

sql

```sql
CREATE STORAGE INTEGRATION GCS_INTEGRATION
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = GCS
ENABLED = TRUE
STORAGE_ALLOWED_LOCATIONS = (
    'gcs://enterprise-data-lake/bronze/'
);
```

Inspect the integration:

sql

```sql
DESC INTEGRATION GCS_INTEGRATION;
```

Retrieve the generated Snowflake cloud-service identity information and configure the corresponding GCS bucket permissions according to the Snowflake integration setup procedure.

The cloud identity and bucket authorization must be configured before the stage can access objects.

## 2.4 Create GCS File Format

sql

```sql
CREATE OR REPLACE FILE FORMAT FF_PARQUET
TYPE = PARQUET;
```

For JSON:

sql

```sql
CREATE OR REPLACE FILE FORMAT FF_JSON
TYPE = JSON
STRIP_OUTER_ARRAY = TRUE;
```

For CSV:

sql

```sql
CREATE OR REPLACE FILE FORMAT FF_CSV
TYPE = CSV
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
NULL_IF = ('NULL', 'null', '')
EMPTY_FIELD_AS_NULL = TRUE
ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE;
```

## 2.5 Create External Stage for GCS

sql

```sql
CREATE OR REPLACE STAGE GCS_ORDERS_STAGE
URL = 'gcs://enterprise-data-lake/bronze/orders/'
STORAGE_INTEGRATION = GCS_INTEGRATION
FILE_FORMAT = FF_PARQUET;
```

Validate access:

sql

```sql
LIST @GCS_ORDERS_STAGE;
```

## 2.6 Load GCS Data into Snowflake RAW

Assume the Parquet source contains:

- ORDER\_ID
- CUSTOMER\_ID
- ORDER\_DATE
- ORDER\_AMOUNT
- STATUS

Create a raw table:

sql

```sql
CREATE TABLE IF NOT EXISTS RAW.RAW_ORDERS (
    ORDER_ID NUMBER,
    CUSTOMER_ID NUMBER,
    ORDER_DATE TIMESTAMP_NTZ,
    ORDER_AMOUNT NUMBER(18,2),
    STATUS VARCHAR,

    SOURCE_FILE VARCHAR,
    INGESTED_AT TIMESTAMP_NTZ
);
```

Load data:

sql

```sql
COPY INTO RAW.RAW_ORDERS
(
    ORDER_ID,
    CUSTOMER_ID,
    ORDER_DATE,
    ORDER_AMOUNT,
    STATUS,
    SOURCE_FILE,
    INGESTED_AT
)
FROM
(
    SELECT
        $1:ORDER_ID::NUMBER,
        $1:CUSTOMER_ID::NUMBER,
        $1:ORDER_DATE::TIMESTAMP_NTZ,
        $1:ORDER_AMOUNT::NUMBER(18,2),
        $1:STATUS::VARCHAR,
        METADATA$FILENAME,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ

    FROM @GCS_ORDERS_STAGE
)
FILE_FORMAT = (FORMAT_NAME = FF_PARQUET)
ON_ERROR = 'CONTINUE';
```

The column extraction shown assumes the Parquet fields are exposed with these names and compatible types. Validate the actual file schema before deployment.

Inspect load history:

sql

```sql
SELECT *
FROM TABLE(
    INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'RAW.RAW_ORDERS',
        START_TIME => DATEADD('DAY', -1, CURRENT_TIMESTAMP())
    )
);
```

## 2.7 Snowpipe with GCS

Snowpipe can continuously load files from supported external cloud storage using cloud event notifications.

Conceptual architecture:

text

```text
Application
    |
    v
GCS Bucket
    |
    v
Cloud Pub/Sub Notification
    |
    v
Snowflake Notification Integration
    |
    v
Snowpipe
    |
    v
Snowflake RAW
```

Example:

sql

```sql
CREATE OR REPLACE PIPE GCS_ORDERS_PIPE
AUTO_INGEST = TRUE
INTEGRATION = GCS_NOTIFICATION_INTEGRATION
AS
COPY INTO RAW.RAW_ORDERS
FROM @GCS_ORDERS_STAGE
FILE_FORMAT = (FORMAT_NAME = FF_PARQUET);
```

The notification integration and cloud event subscription must be configured for the selected Snowflake-supported GCS ingestion architecture.

Use Snowpipe for continuous file ingestion. For direct low-latency event ingestion, evaluate Snowpipe Streaming or a Kafka integration.

## 2.8 AWS S3 Alternative

Create storage integration:

sql

```sql
CREATE STORAGE INTEGRATION S3_INTEGRATION
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_ALLOWED_LOCATIONS = (
    's3://enterprise-data/bronze/'
);
```

Inspect:

sql

```sql
DESC INTEGRATION S3_INTEGRATION;
```

Configure the AWS IAM trust relationship and bucket access using the Snowflake-generated identity information.

Create stage:

sql

```sql
CREATE OR REPLACE STAGE S3_ORDERS_STAGE
URL = 's3://enterprise-data/bronze/orders/'
STORAGE_INTEGRATION = S3_INTEGRATION
FILE_FORMAT = FF_PARQUET;
```

Load:

sql

```sql
COPY INTO RAW.RAW_ORDERS
FROM @S3_ORDERS_STAGE
FILE_FORMAT = (FORMAT_NAME = FF_PARQUET);
```

### Important implementation principle

Do not use AWS access keys or GCP service-account private keys directly in Python scripts, Airflow DAGs or SQL files.

Use supported cloud identities, integrations and managed secrets.

---

# PART 3 – SNOWFLAKE MULTI-LAYER TRANSFORMATION IMPLEMENTATION

This section defines the complete transformation journey after ingestion.

## 3.1 RAW Layer

Purpose:

- Preserve source information.
- Retain ingestion metadata.
- Support replay.
- Support auditing.
- Maintain source lineage.

Recommended metadata:

sql

```sql
CREATE TABLE RAW.RAW_CUSTOMER (
    CUSTOMER_ID NUMBER,
    CUSTOMER_NAME VARCHAR,
    EMAIL VARCHAR,
    CITY VARCHAR,

    SOURCE_SYSTEM VARCHAR,
    SOURCE_FILE VARCHAR,

    SOURCE_COMMIT_TS TIMESTAMP_NTZ,
    SOURCE_SEQUENCE VARCHAR,

    INGESTION_TS TIMESTAMP_NTZ,
    BATCH_ID VARCHAR
);
```

Avoid modifying source business attributes in this layer.

## 3.2 LANDING Layer

Purpose:

- Convert incoming data to a consistent structure.
- Validate basic schema compatibility.
- Attach ingestion metadata.
- Preserve traceability.

Example:

sql

```sql
CREATE OR REPLACE TABLE LANDING.LND_CUSTOMER AS

SELECT
    TRY_TO_NUMBER(CUSTOMER_ID) AS CUSTOMER_ID,

    NULLIF(TRIM(CUSTOMER_NAME), '') AS CUSTOMER_NAME,

    LOWER(TRIM(EMAIL)) AS EMAIL,

    UPPER(TRIM(CITY)) AS CITY,

    SOURCE_SYSTEM,
    SOURCE_FILE,
    SOURCE_COMMIT_TS,
    SOURCE_SEQUENCE,
    INGESTION_TS,
    BATCH_ID

FROM RAW.RAW_CUSTOMER;
```

For a large production dataset, use an incremental model or a view instead of rebuilding the entire table.

## 3.3 PREPARE Layer

Purpose:

- Deduplicate.
- Handle nulls.
- Standardize values.
- Validate data types.
- Identify invalid records.

Example:

sql

```sql
CREATE OR REPLACE TABLE PREPARE.PREP_CUSTOMER AS

WITH DEDUP AS (

    SELECT
        *,

        ROW_NUMBER() OVER (
            PARTITION BY SOURCE_SYSTEM, CUSTOMER_ID
            ORDER BY
                SOURCE_COMMIT_TS DESC,
                SOURCE_SEQUENCE DESC,
                INGESTION_TS DESC
        ) AS RN

    FROM LANDING.LND_CUSTOMER

    WHERE CUSTOMER_ID IS NOT NULL

)

SELECT
    CUSTOMER_ID,
    CUSTOMER_NAME,
    EMAIL,
    CITY,
    SOURCE_SYSTEM,
    SOURCE_COMMIT_TS,
    SOURCE_SEQUENCE,
    INGESTION_TS,
    BATCH_ID

FROM DEDUP

WHERE RN = 1;
```

This creates a latest-record representation. Use an event-level deduplication strategy separately when multiple valid CDC events per customer must be retained.

## 3.4 NORMALIZED Layer

Purpose:

- Establish consistent business entities.
- Remove unnecessary duplication.
- Separate related entities.
- Standardize business relationships.
- Create reusable entity models.

Example normalized entities:

text

```text
NORMALIZED.CUSTOMER
NORMALIZED.CUSTOMER_ADDRESS
NORMALIZED.PRODUCT
NORMALIZED.ORDER_HEADER
NORMALIZED.ORDER_DETAIL
NORMALIZED.PAYMENT
```

Example:

sql

```sql
CREATE OR REPLACE TABLE NORMALIZED.CUSTOMER AS

SELECT
    CUSTOMER_ID,
    CUSTOMER_NAME,
    EMAIL,
    CITY,
    SOURCE_SYSTEM

FROM PREPARE.PREP_CUSTOMER;
```

A normalized operational representation may use third-normal-form principles.

However, do not force every analytical warehouse model into a highly normalized design. The next layer can use dimensional modeling.

## 3.5 SCHEMATIZED Layer

Purpose:

- Create fact tables.
- Create dimension tables.
- Apply surrogate keys.
- Implement SCD.
- Define analytical grain.
- Create reusable dimensional structures.

Example dimension:

sql

```sql
CREATE TABLE IF NOT EXISTS SCHEMATIZED.DIM_CUSTOMER (
    CUSTOMER_SK NUMBER AUTOINCREMENT,
    CUSTOMER_ID NUMBER,
    CUSTOMER_NAME VARCHAR,
    EMAIL VARCHAR,
    CITY VARCHAR,

    EFFECTIVE_START_TS TIMESTAMP_NTZ,
    EFFECTIVE_END_TS TIMESTAMP_NTZ,

    IS_CURRENT BOOLEAN,
    RECORD_HASH VARCHAR
);
```

Example fact:

sql

```sql
CREATE TABLE IF NOT EXISTS SCHEMATIZED.FACT_ORDERS (
    ORDER_ID NUMBER,
    CUSTOMER_SK NUMBER,
    ORDER_DATE DATE,
    ORDER_AMOUNT NUMBER(18,2),
    ORDER_STATUS VARCHAR,
    LOAD_BATCH_ID VARCHAR
);
```

Define the fact grain explicitly:

**One row per order.**

If the source contains one record per order line, use one row per order line instead. Never mix grains without a deliberate design.

## 3.6 MARKETPLACE Layer

Purpose:

- Publish business-facing datasets.
- Aggregate facts.
- Apply approved business definitions.
- Create department-specific data products.
- Support BI and self-service analytics.

Example:

sql

```sql
CREATE OR REPLACE TABLE MARKETPLACE.MART_MONTHLY_SALES AS

SELECT
    DATE_TRUNC('MONTH', ORDER_DATE) AS SALES_MONTH,
    CUSTOMER_SK,
    COUNT(DISTINCT ORDER_ID) AS TOTAL_ORDERS,
    SUM(ORDER_AMOUNT) AS TOTAL_REVENUE,
    AVG(ORDER_AMOUNT) AS AVERAGE_ORDER_VALUE

FROM SCHEMATIZED.FACT_ORDERS

WHERE ORDER_STATUS = 'COMPLETED'

GROUP BY
    DATE_TRUNC('MONTH', ORDER_DATE),
    CUSTOMER_SK;
```

## 3.7 SEMANTIC Layer

Purpose:

- Define business KPIs.
- Standardize calculations.
- Provide consistent consumption views.
- Avoid different BI teams calculating the same metric differently.

Example:

sql

```sql
CREATE OR REPLACE VIEW SEMANTIC.VW_MONTHLY_SALES AS

SELECT
    SALES_MONTH,
    SUM(TOTAL_ORDERS) AS TOTAL_ORDERS,
    SUM(TOTAL_REVENUE) AS TOTAL_REVENUE,

    SUM(TOTAL_REVENUE)
        / NULLIF(SUM(TOTAL_ORDERS), 0)
        AS AVERAGE_ORDER_VALUE

FROM MARKETPLACE.MART_MONTHLY_SALES

GROUP BY SALES_MONTH;
```

The semantic layer should be the governed location for approved KPI definitions, whether implemented in Snowflake views or a BI semantic model.

## 3.8 Tableau and Power BI Integration

Recommended connection:

text

```text
Snowflake MARKETPLACE / SEMANTIC
                  |
          Authentication
                  |
        +---------+---------+
        |                   |
      Tableau            Power BI
        |                   |
      Reports             Reports
      Dashboards          Dashboards
```

Recommended practices:

- Use a dedicated BI role.
- Grant SELECT only on approved consumption schemas.
- Avoid exposing raw PII.
- Use masking policies where required.
- Configure appropriate warehouse sizing.
- Prefer governed views for shared business logic.
- Validate refresh and concurrency requirements.

For Power BI, choose between Import and DirectQuery according to latency, data volume, model complexity and concurrency.

For Tableau, choose between live connections and extracts according to the reporting workload.

---

# PART 4 – ADVANCED SNOWFLAKE ENGINEERING

## 4.1 Advanced SQL Window Functions

### Latest record per business key

sql

```sql
SELECT *
FROM LANDING.LND_CUSTOMER

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CUSTOMER_ID
    ORDER BY
        SOURCE_COMMIT_TS DESC,
        SOURCE_SEQUENCE DESC
) = 1;
```

### Previous and current values

sql

```sql
SELECT
    CUSTOMER_ID,
    CITY,

    LAG(CITY) OVER (
        PARTITION BY CUSTOMER_ID
        ORDER BY SOURCE_COMMIT_TS, SOURCE_SEQUENCE
    ) AS PREVIOUS_CITY,

    LEAD(CITY) OVER (
        PARTITION BY CUSTOMER_ID
        ORDER BY SOURCE_COMMIT_TS, SOURCE_SEQUENCE
    ) AS NEXT_CITY

FROM RAW.RAW_CUSTOMER;
```

### Running revenue

sql

```sql
SELECT
    ORDER_DATE,
    ORDER_ID,
    ORDER_AMOUNT,

    SUM(ORDER_AMOUNT) OVER (
        ORDER BY ORDER_DATE, ORDER_ID
        ROWS BETWEEN UNBOUNDED PRECEDING
        AND CURRENT ROW
    ) AS RUNNING_REVENUE

FROM SCHEMATIZED.FACT_ORDERS;
```

### Rolling 30-day revenue

sql

```sql
SELECT
    ORDER_DATE,

    SUM(DAILY_REVENUE) OVER (
        ORDER BY ORDER_DATE
        RANGE BETWEEN INTERVAL '29 DAYS' PRECEDING
        AND CURRENT ROW
    ) AS ROLLING_30_DAY_REVENUE

FROM MARKETPLACE.DAILY_REVENUE;
```

For dates with missing days, build a calendar spine if the reporting requirement expects every calendar day.

## 4.2 Advanced MERGE

A MERGE source must contain at most one applicable row per target key.

sql

```sql
MERGE INTO NORMALIZED.CUSTOMER T

USING (

    SELECT *
    FROM PREPARE.PREP_CUSTOMER

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY CUSTOMER_ID
        ORDER BY
            SOURCE_COMMIT_TS DESC,
            SOURCE_SEQUENCE DESC
    ) = 1

) S

ON T.CUSTOMER_ID = S.CUSTOMER_ID

WHEN MATCHED
AND S.SOURCE_COMMIT_TS > T.SOURCE_COMMIT_TS

THEN UPDATE SET
    T.CUSTOMER_NAME = S.CUSTOMER_NAME,
    T.EMAIL = S.EMAIL,
    T.CITY = S.CITY,
    T.SOURCE_COMMIT_TS = S.SOURCE_COMMIT_TS,
    T.SOURCE_SEQUENCE = S.SOURCE_SEQUENCE

WHEN NOT MATCHED THEN

INSERT (
    CUSTOMER_ID,
    CUSTOMER_NAME,
    EMAIL,
    CITY,
    SOURCE_COMMIT_TS,
    SOURCE_SEQUENCE
)

VALUES (
    S.CUSTOMER_ID,
    S.CUSTOMER_NAME,
    S.EMAIL,
    S.CITY,
    S.SOURCE_COMMIT_TS,
    S.SOURCE_SEQUENCE
);
```

This example assumes that the source sequence and timestamp are comparable under a documented ordering rule. A production implementation should compare a complete source version, not just timestamps.

## 4.3 Snowflake Streams and Tasks

sql

```sql
CREATE OR REPLACE STREAM RAW_CUSTOMER_STREAM
ON TABLE RAW.RAW_CUSTOMER;
```

Create task:

sql

```sql
CREATE OR REPLACE TASK PROCESS_CUSTOMER_STREAM
WAREHOUSE = ETL_WH
SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA(
    'ENTERPRISE_DW.RAW.RAW_CUSTOMER_STREAM'
)
AS

MERGE INTO NORMALIZED.CUSTOMER T

USING (

    SELECT *
    FROM RAW_CUSTOMER_STREAM

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY CUSTOMER_ID
        ORDER BY
            SOURCE_COMMIT_TS DESC,
            SOURCE_SEQUENCE DESC
    ) = 1

) S

ON T.CUSTOMER_ID = S.CUSTOMER_ID

WHEN MATCHED THEN UPDATE SET
    T.CUSTOMER_NAME = S.CUSTOMER_NAME,
    T.CITY = S.CITY

WHEN NOT MATCHED THEN INSERT (
    CUSTOMER_ID,
    CUSTOMER_NAME,
    CITY
)
VALUES (
    S.CUSTOMER_ID,
    S.CUSTOMER_NAME,
    S.CITY
);
```

Enable:

sql

```sql
ALTER TASK PROCESS_CUSTOMER_STREAM RESUME;
```

This is an illustrative upsert task, not a complete CDC implementation. Add explicit delete handling, source-version checks, transaction-safe audit logging and monitoring.

## 4.4 Dynamic Tables

sql

```sql
CREATE OR REPLACE DYNAMIC TABLE
MARKETPLACE.DT_CUSTOMER_REVENUE

TARGET_LAG = '5 MINUTES'

WAREHOUSE = ETL_WH

AS

SELECT
    CUSTOMER_SK,
    SUM(ORDER_AMOUNT) AS TOTAL_REVENUE,
    COUNT(*) AS TOTAL_ORDERS

FROM SCHEMATIZED.FACT_ORDERS

GROUP BY CUSTOMER_SK;
```

Choose Dynamic Tables when declarative, managed refresh meets the workload's freshness and SQL requirements.

Use dbt incremental models when transformation deployment, version control and model-level orchestration are primary concerns.

## 4.5 Snowflake Query Optimization

Investigate the following:

1. Bytes scanned.
2. Partitions scanned.
3. Partitions total.
4. Local spilling.
5. Remote spilling.
6. Join cardinality.
7. Queue time.
8. Warehouse load.
9. Compilation time.

Example:

sql

```sql
SELECT
    QUERY_ID,
    QUERY_TEXT,
    EXECUTION_TIME,
    BYTES_SCANNED,
    PARTITIONS_SCANNED,
    PARTITIONS_TOTAL,
    QUEUED_OVERLOAD_TIME,
    BYTES_SPILLED_TO_LOCAL_STORAGE,
    BYTES_SPILLED_TO_REMOTE_STORAGE

FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY

WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())

ORDER BY EXECUTION_TIME DESC;
```

Optimize SQL before automatically increasing warehouse size.

---

# PART 5 – ADVANCED CDC AND SCD ENGINEERING

## 5.1 CDC Event Structure

A robust CDC event should contain:

json

```json
{
  "event_id": "evt-10001",
  "source_system": "ORACLE",
  "table_name": "CUSTOMER",
  "business_key": "101",
  "operation": "UPDATE",
  "source_commit_ts": "2026-11-09T10:30:00",
  "source_sequence": "000001250",
  "transaction_id": "TXN-9001",
  "before": {
    "city": "Kolkata"
  },
  "after": {
    "city": "Delhi"
  },
  "ingestion_ts": "2026-11-09T10:31:00"
}
```

For Oracle, capture a suitable log position such as an SCN or connector-specific source offset. For PostgreSQL, use the appropriate WAL/LSN metadata. These values are source-specific and should not be compared across unrelated sources.

## 5.2 Event Deduplication

sql

```sql
CREATE OR REPLACE TEMP TABLE CDC_CANONICAL AS

SELECT *

FROM RAW.RAW_CDC_EVENTS

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY SOURCE_SYSTEM, EVENT_ID
    ORDER BY INGESTION_TS DESC
) = 1;
```

Event-level deduplication must not remove distinct updates to the same customer.

## 5.3 SCD Type 1

Use when only the latest state matters.

sql

```sql
MERGE INTO SCHEMATIZED.DIM_CUSTOMER_CURRENT T

USING PREPARE.PREP_CUSTOMER S

ON T.CUSTOMER_ID = S.CUSTOMER_ID

WHEN MATCHED
AND S.SOURCE_SEQUENCE > T.SOURCE_SEQUENCE

THEN UPDATE SET
    T.CITY = S.CITY,
    T.EMAIL = S.EMAIL,
    T.SOURCE_SEQUENCE = S.SOURCE_SEQUENCE

WHEN NOT MATCHED THEN INSERT (
    CUSTOMER_ID,
    CITY,
    EMAIL,
    SOURCE_SEQUENCE
)

VALUES (
    S.CUSTOMER_ID,
    S.CITY,
    S.EMAIL,
    S.SOURCE_SEQUENCE
);
```

Use a proper source ordering representation rather than a simple string comparison if the sequence is not fixed-width or lexically sortable.

## 5.4 SCD Type 2 – Historical Timeline

Required columns:

text

```text
CUSTOMER_SK
CUSTOMER_ID
CITY
EMAIL
EFFECTIVE_START_TS
EFFECTIVE_END_TS
IS_CURRENT
RECORD_HASH
SOURCE_SEQUENCE
```

Use half-open validity intervals:

text

```text
effective_start <= business_timestamp < effective_end
```

For the current record, effective\_end is NULL.

### Change detection

sql

```sql
SELECT
    S.CUSTOMER_ID,
    S.CITY,
    S.EMAIL,

    MD5(
        CONCAT_WS(
            '|',
            COALESCE(S.CITY, '<NULL>'),
            COALESCE(S.EMAIL, '<NULL>')
        )
    ) AS RECORD_HASH

FROM PREPARE.PREP_CUSTOMER S;
```

In production, use unambiguous canonical serialization and explicit null handling to prevent hash collisions caused by delimiters or formatting.

### Identify changed records

sql

```sql
SELECT
    S.CUSTOMER_ID,
    S.CITY,
    S.EMAIL,
    S.RECORD_HASH,
    S.SOURCE_COMMIT_TS

FROM PREPARE.PREP_CUSTOMER S

JOIN SCHEMATIZED.DIM_CUSTOMER T
    ON S.CUSTOMER_ID = T.CUSTOMER_ID
   AND T.IS_CURRENT = TRUE

WHERE S.RECORD_HASH <> T.RECORD_HASH;
```

### Expire old records

sql

```sql
UPDATE SCHEMATIZED.DIM_CUSTOMER T

SET
    EFFECTIVE_END_TS = S.SOURCE_COMMIT_TS,
    IS_CURRENT = FALSE

FROM PREPARE.PREP_CUSTOMER S

WHERE T.CUSTOMER_ID = S.CUSTOMER_ID
  AND T.IS_CURRENT = TRUE
  AND T.RECORD_HASH <> S.RECORD_HASH;
```

### Insert new versions

sql

```sql
INSERT INTO SCHEMATIZED.DIM_CUSTOMER (
    CUSTOMER_ID,
    CITY,
    EMAIL,
    EFFECTIVE_START_TS,
    EFFECTIVE_END_TS,
    IS_CURRENT,
    RECORD_HASH
)

SELECT
    S.CUSTOMER_ID,
    S.CITY,
    S.EMAIL,
    S.SOURCE_COMMIT_TS,
    NULL,
    TRUE,
    S.RECORD_HASH

FROM PREPARE.PREP_CUSTOMER S

WHERE NOT EXISTS (

    SELECT 1

    FROM SCHEMATIZED.DIM_CUSTOMER T

    WHERE T.CUSTOMER_ID = S.CUSTOMER_ID
      AND T.IS_CURRENT = TRUE
);
```

The expire-and-insert operations must be executed as a controlled atomic unit.

This example handles current-state changes. A full historical SCD2 implementation must process every distinct source event in order and reconstruct affected timelines when late events arrive.

## 5.5 Late-Arriving CDC

Example:

| Event | Source time | Arrival |
| --- | --- | --- |
| A | 10:00 | 10:01 |
| B | 11:00 | 11:01 |
| C | 10:30 | 12:00 |

Correct historical sequence:

text

```text
10:00 - Kolkata
10:30 - Delhi
11:00 - Mumbai
```

Do not discard event C just because event B has already arrived.

Recommended strategy:

1. Persist the complete immutable CDC event log.
2. Identify affected business keys.
3. Retrieve their existing canonical history.
4. Merge newly received events.
5. Deduplicate by event identity.
6. Sort by source ordering metadata.
7. Remove no-op changes where appropriate.
8. Recalculate effective start and end timestamps.
9. Replace affected timelines atomically.
10. Validate history and fact relationships.

## 5.6 CDC Hard Deletes

Represent deletion explicitly.

sql

```sql
UPDATE SCHEMATIZED.DIM_CUSTOMER

SET
    IS_CURRENT = FALSE,
    EFFECTIVE_END_TS = :DELETE_TIMESTAMP

WHERE CUSTOMER_ID = :CUSTOMER_ID
  AND IS_CURRENT = TRUE;
```

Maintain a deletion ledger or tombstone with the source sequence.

An older update must never resurrect a record deleted by a newer event.

## 5.7 SCD Type 3

Type 3 preserves limited historical values.

Example:

sql

```sql
UPDATE DIM_CUSTOMER

SET
    PREVIOUS_CITY = CITY,
    CITY = :NEW_CITY

WHERE CUSTOMER_ID = :CUSTOMER_ID;
```

Use when only the previous and current attribute values are required.

## 5.8 SCD Type 6

SCD Type 6 combines Type 1, Type 2 and Type 3 characteristics.

Typical design:

- Historical versions retained.
- Current attributes available across historical rows.
- Previous attribute values maintained for selected fields.

This is useful for certain reporting requirements but increases complexity. Do not implement it unless the business requires those semantics.

---

# PART 6 – DBT ADVANCED ENGINEERING

## 6.1 Recommended dbt Architecture

text

```text
models/
│
├── staging/
│   ├── stg_oracle_customers.sql
│   ├── stg_oracle_orders.sql
│   └── stg_kafka_events.sql
│
├── prepare/
│   ├── prep_customer_dedup.sql
│   └── prep_order_validation.sql
│
├── normalized/
│   ├── int_customer.sql
│   └── int_order.sql
│
├── schematized/
│   ├── dim_customer.sql
│   ├── dim_product.sql
│   └── fct_orders.sql
│
├── marketplace/
│   ├── mart_sales.sql
│   └── mart_customer_360.sql
│
└── semantic/
    └── vw_business_kpis.sql
```

## 6.2 Advanced Incremental Model

sql

```sql
{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}

WITH SOURCE_DATA AS (

    SELECT *
    FROM {{ ref('stg_orders') }}

    {% if is_incremental() %}

    WHERE INGESTED_AT >= (

        SELECT COALESCE(
            DATEADD(HOUR, -12, MAX(INGESTED_AT)),
            '1900-01-01'::TIMESTAMP_NTZ
        )

        FROM {{ this }}

    )

    {% endif %}

),

DEDUP AS (

    SELECT *

    FROM SOURCE_DATA

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ORDER_ID
        ORDER BY
            SOURCE_COMMIT_TS DESC,
            SOURCE_SEQUENCE DESC
    ) = 1

)

SELECT *
FROM DEDUP;
```

A lookback is a recovery optimization, not a complete late-arrival guarantee.

For high-reliability processing, use a persisted source watermark, source offset, replay mechanism and periodic reconciliation.

## 6.3 dbt Model Contracts

Contracts help define expected output schemas.

yaml

```yaml
models:
  - name: dim_customer

    config:
      contract:
        enforced: true

    columns:
      - name: customer_id
        data_type: number

      - name: customer_name
        data_type: varchar

      - name: city
        data_type: varchar
```

Validate supported contract features against the installed dbt version and Snowflake adapter.

## 6.4 dbt Unit Tests

Example:

yaml

```yaml
unit_tests:
  - name: test_completed_order_amount
    model: fct_orders

    given:
      - input: ref('stg_orders')
        rows:
          - {
              order_id: 1,
              order_amount: 100,
              order_status: 'COMPLETED'
            }

    expect:
      rows:
        - {
            order_id: 1,
            order_amount: 100,
            order_status: 'COMPLETED'
          }
```

Unit tests validate transformation behavior using controlled input data.

They complement, rather than replace, integration and data-quality tests.

## 6.5 dbt Incremental Strategies

| Strategy | Usage |
| --- | --- |
| Append | Immutable event data |
| Merge | Upsert records |
| Delete + Insert | Replace affected business keys |
| Microbatch | Supported event-time batch processing |

Use incremental strategies based on source change semantics, not simply table size.

## 6.6 dbt CI/CD

Example:

bash

```bash
dbt deps
dbt parse
dbt compile
dbt build --select state:modified+
dbt source freshness
dbt docs generate
```

Use:

- Development target.
- Test target.
- Production target.
- State-based selection.
- Environment-specific variables.
- Protected deployment approvals.
- Version-controlled packages.

---

# PART 7 – PYTHON ADVANCED DATA ENGINEERING

## 7.1 Production Ingestion Framework

Recommended components:

text

```text
Python Ingestion Framework
│
├── config/
├── connectors/
├── extractors/
├── validators/
├── transformers/
├── loaders/
├── audit/
├── retry/
├── logging/
├── exceptions/
└── tests/
```

Responsibilities:

- Connector: API, database or file connectivity.
- Extractor: Incremental data extraction.
- Validator: Schema and business validation.
- Transformer: Standardization.
- Loader: S3, GCS or Snowflake writing.
- Audit: Counts, watermarks and execution logs.
- Retry: Transient failure handling.

## 7.2 Database Extraction Using Incremental Watermark

python

```python
from datetime import datetime
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, text


def extract_incremental(
    connection_url: str,
    last_watermark: datetime,
    upper_watermark: datetime
) -> pd.DataFrame:

    engine = create_engine(connection_url)

    query = text("""
        SELECT
            customer_id,
            customer_name,
            city,
            updated_at
        FROM customers
        WHERE updated_at > :lower_bound
          AND updated_at <= :upper_bound
        ORDER BY updated_at, customer_id
    """)

    with engine.connect() as connection:

        result = pd.read_sql(
            query,
            connection,
            params={
                "lower_bound": last_watermark,
                "upper_bound": upper_watermark
            }
        )

    return result
```

For a production CDC pipeline, prefer transaction-log positions or a source-supported consistent snapshot boundary when available.

Timestamp extraction alone can miss changes if timestamps are not reliable or multiple records share the same timestamp.

## 7.3 Python Data Validation

python

```python
def validate_customer_records(df):

    required_columns = [
        "customer_id",
        "customer_name",
        "email"
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing columns: {missing_columns}"
        )

    invalid = df[
        df["customer_id"].isna()
        | df["customer_name"].isna()
    ]

    valid = df.drop(invalid.index)

    return valid, invalid
```

Separate rejected records from valid records. Never silently discard invalid business data.

## 7.4 Python Structured Logging

python

```python
import logging
import json


class JsonFormatter(logging.Formatter):

    def format(self, record):

        return json.dumps({
            "level": record.levelname,
            "message": record.getMessage(),
            "pipeline": getattr(
                record,
                "pipeline",
                None
            ),
            "run_id": getattr(
                record,
                "run_id",
                None
            )
        })


logger = logging.getLogger("data_pipeline")

handler = logging.StreamHandler()

handler.setFormatter(JsonFormatter())

logger.addHandler(handler)

logger.setLevel(logging.INFO)
```

Add execution IDs, source identifiers, batch IDs and error classifications to production logs.

## 7.5 Python Idempotency

Use a deterministic batch identity.

python

```python
import hashlib


def create_batch_id(
    source_name,
    start_time,
    end_time
):

    value = (
        f"{source_name}|"
        f"{start_time}|"
        f"{end_time}"
    )

    return hashlib.sha256(
        value.encode("utf-8")
    ).hexdigest()
```

Store the batch ID in audit tables and target metadata.

A repeated batch should either:

- Recognize that it already completed.
- Safely reprocess the same input.
- Replace the previous batch output atomically.

---

# PART 8 – PYSPARK ADVANCED ENGINEERING

## 8.1 Spark Execution Concepts

Master:

- Driver.
- Executors.
- Jobs.
- Stages.
- Tasks.
- Partitions.
- DAG.
- Catalyst.
- Tungsten.
- AQE.
- Shuffle.
- Broadcast.
- Persistence.
- Checkpointing.

## 8.2 Advanced PySpark Data Processing

python

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    broadcast,
    row_number,
    current_timestamp
)
from pyspark.sql.window import Window


spark = (
    SparkSession.builder
    .appName("EnterpriseCustomerPipeline")
    .config("spark.sql.adaptive.enabled", "true")
    .config(
        "spark.sql.adaptive.skewJoin.enabled",
        "true"
    )
    .getOrCreate()
)


events = spark.read.parquet(
    "gs://enterprise-data-lake/bronze/customers/"
)


customers = events.filter(
    col("customer_id").isNotNull()
)


window_spec = (
    Window
    .partitionBy("customer_id")
    .orderBy(
        col("source_commit_ts").desc(),
        col("source_sequence").desc()
    )
)


latest_customers = (
    customers
    .withColumn(
        "row_num",
        row_number().over(window_spec)
    )
    .filter(col("row_num") == 1)
    .drop("row_num")
)


latest_customers = latest_customers.withColumn(
    "processed_at",
    current_timestamp()
)


latest_customers.write \
    .mode("append") \
    .parquet(
        "gs://enterprise-data-lake/silver/customers/"
    )


spark.stop()
```

For historical CDC, do not use latest-record deduplication as the entire transformation. Retain all valid ordered changes.

The same processing code can use an S3 path:

python

```python
s3_path = (
    "s3://enterprise-data/bronze/customers/"
)
```

## 8.3 Advanced Spark Join Optimization

python

```python
from pyspark.sql.functions import broadcast

result = fact_orders.join(
    broadcast(dim_customer),
    on="customer_id",
    how="left"
)
```

Broadcast only when the build-side dataset is small enough for the executor memory budget.

## 8.4 Explain Query Plan

python

```python
result.explain(True)
```

Inspect:

- Scan operations.
- Exchange operations.
- Sort operations.
- Join strategy.
- Partition pruning.
- Whole-stage code generation.

## 8.5 Handling Data Skew

python

```python
from pyspark.sql.functions import (
    col,
    floor,
    rand
)

salted_fact = fact_orders.withColumn(
    "salt",
    floor(rand() * 10)
)
```

For a corresponding salted join, replicate the relevant dimension rows across salt values and join using both the business key and salt.

Use this only when actual skew is identified in the Spark UI.

## 8.6 Spark File Optimization

Avoid producing thousands of tiny Parquet files.

python

```python
(
    df
    .repartition(100, "transaction_date")
    .write
    .mode("overwrite")
    .partitionBy("transaction_date")
    .parquet(
        "gs://enterprise-data-lake/silver/orders/"
    )
)
```

Choose partition count and file sizes based on data volume and downstream access patterns.

---

# PART 9 – KAFKA AND REAL-TIME STREAMING

## 9.1 Kafka Architecture

text

```text
Producer
   |
   v
Kafka Topic
   |
   +---- Partition 0
   |
   +---- Partition 1
   |
   +---- Partition 2
   |
Consumer Group
   |
   v
Spark Streaming
   |
   v
GCS / S3
   |
   v
Snowflake
```

## 9.2 Kafka Event Identity

Every event should contain:

- event\_id
- business\_key
- operation
- source\_timestamp
- source\_sequence
- transaction\_id
- schema\_version

Use a business key as the Kafka message key when per-entity ordering is important.

## 9.3 PySpark Structured Streaming

python

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    current_timestamp
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)


spark = SparkSession.builder \
    .appName("CustomerCDCStream") \
    .getOrCreate()


schema = StructType([
    StructField("event_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("operation", StringType()),
    StructField("city", StringType()),
    StructField("source_commit_ts", StringType()),
    StructField("source_sequence", StringType())
])


kafka_df = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        "broker:9092"
    )
    .option("subscribe", "customer_cdc")
    .load()
)


events = kafka_df.select(
    from_json(
        col("value").cast("string"),
        schema
    ).alias("event"),

    col("partition"),
    col("offset"),
    col("timestamp")
).select(
    "event.*",
    "partition",
    "offset",
    "timestamp"
)


valid = events.filter(
    col("event_id").isNotNull()
    & col("customer_id").isNotNull()
)


query = (
    valid.writeStream
    .format("parquet")
    .option(
        "path",
        "gs://enterprise-data-lake/cdc/customers/"
    )
    .option(
        "checkpointLocation",
        "gs://enterprise-data-lake/checkpoints/customers/"
    )
    .outputMode("append")
    .start()
)


query.awaitTermination()
```

For S3, replace the paths with the corresponding S3 locations.

For production, configure schema validation, rejected-event output, checkpoint lifecycle, durable replay and a supported transactional or idempotent sink strategy.

---

# PART 10 – AIRFLOW ADVANCED ORCHESTRATION

## 10.1 Enterprise Pipeline DAG

text

```text
START
  |
  v
EXTRACT_SOURCE
  |
  v
WRITE_TO_GCS_OR_S3
  |
  v
VALIDATE_OBJECTS
  |
  v
LOAD_SNOWFLAKE_RAW
  |
  v
PROCESS_LANDING
  |
  v
PROCESS_PREPARE
  |
  v
PROCESS_NORMALIZED
  |
  v
BUILD_SCD_AND_FACTS
  |
  v
BUILD_MARKETPLACE
  |
  v
RUN_DATA_QUALITY
  |
  v
RECONCILIATION
  |
  v
PUBLISH_BI_DATASETS
  |
  v
SUCCESS
```

## 10.2 Airflow TaskFlow Example

python

```python
from datetime import datetime, timedelta

from airflow.decorators import dag, task


@dag(
    dag_id="enterprise_customer_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data_engineering",
        "retries": 3,
        "retry_delay": timedelta(minutes=5)
    },
    tags=["snowflake", "gcs", "customer"]
)
def customer_pipeline():

    @task
    def extract_source():
        return {
            "batch_id": "example-batch",
            "status": "extracted"
        }

    @task
    def upload_to_object_storage(metadata):
        return {
            **metadata,
            "storage_status": "uploaded"
        }

    @task
    def load_snowflake(metadata):
        return {
            **metadata,
            "warehouse_status": "loaded"
        }

    @task
    def execute_dbt(metadata):
        return {
            **metadata,
            "dbt_status": "success"
        }

    @task
    def reconcile(metadata):
        return {
            **metadata,
            "reconciliation_status": "success"
        }

    source = extract_source()

    uploaded = upload_to_object_storage(source)

    loaded = load_snowflake(uploaded)

    transformed = execute_dbt(loaded)

    reconcile(transformed)


customer_pipeline()
```

This example demonstrates dependency flow. In production, tasks should call actual operators or service clients, pass only small metadata through XCom and obtain credentials from Airflow connections or a secrets backend.

## 10.3 Advanced Airflow Concepts

Master:

- Dynamic task mapping.
- TaskFlow API.
- Task groups.
- Deferrable operators.
- Sensors.
- Pools.
- Priority weights.
- Dataset scheduling.
- Backfills.
- Catchup.
- Logical dates.
- Data intervals.
- SLA and deadline monitoring.
- Kubernetes executors.
- Secrets backends.

Never use Airflow XCom to transfer large dataframes or entire source datasets.

---

# PART 11 – DATA QUALITY, DATA CONTRACTS AND OBSERVABILITY

## 11.1 Data Quality Framework

Every pipeline should validate:

| Dimension | Validation |
| --- | --- |
| Completeness | Required columns are populated |
| Uniqueness | Business keys are not duplicated |
| Validity | Data conforms to expected rules |
| Accuracy | Business values are correct |
| Consistency | Related datasets agree |
| Timeliness | Data arrives within SLA |
| Referential integrity | Fact references resolve |

## 11.2 Data Contract Example

yaml

```yaml
dataset: customer

owner: customer_data_team

schema_version: 1.0

primary_key:
  - customer_id

columns:
  - name: customer_id
    type: integer
    nullable: false

  - name: customer_name
    type: string
    nullable: false

  - name: email
    type: string
    nullable: true

  - name: updated_at
    type: timestamp
    nullable: false

quality:
  uniqueness:
    - customer_id

  freshness:
    maximum_delay_minutes: 60
```

Data contracts should define ownership, schema compatibility, business semantics, freshness expectations and change-notification responsibilities.

## 11.3 Reconciliation Framework

sql

```sql
WITH SOURCE AS (

    SELECT
        COUNT(*) AS SOURCE_COUNT,
        SUM(ORDER_AMOUNT) AS SOURCE_AMOUNT

    FROM PREPARE.PREP_ORDERS

),

TARGET AS (

    SELECT
        COUNT(*) AS TARGET_COUNT,
        SUM(ORDER_AMOUNT) AS TARGET_AMOUNT

    FROM SCHEMATIZED.FACT_ORDERS

)

SELECT
    SOURCE_COUNT,
    TARGET_COUNT,
    SOURCE_AMOUNT,
    TARGET_AMOUNT,

    SOURCE_COUNT - TARGET_COUNT
        AS COUNT_DIFFERENCE,

    SOURCE_AMOUNT - TARGET_AMOUNT
        AS AMOUNT_DIFFERENCE

FROM SOURCE
CROSS JOIN TARGET;
```

In production, reconcile by batch, source interval, business date or another consistent grain. Ensure the source and target apply identical filters and business rules.

## 11.4 Observability Metrics

Capture:

- Total source records.
- Total target records.
- Rejected records.
- Duplicate events.
- CDC lag.
- Pipeline duration.
- Query execution duration.
- Warehouse queue duration.
- Spark shuffle spill.
- Kafka consumer lag.
- API retry count.
- Data freshness.
- SLA success rate.

Build operational dashboards for engineering teams separately from business dashboards.

---

# PART 12 – ADVANCED DATA MODELING

## 12.1 Dimensional Modeling

Master:

- Star schema.
- Snowflake schema.
- Fact tables.
- Dimension tables.
- Conformed dimensions.
- Role-playing dimensions.
- Degenerate dimensions.
- Junk dimensions.
- Bridge tables.
- Accumulating snapshots.
- Periodic snapshots.
- Transaction facts.

## 12.2 Fact Table Types

| Type | Example |
| --- | --- |
| Transaction | One row per sales transaction |
| Periodic snapshot | One row per account per day |
| Accumulating snapshot | One row per loan application lifecycle |
| Factless fact | Customer attendance or eligibility event |

## 12.3 Late-Arriving Dimensions

A fact can arrive before its corresponding customer dimension.

Solution:

1. Create an inferred dimension record.
2. Use a designated unknown or inferred surrogate key.
3. Update the inferred dimension when the real attributes arrive.
4. Preserve historical validity.
5. Reconcile unresolved references.

Example:

sql

```sql
INSERT INTO SCHEMATIZED.DIM_CUSTOMER (
    CUSTOMER_ID,
    CUSTOMER_NAME,
    IS_CURRENT
)

SELECT
    F.CUSTOMER_ID,
    'UNKNOWN',
    TRUE

FROM PREPARE.PREP_ORDERS F

LEFT JOIN SCHEMATIZED.DIM_CUSTOMER D
    ON F.CUSTOMER_ID = D.CUSTOMER_ID

WHERE D.CUSTOMER_ID IS NULL;
```

Inferred-member creation must be idempotent and protected against concurrent duplicate inserts.

## 12.4 Fact Correction and Restatement

When a historical order changes:

- Identify affected business facts.
- Determine whether the change is an update or reversal.
- Recalculate financial measures.
- Re-resolve dimensional surrogate keys if necessary.
- Reprocess affected aggregate partitions.
- Reconcile the corrected period.

Never assume that updating a dimension automatically fixes historical fact relationships.

---

# PART 13 – PERFORMANCE AND COST OPTIMIZATION

## 13.1 Snowflake

- Use independent warehouses for independent workloads.
- Optimize scanned data.
- Avoid SELECT \* in production transformations.
- Monitor spilling.
- Use clustering selectively.
- Evaluate search optimization for suitable point lookups.
- Monitor warehouse credits.
- Set resource monitors.
- Review materialization decisions.
- Avoid unnecessary full refreshes.

## 13.2 dbt

- Incremental models for large changing datasets.
- Selective execution.
- Avoid excessive nested ephemeral models.
- Reuse intermediate transformations.
- Apply tests at meaningful business grains.
- Optimize source filters.
- Inspect compiled SQL.
- Use Slim CI where appropriate.

## 13.3 PySpark

- Filter early.
- Project required columns.
- Use broadcast joins selectively.
- Enable AQE where appropriate.
- Avoid unnecessary shuffles.
- Control partition counts.
- Avoid driver collection.
- Avoid unnecessary Python UDFs.
- Compact small files.
- Monitor executor memory and skew.

## 13.4 Object Storage

- Use Parquet for analytical datasets.
- Apply appropriate compression.
- Partition on query-relevant fields.
- Avoid excessive small files.
- Use immutable source object keys.
- Configure lifecycle policies.
- Maintain manifests and ingestion metadata.

---

# PART 14 – SECURITY AND GOVERNANCE

## 14.1 Snowflake RBAC

Create separate roles for:

- Ingestion.
- Transformation.
- Data engineering.
- Analytics.
- BI consumption.
- Security administration.

Example:

sql

```sql
CREATE ROLE IF NOT EXISTS ROLE_INGESTION;
CREATE ROLE IF NOT EXISTS ROLE_TRANSFORMATION;
CREATE ROLE IF NOT EXISTS ROLE_BI;

GRANT USAGE ON DATABASE ENTERPRISE_DW
TO ROLE ROLE_BI;

GRANT USAGE ON SCHEMA ENTERPRISE_DW.MARKETPLACE
TO ROLE ROLE_BI;

GRANT SELECT ON ALL TABLES
IN SCHEMA ENTERPRISE_DW.MARKETPLACE
TO ROLE ROLE_BI;
```

Also configure future grants, appropriate warehouse usage and schema-level privileges.

## 14.2 PII Management

Classify:

- Public data.
- Internal data.
- Confidential data.
- Restricted PII.

Implement:

- Dynamic data masking.
- Row access policies.
- Secure views.
- Encryption.
- Audit trails.
- Retention controls.
- Least privilege.

## 14.3 Cloud Security

For AWS:

- IAM roles.
- S3 bucket policies.
- KMS.
- CloudTrail.
- Secrets Manager.
- VPC endpoints where appropriate.

For GCP:

- Service accounts.
- IAM.
- Cloud KMS.
- Cloud Audit Logs.
- Secret Manager.
- Private connectivity where appropriate.

---

# PART 15 – CI/CD AND INFRASTRUCTURE AS CODE

## 15.1 Git Workflow

text

```text
MAIN
 |
 +--- FEATURE
 |
 +--- DEVELOPMENT
 |
 +--- TESTING
 |
 +--- PRODUCTION
```

Recommended promotion:

1. Pull request.
2. Python unit tests.
3. SQL linting.
4. dbt parsing.
5. dbt compilation.
6. Integration testing.
7. Development deployment.
8. Approval.
9. Production deployment.
10. Post-deployment validation.

## 15.2 Terraform

Use Terraform to manage:

- S3 buckets.
- GCS buckets.
- IAM.
- Cloud identities.
- Glue jobs.
- Snowflake warehouses.
- Snowflake integrations.
- Database and schema permissions.
- Airflow infrastructure.

Keep infrastructure state protected and separate development and production workspaces or state configurations.

## 15.3 GitHub Actions

yaml

```yaml
name: Data Engineering CI

on:
  pull_request:
    branches:
      - main

jobs:
  validate:

    runs-on: ubuntu-latest

    steps:

      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install
        run: |
          pip install pytest sqlfluff dbt-snowflake

      - name: Python Tests
        run: pytest tests/

      - name: SQL Lint
        run: sqlfluff lint models/ --dialect snowflake

      - name: dbt Parse
        run: dbt parse --profiles-dir .
```

Use protected deployment environments and managed secrets. Pin dependencies and action versions according to organizational security policy.

---

# PART 16 – PRODUCTION INCIDENT MANAGEMENT

## Scenario 1: GCS Files Are Not Loading into Snowflake

Investigate:

1. Bucket path.
2. Snowflake stage configuration.
3. Storage integration.
4. GCS IAM permissions.
5. File format.
6. File compatibility.
7. COPY INTO history.
8. Cloud notification configuration if Snowpipe is used.

## Scenario 2: S3 and GCS Both Contain Different Record Counts

Investigate:

- Source extraction consistency.
- File manifests.
- Partition completeness.
- Duplicate files.
- Object arrival timing.
- Time-zone differences.
- Schema evolution.

Use a source watermark and immutable batch ID to compare equivalent extraction intervals.

## Scenario 3: Snowflake RAW Is Correct but PREPARE Is Missing Records

Investigate:

- Filtering logic.
- Deduplication ordering.
- Type conversion.
- Null handling.
- MERGE conditions.
- Late-arriving events.
- Invalid-record routing.

## Scenario 4: SCD2 Has Multiple Current Records

Investigate:

- Concurrent runs.
- Duplicate source keys.
- Same-timestamp events.
- Transaction ordering.
- Non-atomic expire-and-insert processing.

Reconstruct affected histories and add uniqueness and interval validation.

## Scenario 5: Tableau and Power BI Show Different Revenue

Investigate:

- KPI definitions.
- Time-zone conversion.
- Refund handling.
- Duplicate fact records.
- Semantic-model filters.
- Data refresh timing.
- Different aggregation grains.

Create one governed metric definition and reconcile both tools against it.

## Scenario 6: Kafka Consumer Lag Increases

Investigate:

- Partition count.
- Consumer parallelism.
- Processing throughput.
- Downstream write latency.
- Retry loops.
- Poison messages.
- Broker health.

Scale consumers only when there are available partitions and the downstream system can support the increased concurrency.

## Scenario 7: Airflow Retries Create Duplicate Data

Investigate:

- Batch identity.
- Task commit point.
- Watermark updates.
- Partial writes.
- MERGE behavior.
- External API side effects.

Make the task replay-safe and ensure the watermark advances only after successful durable processing.

---

# PART 17 – ADDITIONAL ADVANCED CONCEPTS

A Senior Data Engineer should develop working knowledge of the following.

## Cloud Data Engineering

- AWS Glue.
- AWS DMS.
- AWS EMR.
- Google Cloud Dataflow.
- Google Cloud Dataproc.
- BigQuery architecture.
- Pub/Sub.
- Cloud Composer.
- Cloud Storage lifecycle management.
- Cross-cloud connectivity.

## Snowflake

- Snowpark Python.
- Stored procedures.
- JavaScript procedures.
- Dynamic Tables.
- Snowpipe Streaming.
- Hybrid Tables.
- Iceberg Tables.
- Secure Data Sharing.
- Replication and Failover.
- Resource Monitors.
- Masking and Row Access Policies.

## Streaming

- Debezium.
- Kafka Connect.
- Schema Registry.
- Avro.
- Protobuf.
- Event sourcing.
- Exactly-once boundaries.
- Stateful processing.
- Backpressure.
- Watermarks.
- Replay and recovery.

## Data Architecture

- Medallion architecture.
- Data mesh.
- Data fabric.
- Lakehouse.
- Open table formats.
- Data contracts.
- Domain-oriented data products.
- Master data management.
- Data lineage.
- Data governance.

## Engineering Practices

- Unit testing.
- Integration testing.
- Contract testing.
- CI/CD.
- Infrastructure as Code.
- Observability.
- Incident response.
- Capacity planning.
- Cost governance.
- Disaster recovery.

---

# PART 18 – SENIOR DATA ENGINEER INTERVIEW PREPARATION

## Architecture Questions

1. Design a complete enterprise data platform using Snowflake.
2. Explain GCS versus S3 for data lake architecture.
3. How would you implement multi-source ingestion?
4. How do you design a replayable CDC architecture?
5. Explain the responsibilities of RAW, LANDING, PREPARE, NORMALIZED and MARKETPLACE.
6. How would you support both batch and streaming?
7. How would you design a data platform for multiple business domains?
8. How would you implement disaster recovery?

## Snowflake Questions

1. Explain micro-partition pruning.
2. Explain warehouse scaling.
3. How do you investigate spilling?
4. Explain Streams versus Dynamic Tables.
5. How do you implement Snowpipe?
6. How do you optimize large MERGE statements?
7. How do you implement row-level security?
8. How do you handle schema evolution?
9. Explain Time Travel and cloning.
10. How do you manage query concurrency?

## dbt Questions

1. Explain incremental models.
2. Explain snapshots.
3. How do you handle late-arriving records?
4. How do you build an SCD2 model?
5. Explain model contracts.
6. Explain state-based selection.
7. How do you manage schema changes?
8. How do you implement Slim CI?
9. How do you test incremental transformations?
10. How do you manage production full refreshes?

## CDC Questions

1. How do you deduplicate CDC events?
2. How do you handle out-of-order events?
3. How do you handle hard deletes?
4. How do you reconstruct an SCD2 timeline?
5. What is the difference between source commit time and ingestion time?
6. How do you prevent stale updates?
7. What happens when multiple events have the same timestamp?
8. How do you replay historical events?

## PySpark Questions

1. Explain Catalyst.
2. Explain AQE.
3. What is a shuffle?
4. How do you resolve data skew?
5. When should broadcast joins be used?
6. Explain repartition versus coalesce.
7. What is checkpointing?
8. How do you troubleshoot executor failures?
9. How do you optimize file output?
10. Explain Structured Streaming recovery.

## Production Scenario Questions

1. Your pipeline fails after loading RAW but before PREPARE. How do you recover?
2. A source system sends duplicate updates. How do you handle them?
3. A customer changes address three times in one day. How do you preserve history?
4. A delete arrives before an older update. How do you prevent resurrection?
5. Snowflake costs increase by 40%. How do you investigate?
6. A dbt incremental model misses a record. How do you recover?
7. A Power BI report shows different results from Tableau. How do you troubleshoot?
8. Your Spark job has one task running much longer than the others. What do you investigate?
9. GCS ingestion suddenly stops. What do you inspect?
10. An Airflow retry processes an already completed batch. How do you prevent duplicate writes?

---

# PART 19 – COMPLETE HANDS-ON CAPSTONE PROJECT

## Project: Enterprise Customer and Sales Data Platform

### Business requirement

An enterprise receives data from:

- Oracle customer database.
- PostgreSQL order database.
- REST APIs.
- Kafka order events.
- Daily CSV and Parquet files.

The platform must support:

- Batch ingestion.
- CDC ingestion.
- Historical customer tracking.
- Sales reporting.
- Customer analytics.
- Tableau and Power BI consumption.

### Proposed implementation

text

```text
                  SOURCE SYSTEMS
              Oracle / PostgreSQL / APIs
                         |
                +--------+--------+
                |                 |
              Batch              CDC
                |                 |
              Airflow        DMS / Debezium
                |                 |
                |               Kafka
                |                 |
                +--------+--------+
                         |
                +--------+--------+
                |                 |
              AWS S3              GCS
                |                 |
                +--------+--------+
                         |
                   SNOWFLAKE
                         |
                       RAW
                         |
                      LANDING
                         |
                      PREPARE
                         |
                     NORMALIZED
                         |
                    SCHEMATIZED
                         |
                    MARKETPLACE
                         |
                      SEMANTIC
                         |
                  +------+------+
                  |             |
                TABLEAU       POWER BI
```

### Project implementation sequence

| Phase | Implementation |
| --- | --- |
| 1 | Source database setup |
| 2 | Batch extraction using Python |
| 3 | CDC extraction using DMS or Debezium |
| 4 | Kafka event processing |
| 5 | GCS or S3 object storage |
| 6 | Snowflake storage integration |
| 7 | COPY INTO / Snowpipe |
| 8 | RAW and LANDING development |
| 9 | PREPARE transformations |
| 10 | NORMALIZED entity models |
| 11 | SCD Type 1 and Type 2 |
| 12 | FACT and DIMENSION models |
| 13 | dbt incremental processing |
| 14 | MARKETPLACE development |
| 15 | SEMANTIC KPI development |
| 16 | Airflow orchestration |
| 17 | Data quality and reconciliation |
| 18 | Tableau and Power BI integration |
| 19 | CI/CD deployment |
| 20 | Production monitoring and recovery |

### Expected project deliverables

1. Complete architecture diagram.
2. Cloud storage configuration.
3. Python ingestion framework.
4. CDC event-processing framework.
5. Snowflake database and schema design.
6. dbt transformation project.
7. SCD Type 2 implementation.
8. PySpark processing jobs.
9. Airflow DAGs.
10. Data-quality framework.
11. Audit and watermark tables.
12. BI marketplace datasets.
13. Tableau or Power BI dashboard.
14. CI/CD workflow.
15. Production troubleshooting documentation.

---

# PART 20 – FINAL SENIOR DATA ENGINEER MASTERY CHECKLIST

## Architecture

- [ ]Understand batch, micro-batch and streaming.
- [ ]Design S3 and GCS storage architectures.
- [ ]Design Snowflake multi-layer processing.
- [ ]Understand lakehouse and data warehouse concepts.
- [ ]Define ingestion and recovery boundaries.

## Snowflake

- [ ]Advanced SQL.
- [ ]Query Profile.
- [ ]Micro-partition pruning.
- [ ]Clustering.
- [ ]Streams and Tasks.
- [ ]Dynamic Tables.
- [ ]Snowpipe.
- [ ]Snowpipe Streaming.
- [ ]Snowpark.
- [ ]RBAC and governance.

## dbt

- [ ]Sources and refs.
- [ ]Incremental models.
- [ ]Snapshots.
- [ ]Macros.
- [ ]Model contracts.
- [ ]Unit tests.
- [ ]State-based selection.
- [ ]CI/CD.

## CDC and Data Modeling

- [ ]Event deduplication.
- [ ]Source sequence ordering.
- [ ]Late-arriving events.
- [ ]Hard deletes.
- [ ]SCD Type 1.
- [ ]SCD Type 2.
- [ ]SCD Type 3 and 6.
- [ ]Late-arriving dimensions.
- [ ]Fact restatement.
- [ ]Timeline reconstruction.

## Python and PySpark

- [ ]Production ingestion framework.
- [ ]API pagination.
- [ ]Retry and timeout.
- [ ]Structured logging.
- [ ]Schema validation.
- [ ]Spark joins.
- [ ]AQE.
- [ ]Skew handling.
- [ ]Shuffle optimization.
- [ ]Structured Streaming.

## Airflow and Kafka

- [ ]DAG design.
- [ ]TaskFlow.
- [ ]Dynamic mapping.
- [ ]Backfills.
- [ ]Idempotency.
- [ ]Kafka partitions.
- [ ]Consumer groups.
- [ ]Offsets.
- [ ]Watermarks.
- [ ]Replay and recovery.

## Production Engineering

- [ ]Audit logging.
- [ ]Data quality.
- [ ]Reconciliation.
- [ ]Monitoring.
- [ ]Alerting.
- [ ]Data contracts.
- [ ]Security.
- [ ]CI/CD.
- [ ]Terraform.
- [ ]Disaster recovery.

---

# FINAL ENGINEERING PRINCIPLE

A Senior Data Engineer must be able to design, develop, optimize, deploy and operate an entire data platform.

The objective is not simply to move data from Oracle to Snowflake or to write a complex PySpark transformation.

The objective is to build a platform that is:

1. Correct – Business data is accurate.
2. Reliable – Failures can be recovered.
3. Idempotent – Reprocessing does not corrupt data.
4. Scalable – Processing supports increasing volumes.
5. Performant – Queries and pipelines meet SLAs.
6. Cost-efficient – Compute and storage are optimized.
7. Secure – Sensitive data is appropriately protected.
8. Observable – Every important operation is measurable.
9. Governed – Data ownership and definitions are clear.
10. Maintainable – Other engineers can understand and operate it.

**The recommended learning and implementation project is:**

Oracle / PostgreSQL CDC → Kafka → AWS S3 or GCS → Snowflake RAW → LANDING → PREPARE → NORMALIZED → SCHEMATIZED → MARKETPLACE → SEMANTIC → Tableau / Power BI.

Build the platform end to end, implement historical CDC correctly, automate it through Airflow and dbt, and demonstrate recovery from real production failure scenarios.

That is the level of practical engineering capability expected from a strong Senior Data Engineer.

## My additional recommendation: how to master this SOP

Don't try to learn all the technologies simultaneously. Work through the following progression.

| Stage | Focus | Practical outcome |
| --- | --- | --- |
| 1 | Advanced SQL + Snowflake | Build all Snowflake layers |
| 2 | Python + APIs | Build reusable ingestion framework |
| 3 | S3 + GCS | Implement both storage alternatives |
| 4 | CDC + Kafka | Build event-driven ingestion |
| 5 | dbt | Build incremental models and SCD2 |
| 6 | PySpark | Build distributed transformations |
| 7 | Airflow | Orchestrate end-to-end pipelines |
| 8 | Data modeling | Build facts, dimensions and marts |
| 9 | BI | Publish Tableau and Power BI datasets |
| 10 | Production engineering | Monitoring, recovery, CI/CD and optimization |

One particularly important distinction: Your RAW → LANDING → PREPARE → NORMALIZED → SCHEMATIZED → MARKETPLACE design is a valid enterprise convention, but it is not a mandatory Snowflake architecture. The value comes from defining clear contracts and responsibilities for every layer, not from creating more physical copies of data.

I recommend implementing the same project twice at the storage-ingestion boundary: once using S3 and once using GCS. Keep the Snowflake transformation and dbt layers identical. This demonstrates genuine cloud-independent data engineering architecture.