Below is an expanded Senior Data Engineer – Snowflake, dbt, Python, PySpark, AWS, Real-Time Streaming & Enterprise Data Architecture Master SOP.

I have structured it as a combination of:

1. Production implementation handbook.
2. End-to-end architecture reference.
3. SQL, Python, PySpark, dbt and Airflow coding guide.
4. Performance tuning and troubleshooting manual.
5. Senior Data Engineer L2/L3 interview preparation.
6. Real-world production scenarios with solutions.
7. Best practices, design decisions, trade-offs and common mistakes.

I have also included important corrections to some patterns in your original document. For example, handling duplicate CDC events with deterministic ordering, rebuilding SCD Type 2 timelines safely, avoiding incorrect incremental lookbacks, managing hard deletes, and implementing idempotent streaming pipelines.

# Senior Data Engineer Master SOP

Writing

# SENIOR DATA ENGINEER – MASTER STANDARD OPERATING PROCEDURE

## Snowflake | dbt | Python | PySpark | Airflow | AWS | CDC | Batch | Streaming | Enterprise Architecture

**Document Type:** Technical SOP and Production Engineering Handbook\
**Target Role:** Senior Data Engineer / Lead Data Engineer\
**Experience Level:** Mid-Senior to Senior (L2/L3)\
**Technology Stack:** Snowflake, dbt Core, Python, PySpark, Apache Spark, Airflow, AWS S3, Glue, IAM, Kafka, Snowpipe, Snowpipe Streaming, Dynamic Tables, Streams and Tasks.

---

# PART 1 – ENTERPRISE DATA ENGINEERING FUNDAMENTALS

## 1.1 Data Engineering Architecture

A modern enterprise data platform generally consists of five major layers:

1. Source systems – Oracle, SQL Server, PostgreSQL, MySQL, APIs, SaaS applications.
2. Ingestion – CDC, Kafka, APIs, batch extraction, AWS DMS, file ingestion.
3. Storage – Amazon S3, Snowflake, data lake and lakehouse.
4. Transformation – PySpark, dbt, Snowflake SQL, AWS Glue.
5. Consumption – Tableau, Power BI, analytics, reporting and machine learning.

### Reference architecture

text

```text
              SOURCE SYSTEMS
       Oracle | PostgreSQL | APIs | ERP
                    |
          +---------+----------+
          |                    |
       CDC/Kafka            Batch/API
          |                    |
          +---------+----------+
                    |
             INGESTION LAYER
         AWS DMS | Kafka Connect
                    |
              AWS S3 BRONZE
             Raw Immutable Data
                    |
           Glue / PySpark / Spark
                    |
              AWS S3 SILVER
           Validated Parquet Data
                    |
             Snowflake Ingestion
         COPY INTO / Snowpipe / Kafka
                    |
              SNOWFLAKE
                    |
          +---------+----------+
          |         |          |
       RAW       STAGING    INTERMEDIATE
          |         |          |
          +---------+----------+
                    |
                 dbt CORE
                    |
            FACTS AND DIMENSIONS
                    |
               DATA MARTS
                    |
          BI / Tableau / Power BI
```

### Important architectural principle

Keep ingestion, transformation, orchestration and consumption responsibilities separate.

- Kafka handles event transportation.
- S3 provides durable object storage.
- Glue/PySpark performs distributed processing.
- Snowflake provides analytical storage and compute.
- dbt manages SQL transformation and dependency graphs.
- Airflow manages workflow orchestration.
- Data quality frameworks validate business and technical expectations.

Do not introduce Spark, Kafka or Glue into every pipeline automatically. Use the simplest architecture that satisfies latency, scale, reliability and operational requirements.

## 1.2 Batch vs Micro-Batch vs Streaming

| Characteristic | Batch | Micro-batch | Streaming |
| --- | --- | --- | --- |
| Processing | Scheduled | Small intervals | Continuous |
| Latency | Minutes/hours | Seconds/minutes | Milliseconds/seconds |
| Example | Daily ERP extraction | 5-minute API ingestion | Kafka clickstream |
| Complexity | Low | Medium | High |
| Recovery | Rerun batch | Replay interval | Offset/checkpoint recovery |
| Typical tools | Airflow, dbt | Spark, Snowpipe | Kafka, Flink, Spark Streaming |

### Batch processing example

python

```python
def run_daily_batch(execution_date):
    source = extract_source_data(execution_date)

    validate_source(source)

    write_to_s3(
        data=source,
        path=f"s3://company-data/bronze/orders/date={execution_date}/"
    )

    load_snowflake(execution_date)

    run_dbt_models(execution_date)
```

**Explanation:**

1. Extract records for a fixed business date.
2. Validate mandatory fields.
3. Persist immutable source data in S3.
4. Load the corresponding partition into Snowflake.
5. Execute dbt transformations.
6. Run reconciliation and quality checks.

The execution date should be passed as a parameter rather than relying on the machine's current date.

### Micro-batch processing

For a system receiving data every five minutes:

python

```python
from datetime import datetime, timedelta, timezone

def get_processing_window():
    window_end = datetime.now(timezone.utc)
    window_start = window_end - timedelta(minutes=5)

    return window_start, window_end
```

Production systems should preferably use orchestrator-provided interval boundaries rather than calculating windows from wall-clock time. Persist the window boundaries with each run.

### Streaming processing

python

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder \
    .appName("KafkaOrderProcessor") \
    .getOrCreate()

schema = StructType([
    StructField("event_id", StringType()),
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_timestamp", StringType())
])

raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "latest") \
    .load()

events = raw.select(
    from_json(col("value").cast("string"), schema).alias("event"),
    col("topic"),
    col("partition"),
    col("offset")
).select("event.*", "topic", "partition", "offset")

valid_events = events.filter(
    col("event_id").isNotNull() &
    col("order_id").isNotNull()
)

query = valid_events.writeStream \
    .format("parquet") \
    .option("path", "s3://company-data/bronze/orders/") \
    .option("checkpointLocation", "s3://company-data/checkpoints/orders/") \
    .outputMode("append") \
    .start()

query.awaitTermination()
```

**Explanation:**

- `readStream` establishes a streaming source.
- Kafka offsets identify consumed messages.
- `from_json` converts JSON into structured columns.
- Validation removes invalid records from the valid output.
- The checkpoint records streaming progress.
- `awaitTermination()` keeps the streaming application running.

A production implementation should also route invalid events into a dead-letter location and retain the original Kafka metadata.

---

# PART 2 – CDC AND CHANGE DATA CAPTURE

## 2.1 What is CDC?

Change Data Capture identifies and extracts changes from operational databases.

Common operations:

- INSERT
- UPDATE
- DELETE

Example:

Original source record:

| customer\_id | city | updated\_at |
| --- | --- | --- |
| 101 | Kolkata | 10:00 |

Updated source record:

| customer\_id | city | updated\_at |
| --- | --- | --- |
| 101 | Delhi | 10:15 |

CDC captures the change rather than extracting the entire source table again.

### CDC architecture

text

```text
Oracle / PostgreSQL
        |
   Transaction Log
        |
   Debezium / AWS DMS
        |
       Kafka
        |
   Kafka Consumer
        |
      AWS S3
        |
    Snowflake
        |
      dbt
        |
   Analytics Models
```

### Important CDC metadata

A production CDC event should ideally contain:

| Column | Purpose |
| --- | --- |
| event\_id | Unique event identifier |
| business\_key | Customer/order identifier |
| operation | INSERT, UPDATE, DELETE |
| before | Previous record values |
| after | New record values |
| source\_commit\_ts | Source transaction timestamp |
| source\_sequence | Ordering information |
| transaction\_id | Source transaction identifier |
| kafka\_partition | Kafka partition |
| kafka\_offset | Kafka offset |
| ingestion\_ts | Warehouse arrival timestamp |

**Never confuse source commit time with ingestion time.**

Source commit time represents when a business change occurred. Ingestion time represents when the platform received it.

For ordering, prefer source log sequence numbers, transaction ordering metadata or a documented source-specific version. Timestamps alone may not uniquely order events.

## 2.2 CDC Deduplication

Problem: The same CDC event arrives multiple times.

Solution: Use a deterministic event identity and `ROW_NUMBER()`.

sql

```sql
WITH ranked_events AS (
    SELECT
        event_id,
        customer_id,
        city,
        operation,
        source_commit_ts,
        source_sequence,
        ingestion_ts,

        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY ingestion_ts DESC
        ) AS rn

    FROM STG_CUSTOMER_CDC
)

SELECT *
FROM ranked_events
WHERE rn = 1;
```

**Explanation:**

- Partition by the unique event identifier.
- Rank repeated deliveries.
- Keep one canonical copy.
- Preserve the original event in the raw layer for auditing.

If event IDs are unavailable, use a stable source event key. Do not blindly deduplicate by business key because multiple legitimate changes to the same customer may occur.

## 2.3 SCD Type 1 – Latest State Only

SCD Type 1 overwrites old attribute values.

Example:

| customer\_id | city |
| --- | --- |
| 101 | Kolkata |

After update:

| customer\_id | city |
| --- | --- |
| 101 | Delhi |

### Production SQL

First deduplicate source events so each target key has no more than one source row.

sql

```sql
MERGE INTO DIM_CUSTOMER_CURRENT T
USING (
    SELECT
        customer_id,
        city,
        source_commit_ts,
        source_sequence
    FROM STG_CUSTOMER_CDC
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY customer_id
        ORDER BY source_commit_ts DESC, source_sequence DESC
    ) = 1
) S
ON T.customer_id = S.customer_id

WHEN MATCHED AND (
    S.source_commit_ts > T.updated_at
    OR (
        S.source_commit_ts = T.updated_at
        AND S.source_sequence > T.source_sequence
    )
)
THEN UPDATE SET
    T.city = S.city,
    T.updated_at = S.source_commit_ts,
    T.source_sequence = S.source_sequence

WHEN NOT MATCHED THEN
INSERT (
    customer_id,
    city,
    updated_at,
    source_sequence
)
VALUES (
    S.customer_id,
    S.city,
    S.source_commit_ts,
    S.source_sequence
);
```

**Important:** A source timestamp and sequence must be compared consistently. The example assumes sequence values are comparable within the relevant source ordering domain.

### SCD Type 1 use cases

- Current customer address.
- Current product description.
- Current account status.
- Current employee department.

Historical changes are intentionally not retained in the target dimension.

## 2.4 SCD Type 2 – Complete Historical Tracking

SCD Type 2 preserves historical versions.

Example:

| customer\_sk | customer\_id | city | start\_date | end\_date | current |
| --- | --- | --- | --- | --- | --- |
| 1 | 101 | Kolkata | Jan 1 | Feb 10 | FALSE |
| 2 | 101 | Delhi | Feb 10 | Apr 15 | FALSE |
| 3 | 101 | Mumbai | Apr 15 | NULL | TRUE |

### Table creation

sql

```sql
CREATE TABLE DIM_CUSTOMER (
    customer_sk NUMBER AUTOINCREMENT,
    customer_id NUMBER NOT NULL,
    customer_name VARCHAR,
    city VARCHAR,
    tier VARCHAR,
    effective_start_ts TIMESTAMP_NTZ,
    effective_end_ts TIMESTAMP_NTZ,
    is_current BOOLEAN,
    record_hash VARCHAR,
    source_sequence NUMBER,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### SCD Type 2 implementation – New version detection

sql

```sql
CREATE OR REPLACE TEMP TABLE STG_CUSTOMER_DEDUP AS
SELECT *
FROM STG_CUSTOMERS
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY customer_id
    ORDER BY updated_at DESC, source_sequence DESC
) = 1;
```

The staging operation creates one deterministic latest record per business key. This pattern is suitable for current-state ingestion, not complete historical event reconstruction.

### Step 1: Expire changed current records

sql

```sql
UPDATE DIM_CUSTOMER T
SET
    effective_end_ts = S.updated_at,
    is_current = FALSE
FROM STG_CUSTOMER_DEDUP S
WHERE T.customer_id = S.customer_id
  AND T.is_current = TRUE
  AND (
      T.city IS DISTINCT FROM S.city
      OR T.tier IS DISTINCT FROM S.tier
      OR T.customer_name IS DISTINCT FROM S.customer_name
  );
```

### Step 2: Insert new or changed records

sql

```sql
INSERT INTO DIM_CUSTOMER (
    customer_id,
    customer_name,
    city,
    tier,
    effective_start_ts,
    effective_end_ts,
    is_current,
    source_sequence
)
SELECT
    S.customer_id,
    S.customer_name,
    S.city,
    S.tier,
    S.updated_at,
    NULL,
    TRUE,
    S.source_sequence
FROM STG_CUSTOMER_DEDUP S
WHERE NOT EXISTS (
    SELECT 1
    FROM DIM_CUSTOMER T
    WHERE T.customer_id = S.customer_id
      AND T.is_current = TRUE
);
```

**Explanation:**

- First expire the old version.
- Then insert the new version.
- Existing unchanged customers are not duplicated.
- Run both operations in a controlled transaction or equivalent atomic orchestration.
- Add concurrency protection and batch idempotency.

This simplified pattern handles current-state changes. For historical backfills, multiple changes per business key in one batch, or out-of-order CDC, use a complete timeline reconstruction process instead.

## 2.5 Handling Late-Arriving CDC Events

Example:

| Event | Source timestamp | Arrival |
| --- | --- | --- |
| A | 10:00 | 10:01 |
| B | 11:00 | 11:01 |
| C | 10:30 | 12:00 |

Event C arrives late but belongs between A and B.

For SCD Type 1, the current state must remain B.

For SCD Type 2, C may need to be inserted between A and B.

### Timeline reconstruction

sql

```sql
WITH timeline AS (
    SELECT
        customer_id,
        city,
        source_commit_ts,
        source_sequence
    FROM STG_CUSTOMER_CDC

    UNION ALL

    SELECT
        customer_id,
        city,
        effective_start_ts,
        source_sequence
    FROM DIM_CUSTOMER
    WHERE customer_id = 101
),

canonical_events AS (
    SELECT *
    FROM timeline
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY customer_id, source_commit_ts, source_sequence
        ORDER BY source_commit_ts
    ) = 1
),

restitched AS (
    SELECT
        customer_id,
        city,
        source_commit_ts AS effective_start_ts,

        LEAD(source_commit_ts) OVER (
            PARTITION BY customer_id
            ORDER BY source_commit_ts, source_sequence
        ) AS effective_end_ts

    FROM canonical_events
)

SELECT
    *,
    effective_end_ts IS NULL AS is_current
FROM restitched;
```

**Production considerations:**

1. Define the event ordering rule.
2. Resolve multiple events with identical timestamps.
3. Preserve deletes as tombstone events.
4. Rebuild only affected business keys.
5. Validate that intervals do not overlap.
6. Replace affected history atomically.
7. Ensure surrogate-key and downstream fact-table implications are addressed.

Do not use this simplified query as a complete production SCD2 loader: it must also compare attributes, identify no-op events, handle deletes and apply a controlled replacement strategy.

## 2.6 CDC Hard Deletes

A DELETE event must not be treated as an ordinary missing record.

Recommended design:

sql

```sql
MERGE INTO CUSTOMER_CURRENT T
USING STG_CUSTOMER_CDC S
ON T.customer_id = S.customer_id

WHEN MATCHED
 AND S.operation = 'DELETE'
 AND S.source_sequence > T.source_sequence
THEN UPDATE SET
    T.is_deleted = TRUE,
    T.deleted_at = S.source_commit_ts,
    T.source_sequence = S.source_sequence

WHEN MATCHED
 AND S.operation IN ('INSERT', 'UPDATE')
 AND S.source_sequence > T.source_sequence
THEN UPDATE SET
    T.city = S.city,
    T.is_deleted = FALSE,
    T.updated_at = S.source_commit_ts,
    T.source_sequence = S.source_sequence

WHEN NOT MATCHED AND S.operation <> 'DELETE'
THEN INSERT (
    customer_id, city, updated_at, source_sequence, is_deleted
)
VALUES (
    S.customer_id, S.city, S.source_commit_ts, S.source_sequence, FALSE
);
```

A production implementation should also handle a delete arriving before an older insert. Tombstones or a separate event ledger prevent resurrection of deleted records.

---

# PART 3 – SNOWFLAKE COMPLETE TECHNICAL HANDBOOK

## 3.1 Snowflake Architecture

Snowflake has three primary architectural layers:

### Storage layer

- Stores compressed data in micro-partitions.
- Maintains partition metadata.
- Supports automatic micro-partition pruning.
- Separates physical storage from compute.

### Compute layer

- Virtual warehouses execute SQL.
- Different warehouses can run independently.
- Warehouses can scale vertically or horizontally.

### Cloud services layer

- Authentication and access control.
- Query compilation and optimization.
- Metadata and transaction management.
- Infrastructure coordination.

### Snowflake architecture diagram

text

```text
               CLOUD SERVICES
          Authentication / Metadata
                    |
       +------------+------------+
       |            |            |
    Warehouse A  Warehouse B  Warehouse C
       |            |            |
       +------------+------------+
                    |
               STORAGE LAYER
             Micro-Partitions
                    |
               Cloud Storage
```

## 3.2 Snowflake Objects

| Object | Purpose |
| --- | --- |
| Database | Logical data container |
| Schema | Groups database objects |
| Table | Persistent structured storage |
| View | Stored SQL definition |
| Secure View | Controlled data exposure |
| Materialized View | Maintained precomputed result |
| Stage | File storage reference |
| File Format | Defines file parsing rules |
| Stream | Change tracking over table changes |
| Task | Scheduled or event-driven SQL execution |
| Pipe | Continuous file ingestion |
| Warehouse | Query compute resource |
| Dynamic Table | Declarative, automatically refreshed transformation |

## 3.3 Database and Schema Setup

sql

```sql
CREATE DATABASE IF NOT EXISTS ENTERPRISE_DW;

CREATE SCHEMA IF NOT EXISTS ENTERPRISE_DW.RAW;
CREATE SCHEMA IF NOT EXISTS ENTERPRISE_DW.STAGING;
CREATE SCHEMA IF NOT EXISTS ENTERPRISE_DW.CORE;
CREATE SCHEMA IF NOT EXISTS ENTERPRISE_DW.MARTS;

CREATE WAREHOUSE IF NOT EXISTS ETL_WH
WITH
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

CREATE WAREHOUSE IF NOT EXISTS BI_WH
WITH
    WAREHOUSE_SIZE = 'LARGE'
    AUTO_SUSPEND = 120
    AUTO_RESUME = TRUE;
```

**Explanation:** Separate ingestion, transformation and reporting compute to reduce workload contention.

## 3.4 Virtual Warehouse Optimization

Two different problems require different solutions.

**Problem A: Individual query is slow**

Possible causes:

- Insufficient memory.
- Expensive joins.
- Large scans.
- Data skew.
- Disk spilling.
- Poor partition pruning.

Possible solution:

- Optimize SQL.
- Reduce scanned data.
- Improve joins.
- Scale warehouse vertically if compute is the bottleneck.

**Problem B: Queries are queued**

Possible causes:

- Too many concurrent workloads.
- Warehouse capacity exhausted.
- Long-running queries occupying resources.

Possible solution:

- Multi-cluster warehouses.
- Workload isolation.
- Query scheduling.
- Resource monitoring.

### Multi-cluster warehouse

sql

```sql
CREATE OR REPLACE WAREHOUSE BI_MULTI_WH
WITH
    WAREHOUSE_SIZE = 'MEDIUM'
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 4
    SCALING_POLICY = 'STANDARD'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;
```

Multi-cluster warehouses primarily address concurrency. Increasing warehouse size primarily addresses individual query resource needs. These are not interchangeable fixes.

## 3.5 Query Profile and Performance Analysis

sql

```sql
SELECT
    QUERY_ID,
    QUERY_TEXT,
    EXECUTION_TIME,
    COMPILATION_TIME,
    QUEUED_PROVISIONING_TIME,
    QUEUED_OVERLOAD_TIME,
    BYTES_SCANNED,
    ROWS_PRODUCED,
    PARTITIONS_SCANNED,
    PARTITIONS_TOTAL
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME >= DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY EXECUTION_TIME DESC;
```

**Important metrics:**

- Bytes scanned: data read by the query.
- Partitions scanned: micro-partitions accessed.
- Partitions total: total available partitions.
- Queued overload time: waiting due to workload pressure.
- Execution time: actual execution duration.
- Compilation time: query compilation overhead.

Query Profile is particularly useful for identifying:

- Table scans.
- Join explosions.
- Local and remote disk spilling.
- Expensive aggregation.
- Poor partition pruning.

## 3.6 Micro-Partition Pruning

Snowflake automatically creates micro-partitions and maintains metadata such as minimum and maximum column values.

Bad:

sql

```sql
SELECT *
FROM SALES_FACT
WHERE YEAR(ORDER_DATE) = 2025;
```

Better:

sql

```sql
SELECT *
FROM SALES_FACT
WHERE ORDER_DATE >= '2025-01-01'
  AND ORDER_DATE < '2026-01-01';
```

The second form exposes a direct range predicate and is generally easier for the optimizer to prune.

## 3.7 Clustering

sql

```sql
ALTER TABLE SALES_FACT
CLUSTER BY (ORDER_DATE, REGION_ID);
```

Check:

sql

```sql
SELECT SYSTEM$CLUSTERING_INFORMATION(
    'ENTERPRISE_DW.CORE.SALES_FACT'
);
```

Use clustering when:

- Tables are very large.
- Queries repeatedly filter on selected dimensions.
- Natural micro-partition organization is insufficient.
- Query Profile shows meaningful pruning opportunities.

Avoid adding clustering keys to every table. Clustering has maintenance costs and should be justified by workload measurements.

## 3.8 Search Optimization Service

sql

```sql
ALTER TABLE CUSTOMER
ADD SEARCH OPTIMIZATION ON EQUALITY(EMAIL);
```

Suitable use cases:

- Selective point lookups.
- High-selectivity equality predicates.
- Certain supported substring and semi-structured searches.

It is not a universal replacement for clustering or partition pruning. Evaluate the supported predicates, storage overhead and maintenance costs.

## 3.9 Snowflake File Loading Using COPY INTO

sql

```sql
CREATE OR REPLACE FILE FORMAT JSON_FORMAT
TYPE = JSON
STRIP_OUTER_ARRAY = TRUE;

CREATE OR REPLACE STAGE S3_ORDERS_STAGE
URL = 's3://company-data/bronze/orders/'
STORAGE_INTEGRATION = S3_INTEGRATION
FILE_FORMAT = JSON_FORMAT;

COPY INTO RAW_ORDERS
FROM @S3_ORDERS_STAGE
ON_ERROR = 'CONTINUE';
```

**Explanation:**

1. Define a reusable file format.
2. Define an external stage.
3. Use a storage integration for secure access.
4. Execute COPY INTO.
5. Inspect rejected files and load history.

Avoid hardcoding AWS access keys in stage definitions or pipeline code.

## 3.10 Snowpipe

Snowpipe supports continuous file ingestion from cloud storage through event notifications.

sql

```sql
CREATE OR REPLACE PIPE ORDERS_PIPE
AUTO_INGEST = TRUE
AS
COPY INTO RAW_ORDERS
FROM @S3_ORDERS_STAGE
FILE_FORMAT = (FORMAT_NAME = JSON_FORMAT);
```

Architecture:

text

```text
Application
    |
    v
Amazon S3
    |
S3 Event Notification
    |
Cloud Queue
    |
Snowpipe
    |
Snowflake RAW Table
```

Snowpipe is designed for continuous file ingestion, not as a replacement for every low-latency event-streaming architecture.

## 3.11 Snowflake Streams and Tasks

Streams track changes to supported source objects.

sql

```sql
CREATE OR REPLACE STREAM CUSTOMER_STREAM
ON TABLE RAW_CUSTOMERS;
```

Task:

sql

```sql
CREATE OR REPLACE TASK PROCESS_CUSTOMER_CHANGES
WAREHOUSE = ETL_WH
SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('CUSTOMER_STREAM')
AS
MERGE INTO CUSTOMER_CURRENT T
USING (
    SELECT *
    FROM CUSTOMER_STREAM
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY CUSTOMER_ID
        ORDER BY UPDATED_AT DESC
    ) = 1
) S
ON T.CUSTOMER_ID = S.CUSTOMER_ID
WHEN MATCHED THEN UPDATE SET
    T.CITY = S.CITY,
    T.UPDATED_AT = S.UPDATED_AT
WHEN NOT MATCHED THEN INSERT (
    CUSTOMER_ID, CITY, UPDATED_AT
)
VALUES (
    S.CUSTOMER_ID, S.CITY, S.UPDATED_AT
);
```

Enable:

sql

```sql
ALTER TASK PROCESS_CUSTOMER_CHANGES RESUME;
```

Production considerations:

- A stream tracks changes, not an unlimited historical event log.
- Streams have offsets and consumption behavior.
- Task failures and retries must be monitored.
- Deduplicate source records before MERGE.
- Handle DELETE records explicitly.
- Design consumers to be restartable and idempotent.

## 3.12 Dynamic Tables

Dynamic Tables allow declarative transformation pipelines with target freshness.

sql

```sql
CREATE OR REPLACE DYNAMIC TABLE CUSTOMER_SALES_SUMMARY
TARGET_LAG = '5 MINUTES'
WAREHOUSE = ETL_WH
AS
SELECT
    CUSTOMER_ID,
    COUNT(*) AS TOTAL_ORDERS,
    SUM(ORDER_AMOUNT) AS TOTAL_REVENUE
FROM FACT_ORDERS
GROUP BY CUSTOMER_ID;
```

They are useful when Snowflake-managed refresh orchestration fits the workload.

Choose among:

- dbt incremental models for code-centric transformation and deployment control.
- Streams and Tasks for explicit change-processing logic.
- Dynamic Tables for declarative managed refresh.
- Snowpipe for continuous file ingestion.

These technologies can coexist.

## 3.13 Time Travel and Fail-safe

Time Travel supports historical querying and recovery within the configured retention period.

sql

```sql
SELECT *
FROM CUSTOMER
AT (OFFSET => -3600);
```

Recover a dropped table:

sql

```sql
UNDROP TABLE CUSTOMER;
```

Time Travel is not a substitute for a long-term independent backup strategy. Fail-safe is a separate Snowflake recovery mechanism and is not an ordinary user-operated query interface.

## 3.14 Zero-Copy Cloning

sql

```sql
CREATE TABLE CUSTOMER_DEV
CLONE CUSTOMER_PROD;
```

Useful for:

- Testing.
- Development.
- Reproducing production defects.
- Data migration validation.

Cloning initially shares underlying storage references; subsequent changes create additional storage as needed.

## 3.15 RBAC and Data Security

Recommended hierarchy:

text

```text
ACCOUNTADMIN
     |
SECURITYADMIN
     |
SYSADMIN
     |
+----------------------+
|                      |
ETL_ROLE              BI_ROLE
|                      |
RAW / CORE             MARTS
```

Example:

sql

```sql
CREATE ROLE IF NOT EXISTS DATA_ENGINEER_ROLE;

GRANT USAGE ON DATABASE ENTERPRISE_DW
TO ROLE DATA_ENGINEER_ROLE;

GRANT USAGE ON SCHEMA ENTERPRISE_DW.CORE
TO ROLE DATA_ENGINEER_ROLE;

GRANT SELECT, INSERT, UPDATE, DELETE
ON ALL TABLES IN SCHEMA ENTERPRISE_DW.CORE
TO ROLE DATA_ENGINEER_ROLE;
```

Use:

- Least privilege.
- Dedicated service users.
- Key-pair authentication where appropriate.
- Network policies.
- Masking policies.
- Row access policies.
- Secure views.
- Separate development and production roles.

---

# PART 4 – DBT CORE ENGINEERING

## 4.1 dbt Architecture

text

```text
SOURCES
   |
   v
STAGING
   |
   v
INTERMEDIATE
   |
   v
CORE FACTS / DIMENSIONS
   |
   v
MARTS
   |
   v
BI REPORTING
```

### Materialization comparison

| Materialization | Physical object | Use case |
| --- | --- | --- |
| View | View | Lightweight staging |
| Table | Table | Full refresh models |
| Incremental | Table | Large evolving datasets |
| Ephemeral | No standalone object | Small reusable logic |
| Snapshot | Historical records | Tracking mutable source records |

## 4.2 Project Structure

text

```text
dbt_project/
│
├── dbt_project.yml
├── packages.yml
├── profiles.yml
│
├── models/
│   ├── staging/
│   │   ├── sources.yml
│   │   ├── stg_customers.sql
│   │   └── stg_orders.sql
│   │
│   ├── intermediate/
│   │   └── int_order_enrichment.sql
│   │
│   ├── core/
│   │   ├── dim_customer.sql
│   │   └── fct_orders.sql
│   │
│   └── marts/
│       └── mart_monthly_revenue.sql
│
├── snapshots/
├── macros/
├── tests/
└── seeds/
```

## 4.3 Source Configuration

yaml

```yaml
version: 2

sources:
  - name: raw_erp
    database: ENTERPRISE_DW
    schema: RAW

    tables:
      - name: orders
        description: Raw ERP order records

      - name: customers
        description: Raw customer records
```

Usage:

sql

```sql
SELECT *
FROM {{ source('raw_erp', 'orders') }}
```

## 4.4 Staging Model

sql

```sql
{{ config(materialized='view') }}

SELECT
    TRIM(ORDER_ID) AS ORDER_ID,
    TRY_TO_NUMBER(CUSTOMER_ID) AS CUSTOMER_ID,
    TRY_TO_TIMESTAMP_NTZ(ORDER_DATE) AS ORDER_DATE,
    UPPER(TRIM(STATUS)) AS ORDER_STATUS,
    TRY_TO_DECIMAL(TOTAL_AMOUNT, 18, 2) AS ORDER_AMOUNT,
    _ETL_LOADED_AT AS INGESTED_AT

FROM {{ source('raw_erp', 'orders') }}

WHERE ORDER_ID IS NOT NULL
```

Purpose:

- Rename columns.
- Standardize types.
- Clean simple formatting.
- Avoid unnecessary business aggregation.
- Preserve source lineage.

## 4.5 Incremental Model

sql

```sql
{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns'
    )
}}

SELECT
    ORDER_ID,
    CUSTOMER_ID,
    ORDER_DATE,
    ORDER_STATUS,
    ORDER_AMOUNT,
    INGESTED_AT

FROM {{ ref('stg_orders') }}

{% if is_incremental() %}

WHERE INGESTED_AT >= (
    SELECT DATEADD(
        HOUR,
        -6,
        COALESCE(MAX(INGESTED_AT), '1900-01-01'::TIMESTAMP_NTZ)
    )
    FROM {{ this }}
)

{% endif %}
```

### Explanation

`is_incremental()` checks whether the model is executing as an incremental run rather than its initial build.

`{{ this }}` refers to the target relation.

The six-hour lookback reprocesses recent records to capture late arrivals.

### Important limitation

A lookback window is not sufficient for all CDC systems. An event that arrives after six hours can still be missed.

For reliable pipelines:

- Track source offsets or ingestion watermarks.
- Maintain a replayable raw layer.
- Reprocess affected business keys.
- Provide periodic backfills.
- Monitor late-arrival distributions.

Also ensure that source deduplication produces one row per `unique_key` before a Snowflake MERGE.

## 4.6 dbt Incremental Strategies

### Merge

sql

```sql
{{ config(
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='merge'
) }}
```

Suitable for upserts.

### Delete and insert

sql

```sql
{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='delete+insert'
) }}
```

Useful when replacement of affected keys is preferable.

### Append

sql

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}
```

Suitable for immutable append-only event data with separate deduplication controls.

### Microbatch

For supported dbt versions and adapters, microbatch processing can divide time-series data into discrete event-time batches. Verify adapter capabilities and configuration before using it.

## 4.7 dbt Snapshots

Snapshots track historical changes in mutable source records.

sql

```sql
{% snapshot customer_snapshot %}

{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=True
    )
}}

SELECT *
FROM {{ source('raw_erp', 'customers') }}

{% endsnapshot %}
```

Timestamp strategy:

- Detects changes using a reliable updated timestamp.
- Creates historical versions.
- Maintains validity metadata.

Check the exact hard-delete behavior supported by your installed dbt version. Snapshot invalidation and physical deletion of downstream records are not the same thing.

## 4.8 dbt Tests

yaml

```yaml
version: 2

models:
  - name: fct_orders

    columns:
      - name: order_id
        data_tests:
          - unique
          - not_null

      - name: customer_id
        data_tests:
          - not_null

      - name: order_status
        data_tests:
          - accepted_values:
              arguments:
                values:
                  - PENDING
                  - COMPLETED
                  - CANCELLED
                  - REFUNDED
```

## 4.9 Custom dbt Test – Referential Integrity

File: `tests/assert_orders_have_customers.sql`

sql

```sql
SELECT
    O.ORDER_ID,
    O.CUSTOMER_ID

FROM {{ ref('fct_orders') }} O

LEFT JOIN {{ ref('dim_customer') }} C
    ON O.CUSTOMER_ID = C.CUSTOMER_ID

WHERE C.CUSTOMER_ID IS NULL
```

A successful test returns zero failing rows.

## 4.10 dbt Macros

File: `macros/generate_surrogate_key.sql`

sql

```sql
{% macro generate_customer_key(customer_id) %}

    MD5(
        COALESCE(
            CAST({{ customer_id }} AS VARCHAR),
            'UNKNOWN'
        )
    )

{% endmacro %}
```

Usage:

sql

```sql
SELECT
    {{ generate_customer_key('CUSTOMER_ID') }} AS CUSTOMER_SK
FROM {{ ref('stg_customers') }}
```

For composite keys, use dbt's maintained surrogate-key utilities where available. Ensure null handling and delimiter behavior are consistent.

## 4.11 dbt Commands

bash

```bash
dbt debug

dbt deps

dbt parse

dbt compile

dbt run

dbt test

dbt build

dbt snapshot

dbt source freshness

dbt run --select stg_orders

dbt run --select +fct_orders

dbt run --select fct_orders+

dbt build --select state:modified+

dbt docs generate

dbt docs serve
```

**Important commands:**

- `dbt compile`: Compile models without executing transformations.
- `dbt run`: Execute selected models.
- `dbt test`: Execute selected tests.
- `dbt build`: Build selected resources and execute applicable tests.
- `dbt snapshot`: Execute snapshots.
- `dbt deps`: Install package dependencies.
- `dbt source freshness`: Evaluate source freshness against configured thresholds.

---

# PART 5 – PRODUCTION PYTHON ENGINEERING

## 5.1 Python Production Standards

A production ingestion framework should include:

- Structured logging.
- Type hints.
- Configuration management.
- Retry mechanisms.
- API pagination.
- Authentication.
- Exception classification.
- Timeout handling.
- Idempotency.
- Atomic file operations.
- Unit tests.
- Observability.

## 5.2 API Extraction Framework

python

```python
import logging
import requests

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


logger = logging.getLogger(__name__)


class APIClient:

    def __init__(self, base_url, token, timeout=30):

        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

        self.session = requests.Session()

        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Accept": "application/json"
        })

        retry = Retry(
            total=4,
            connect=4,
            read=4,
            status=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            respect_retry_after_header=True
        )

        adapter = HTTPAdapter(max_retries=retry)

        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def get(self, endpoint, params=None):

        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        try:

            response = self.session.get(
                url,
                params=params,
                timeout=self.timeout
            )

            response.raise_for_status()

            return response.json()

        except requests.exceptions.RequestException:

            logger.exception(
                "API request failed",
                extra={"endpoint": endpoint}
            )

            raise

    def close(self):
        self.session.close()
```

### Explanation

- Session reuses HTTP connections.
- Timeouts prevent indefinitely blocked requests.
- Retry handles transient failures.
- HTTP 429 and selected 5xx responses are retried.
- `raise_for_status()` raises an exception for HTTP failures.
- Exceptions are logged and propagated.

Do not retry every HTTP 4xx response. Authentication and validation failures generally require corrective action.

## 5.3 API Pagination

python

```python
def fetch_all_pages(client, endpoint):

    page = 1
    records = []

    while True:

        payload = client.get(
            endpoint,
            params={
                "page": page,
                "page_size": 1000
            }
        )

        batch = payload.get("data", [])

        records.extend(batch)

        if not payload.get("has_next", False):
            break

        page += 1

    return records
```

For high-volume APIs, stream records to durable storage page by page rather than retaining the entire extraction in memory.

## 5.4 Safe File Writing

python

```python
import json
import os

from pathlib import Path


def save_json(data, output_path):

    path = Path(output_path)

    path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    temporary_path = path.with_suffix(
        path.suffix + ".tmp"
    )

    with temporary_path.open(
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            data,
            file,
            ensure_ascii=False,
            indent=2
        )

        file.flush()
        os.fsync(file.fileno())

    os.replace(
        temporary_path,
        path
    )

    return path
```

This protects against partially written files on local filesystems. For S3, use immutable object keys, completed multipart uploads, and a manifest or commit marker rather than assuming local rename semantics.

## 5.5 Python Database Connection Management

python

```python
import snowflake.connector


def execute_query(config, sql, params=None):

    conn = snowflake.connector.connect(**config)

    try:

        with conn.cursor() as cursor:

            cursor.execute(sql, params)

            return cursor.fetchall()

    finally:

        conn.close()
```

Use parameterized SQL for values. Never build SQL by directly interpolating user-controlled input.

---

# PART 6 – PYSPARK DISTRIBUTED PROCESSING

## 6.1 Spark Architecture

text

```text
                 DRIVER
                   |
              Spark Session
                   |
              DAG Scheduler
                   |
             Task Scheduler
                   |
       +-----------+-----------+
       |           |           |
    Executor 1  Executor 2  Executor 3
       |           |           |
    Partition   Partition   Partition
       |           |           |
       +-----------+-----------+
                   |
              Storage Layer
             S3 / Parquet
```

### Driver

- Builds logical plans.
- Coordinates execution.
- Schedules jobs and tasks.
- Collects execution metadata.

### Executors

- Execute distributed tasks.
- Process data partitions.
- Cache and shuffle intermediate data.

## 6.2 Narrow and Wide Transformations

Narrow:

- select
- filter
- withColumn
- map

Wide:

- groupBy
- distinct
- repartition
- many joins
- orderBy

Wide transformations commonly require shuffles.

## 6.3 Optimized PySpark Example

python

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    broadcast,
    current_timestamp,
    to_date
)


spark = (
    SparkSession.builder
    .appName("EnterpriseSalesPipeline")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .getOrCreate()
)


sales = (
    spark.read
    .parquet("s3://company-data/bronze/sales/")
)


customers = (
    spark.read
    .parquet("s3://company-data/bronze/customers/")
)


sales_filtered = (
    sales
    .filter(col("customer_id").isNotNull())
    .filter(col("amount") > 0)
    .select(
        "transaction_id",
        "customer_id",
        "transaction_timestamp",
        "amount"
    )
)


enriched = (
    sales_filtered
    .join(
        broadcast(customers.select(
            "customer_id",
            "customer_name",
            "region"
        )),
        on="customer_id",
        how="left"
    )
    .withColumn(
        "transaction_date",
        to_date(col("transaction_timestamp"))
    )
    .withColumn(
        "processed_at",
        current_timestamp()
    )
)


(
    enriched.write
    .mode("append")
    .partitionBy("transaction_date")
    .parquet("s3://company-data/silver/sales/")
)


spark.stop()
```

## 6.4 Broadcast Joins

python

```python
from pyspark.sql.functions import broadcast

result = large_fact.join(
    broadcast(small_dimension),
    on="customer_id",
    how="left"
)
```

Broadcast is suitable when the build-side dataset is sufficiently small to fit safely in executor memory.

The often-quoted 100 MB threshold is only a rough guideline. Actual suitability depends on memory, serialization overhead, cluster resources and Spark configuration.

## 6.5 Data Skew

Problem:

One customer has 50% of the records while other customers have very few.

A normal join or aggregation can leave one partition processing far more data than others.

Solutions:

- Adaptive Query Execution.
- Skew join handling.
- Salting.
- Pre-aggregation.
- Filtering early.
- Repartitioning using suitable keys.

### Salting example

python

```python
from pyspark.sql.functions import (
    col,
    floor,
    rand,
    explode,
    array,
    lit
)

salted_fact = fact.withColumn(
    "salt",
    floor(rand() * 10)
)

salted_dimension = dimension.withColumn(
    "salt",
    explode(array(*[
        lit(i) for i in range(10)
    ]))
)

result = salted_fact.join(
    salted_dimension,
    ["customer_id", "salt"],
    "left"
).drop("salt")
```

Use salting selectively. It increases data replication and should be applied only when skew is demonstrated.

## 6.6 Repartition vs Coalesce

python

```python
df.repartition(200, "customer_id")
```

Repartition generally redistributes records and causes a shuffle.

python

```python
df.coalesce(20)
```

Coalesce reduces partitions, often without a full shuffle.

Use repartition when better distribution is required; use coalesce to reduce output partition count when a full redistribution is unnecessary.

## 6.7 Spark Optimization Checklist

- Define explicit schemas.
- Filter early.
- Select only required columns.
- Avoid unnecessary `collect()`.
- Avoid Python UDFs when native Spark functions are available.
- Broadcast genuinely small dimensions.
- Inspect query plans with `explain(True)`.
- Enable AQE where supported.
- Monitor shuffle spill.
- Tune output file sizes.
- Avoid excessive tiny files.
- Partition by useful low-to-moderate cardinality columns.
- Avoid partitioning by highly unique identifiers.

---

# PART 7 – REAL-TIME PROCESSING AND KAFKA

## 7.1 Kafka Fundamentals

| Component | Explanation |
| --- | --- |
| Producer | Publishes messages |
| Consumer | Reads messages |
| Topic | Logical event stream |
| Partition | Parallel unit of ordering |
| Offset | Position in partition |
| Consumer group | Coordinated consumers |
| Broker | Kafka server |
| Retention | How long records remain available |

### Important ordering principle

Kafka ordering is guaranteed within a partition, not across an entire topic.

Use a business key such as `customer_id` or `order_id` as the message key when ordering per entity is required.

## 7.2 Producer Example

python

```python
import json

from kafka import KafkaProducer


producer = KafkaProducer(
    bootstrap_servers=["broker:9092"],
    key_serializer=lambda key: key.encode("utf-8"),
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    acks="all",
    enable_idempotence=True
)


event = {
    "event_id": "evt-1001",
    "order_id": "ORD-1001",
    "event_type": "ORDER_CREATED"
}


future = producer.send(
    "orders",
    key=event["order_id"],
    value=event
)

metadata = future.get(timeout=30)

producer.flush()
producer.close()
```

## 7.3 Consumer Example

python

```python
import json

from kafka import KafkaConsumer


consumer = KafkaConsumer(
    "orders",
    bootstrap_servers=["broker:9092"],
    group_id="order-processing-group",
    enable_auto_commit=False,
    auto_offset_reset="earliest",
    value_deserializer=lambda value: json.loads(
        value.decode("utf-8")
    )
)

for message in consumer:

    event = message.value

    try:

        process_event_idempotently(event)

        consumer.commit()

    except Exception:

        # Do not commit an event that has not been durably processed.
        raise
```

A production consumer also needs a retry strategy, poison-message handling, dead-letter topic and controlled shutdown.

## 7.4 At-Least-Once vs At-Most-Once vs Exactly-Once

**At-most-once**

- Message may be lost.
- Usually commits before processing.

**At-least-once**

- Message is not acknowledged until processing succeeds.
- Duplicate delivery is possible.

**Exactly-once**

- Requires transactional or coordinated semantics within the relevant system boundaries.
- Does not automatically guarantee exactly-once behavior across Kafka, S3 and Snowflake together.

A practical enterprise design is usually at-least-once delivery plus idempotent writes and durable event deduplication.

## 7.5 Event-Time Processing and Watermarks

Event time is when the event occurred.

Processing time is when the processing engine handled it.

Watermarks help streaming engines manage late-arriving events and state retention.

python

```python
from pyspark.sql.functions import (
    col,
    window
)

aggregated = (
    streaming_events
    .withWatermark("event_timestamp", "30 minutes")
    .groupBy(
        window(
            col("event_timestamp"),
            "10 minutes"
        ),
        col("campaign_id")
    )
    .count()
)
```

A 30-minute watermark is a state-management policy, not a guarantee that all later events are rejected in every output mode. Understand the chosen engine's late-event semantics and replay requirements.

---

# PART 8 – AIRFLOW ORCHESTRATION

## 8.1 Airflow Architecture

text

```text
Scheduler
   |
   v
DAG Parsing
   |
   v
Task Scheduling
   |
   v
Executor
   |
   +------ Worker 1
   +------ Worker 2
   +------ Worker 3
   |
Metadata Database
   |
Webserver / UI
```

## 8.2 Production DAG

python

```python
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def validate_pipeline(**context):

    logical_date = context["logical_date"]

    print(
        f"Validating pipeline for {logical_date}"
    )


default_args = {
    "owner": "data_engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2)
}


with DAG(
    dag_id="enterprise_sales_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["sales", "snowflake", "dbt"]
) as dag:

    extract = BashOperator(
        task_id="extract_data",
        bash_command=(
            "python /opt/airflow/jobs/extract.py "
            "--business-date '{{ ds }}'"
        )
    )

    validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_pipeline
    )

    transform = BashOperator(
        task_id="dbt_build",
        bash_command=(
            "cd /opt/airflow/dbt_project && "
            "dbt build --target prod"
        )
    )

    reconcile = BashOperator(
        task_id="reconciliation",
        bash_command=(
            "python /opt/airflow/jobs/reconcile.py "
            "--business-date '{{ ds }}'"
        )
    )

    extract >> validate >> transform >> reconcile
```

## 8.3 Airflow Best Practices

1. Avoid network connections during DAG parsing.
2. Avoid heavy imports at module level.
3. Keep tasks idempotent.
4. Use logical dates and data intervals.
5. Set execution timeouts.
6. Use retries with appropriate backoff.
7. Use pools to control external system concurrency.
8. Use sensors carefully.
9. Prefer deferrable operators where supported.
10. Keep secrets in a secrets backend or connection store.
11. Make tasks observable.
12. Use task groups for complex workflows.

## 8.4 Airflow Catchup

`catchup=True` allows eligible historical intervals to be scheduled from the start date.

`catchup=False` prevents automatic scheduling of all missed historical intervals.

Historical backfills should be planned deliberately to avoid overwhelming source databases and warehouses.

---

# PART 9 – AWS DATA ENGINEERING

## 9.1 S3 Data Lake Architecture

text

```text
s3://enterprise-data/
    |
    +--- bronze/
    |      +--- customers/
    |      +--- orders/
    |
    +--- silver/
    |      +--- customers/
    |      +--- orders/
    |
    +--- gold/
           +--- sales_mart/
           +--- customer_360/
```

### Partitioned layout

text

```text
s3://enterprise-data/bronze/orders/
    year=2026/
        month=11/
            day=09/
                part-0001.parquet
```

Partition according to common filtering and access patterns. Avoid excessive partition cardinality.

## 9.2 Glue ETL

python

```python
from awsglue.context import GlueContext
from pyspark.context import SparkContext

from pyspark.sql.functions import (
    col,
    current_timestamp,
    to_date
)


sc = SparkContext.getOrCreate()

glue_context = GlueContext(sc)

spark = glue_context.spark_session


df = (
    spark.read
    .option("multiline", "false")
    .json("s3://enterprise-data/bronze/orders/")
)


cleaned = (
    df
    .filter(col("order_id").isNotNull())
    .withColumn(
        "order_date",
        to_date(col("order_timestamp"))
    )
    .withColumn(
        "processed_at",
        current_timestamp()
    )
)


(
    cleaned.write
    .mode("append")
    .partitionBy("order_date")
    .parquet("s3://enterprise-data/silver/orders/")
)
```

A production Glue job should also include Glue job bookmarks or an explicit ingestion manifest, schema validation, rejected-record handling, write idempotency and appropriate catalog updates.

## 9.3 AWS IAM

Use least privilege.

Example policy scope:

- Read from designated bronze prefixes.
- Write to designated silver prefixes.
- Access a specific Glue database.
- Retrieve only required secrets.
- Access only the necessary KMS keys.

Avoid broad `s3:*` and `glue:*` permissions for production workloads.

## 9.4 AWS Glue Data Catalog

The catalog stores:

- Database definitions.
- Table metadata.
- Column definitions.
- S3 locations.
- Partition metadata.
- Schema information.

Glue Catalog provides metadata; it does not automatically guarantee that every physical file conforms to the registered schema.

## 9.5 AWS DMS

AWS Database Migration Service can support:

- Full load.
- Full load plus CDC.
- Continuous replication from supported sources.
- Change capture into supported targets.

A typical architecture:

text

```text
Oracle
  |
AWS DMS Full Load
  |
AWS DMS CDC
  |
S3 / Kafka
  |
Snowflake
```

Monitor replication latency, task errors, source log retention and recovery checkpoints.

---

# PART 10 – DATA WAREHOUSING AND DIMENSIONAL MODELING

## 10.1 Star Schema

text

```text
              DIM_CUSTOMER
                    |
                    |
DIM_PRODUCT --- FACT_SALES --- DIM_DATE
                    |
                    |
                DIM_REGION
```

Fact tables contain measurable business events.

Dimension tables contain descriptive business attributes.

## 10.2 Fact Table Types

**Transaction fact:** One row per business event.

**Periodic snapshot:** One row per entity per reporting period.

**Accumulating snapshot:** One row per process with milestone dates updated over time.

Examples:

- Sales transaction.
- Daily inventory.
- Loan application lifecycle.

## 10.3 Surrogate Keys

Business key:

`customer_id = 101`

Surrogate key:

`customer_sk = 10001`

Surrogate keys allow multiple historical versions of one business entity.

## 10.4 Point-in-Time Fact Lookup

sql

```sql
SELECT
    F.SALE_ID,
    F.ORDER_DATE,
    D.CUSTOMER_SK,
    D.CITY,
    F.AMOUNT

FROM FACT_SALES F

JOIN DIM_CUSTOMER D
    ON F.CUSTOMER_ID = D.CUSTOMER_ID
   AND F.ORDER_DATE >= D.EFFECTIVE_START_TS
   AND F.ORDER_DATE < COALESCE(
       D.EFFECTIVE_END_TS,
       '9999-12-31'::TIMESTAMP_NTZ
   );
```

This associates a fact with the dimension version valid at the fact's business event time.

## 10.5 SCD Type 0, 1, 2 and 3

| Type | Behavior | Example |
| --- | --- | --- |
| Type 0 | Never change | Original registration date |
| Type 1 | Overwrite | Current customer email |
| Type 2 | Preserve all versions | Customer city history |
| Type 3 | Store limited previous values | Current and previous department |

---

# PART 11 – DATA QUALITY AND RECONCILIATION

## 11.1 Data Quality Dimensions

1. Completeness.
2. Accuracy.
3. Consistency.
4. Validity.
5. Uniqueness.
6. Timeliness.
7. Referential integrity.

## 11.2 Reconciliation SQL

sql

```sql
WITH SOURCE_METRICS AS (

    SELECT
        COUNT(*) AS SOURCE_COUNT,
        COALESCE(SUM(AMOUNT), 0) AS SOURCE_AMOUNT

    FROM STG_ORDERS

    WHERE CREATED_AT >= '2026-11-09'
      AND CREATED_AT < '2026-11-10'

),

TARGET_METRICS AS (

    SELECT
        COUNT(*) AS TARGET_COUNT,
        COALESCE(SUM(TOTAL_AMOUNT), 0) AS TARGET_AMOUNT

    FROM FACT_ORDERS

    WHERE ORDER_DATE >= '2026-11-09'
      AND ORDER_DATE < '2026-11-10'

)

SELECT
    SOURCE_COUNT,
    TARGET_COUNT,
    SOURCE_AMOUNT,
    TARGET_AMOUNT,

    SOURCE_COUNT - TARGET_COUNT AS COUNT_DIFFERENCE,

    SOURCE_AMOUNT - TARGET_AMOUNT AS AMOUNT_DIFFERENCE

FROM SOURCE_METRICS
CROSS JOIN TARGET_METRICS;
```

Use an agreed business grain and identical inclusion/exclusion rules on both sides. Count equality alone is not proof of correctness.

## 11.3 Duplicate Detection

sql

```sql
SELECT
    ORDER_ID,
    COUNT(*) AS RECORD_COUNT

FROM FACT_ORDERS

GROUP BY ORDER_ID

HAVING COUNT(*) > 1;
```

## 11.4 Null Validation

sql

```sql
SELECT COUNT(*) AS INVALID_RECORDS
FROM CUSTOMER
WHERE CUSTOMER_ID IS NULL;
```

## 11.5 Referential Integrity

sql

```sql
SELECT
    F.CUSTOMER_ID

FROM FACT_SALES F

LEFT JOIN DIM_CUSTOMER D
    ON F.CUSTOMER_SK = D.CUSTOMER_SK

WHERE D.CUSTOMER_SK IS NULL;
```

## 11.6 SCD2 Overlap Validation

sql

```sql
WITH CHECKS AS (

    SELECT
        CUSTOMER_ID,
        EFFECTIVE_START_TS,
        EFFECTIVE_END_TS,

        LAG(EFFECTIVE_END_TS) OVER (
            PARTITION BY CUSTOMER_ID
            ORDER BY EFFECTIVE_START_TS
        ) AS PREVIOUS_END_TS

    FROM DIM_CUSTOMER
)

SELECT *
FROM CHECKS
WHERE PREVIOUS_END_TS > EFFECTIVE_START_TS;
```

Also test:

- Exactly one current row per active customer.
- No invalid negative intervals.
- No duplicate event versions.
- No gaps where continuous validity is required.

---

# PART 12 – GIT, CI/CD AND DEPLOYMENT

## 12.1 Git Branching Strategy

text

```text
main
 |
 +--- feature/customer-dimension
 |
 +--- feature/order-fact
 |
 +--- bugfix/cdc-deduplication
```

Recommended workflow:

1. Create feature branch.
2. Develop SQL/Python/dbt changes.
3. Execute unit tests.
4. Run SQL linting.
5. Run dbt parse and compile.
6. Submit pull request.
7. Review code.
8. Deploy to development.
9. Execute integration tests.
10. Promote to production.

## 12.2 GitHub Actions Example

yaml

```yaml
name: Data Engineering CI

on:
  pull_request:
    branches:
      - main

jobs:

  test:

    runs-on: ubuntu-latest

    steps:

      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: |
          pip install pytest sqlfluff dbt-snowflake

      - name: Python tests
        run: pytest tests/

      - name: SQL lint
        run: sqlfluff lint models/ --dialect snowflake

      - name: dbt parse
        run: |
          cd dbt_project
          dbt parse --profiles-dir .
```

Production deployment should use protected environments, approved promotions, secret management and a controlled release process. Do not deploy every successful pull request directly into production.

## 12.3 Infrastructure as Code

Use Terraform or an equivalent tool to provision:

- S3 buckets.
- IAM roles.
- Glue databases and jobs.
- Snowflake warehouses.
- Snowflake databases and schemas.
- Storage integrations.
- Airflow infrastructure.
- Network and security policies.

Separate infrastructure provisioning from data transformation deployment.

---

# PART 13 – ENTERPRISE END-TO-END IMPLEMENTATION

## 13.1 Recommended Pipeline

text

```text
                 SOURCE SYSTEMS
               Oracle / APIs / ERP
                       |
              +--------+--------+
              |                 |
             CDC               Batch
              |                 |
          Kafka/DMS          Airflow
              |                 |
              +--------+--------+
                       |
                   AWS S3
                    BRONZE
                       |
                 Glue / Spark
                       |
                    SILVER
                       |
              Snowflake Ingestion
                       |
                 RAW SCHEMAS
                       |
                  dbt STAGING
                       |
                dbt INTERMEDIATE
                       |
              FACTS / DIMENSIONS
                       |
                  DATA MARTS
                       |
             DATA QUALITY CHECKS
                       |
                 RECONCILIATION
                       |
                BI / ANALYTICS
```

## 13.2 Production Metadata Tables

### Pipeline execution log

sql

```sql
CREATE TABLE ETL_AUDIT_LOG (
    RUN_ID VARCHAR,
    PIPELINE_NAME VARCHAR,
    SOURCE_NAME VARCHAR,
    START_TIME TIMESTAMP_NTZ,
    END_TIME TIMESTAMP_NTZ,
    STATUS VARCHAR,
    SOURCE_COUNT NUMBER,
    TARGET_COUNT NUMBER,
    ERROR_MESSAGE VARCHAR,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### Watermark table

sql

```sql
CREATE TABLE ETL_WATERMARK (
    PIPELINE_NAME VARCHAR,
    SOURCE_NAME VARCHAR,
    LAST_SUCCESSFUL_TIMESTAMP TIMESTAMP_NTZ,
    LAST_SUCCESSFUL_SEQUENCE VARCHAR,
    UPDATED_AT TIMESTAMP_NTZ
);
```

The watermark must advance only after the corresponding data is durably processed and validated.

### Dead-letter table

sql

```sql
CREATE TABLE CDC_REJECTED_EVENTS (
    EVENT_ID VARCHAR,
    SOURCE_NAME VARCHAR,
    ERROR_REASON VARCHAR,
    RAW_PAYLOAD VARIANT,
    INGESTED_AT TIMESTAMP_NTZ
);
```

## 13.3 Production Recovery Strategy

If a pipeline fails:

1. Identify the failed task.
2. Inspect source and target watermarks.
3. Check whether the previous transaction committed.
4. Inspect event IDs and batch IDs.
5. Determine whether partial output exists.
6. Replay the affected source interval.
7. Apply idempotent transformations.
8. Validate counts and financial totals.
9. Advance the watermark.
10. Record recovery details.

Never blindly rerun a failed pipeline without checking its commit and idempotency behavior.

---

# PART 14 – SENIOR DATA ENGINEER INTERVIEW QUESTIONS AND ANSWERS

## A. SNOWFLAKE – ARCHITECTURE AND PERFORMANCE

### Q1. Explain Snowflake architecture.

**Answer:**

Snowflake uses a multi-cluster shared-data architecture with three major layers:

- Storage: Compressed micro-partitioned data.
- Compute: Independent virtual warehouses.
- Cloud services: Authentication, metadata, optimization and transaction coordination.

The separation allows multiple compute workloads to access shared data independently.

### Q2. What is a micro-partition?

**Answer:**

A micro-partition is a Snowflake-managed immutable storage unit containing compressed table data and metadata.

Snowflake uses metadata such as minimum and maximum values to determine which partitions can be skipped.

This is called micro-partition pruning.

### Q3. What is the difference between scaling up and scaling out?

**Answer:**

Scaling up increases the size of a warehouse to provide more compute resources to individual queries.

Scaling out increases the number of clusters to handle concurrent workloads.

Use scale-up for resource-intensive queries and scale-out for concurrency pressure.

### Q4. What causes spilling?

**Answer:**

Spilling happens when intermediate query data exceeds available memory and is written to local or remote storage.

Typical causes:

- Large joins.
- Large aggregations.
- High-cardinality sorting.
- Insufficient warehouse resources.
- Poor query design.

Solutions include query optimization, reducing intermediate rows, improving joins and scaling compute when appropriate.

### Q5. What is clustering?

**Answer:**

Clustering improves physical data organization for workloads that repeatedly filter on particular columns.

Snowflake already performs automatic micro-partition management. Explicit clustering is useful when natural organization is insufficient for large tables and recurring access patterns.

### Q6. What is Search Optimization Service?

**Answer:**

SOS maintains search access structures for supported selective lookup patterns.

It can improve selective point-lookup performance but introduces storage and maintenance costs.

### Q7. What is a materialized view?

**Answer:**

A materialized view stores a maintained result of a supported query.

It is useful when expensive eligible queries are repeated frequently.

Trade-offs include maintenance cost, storage and SQL restrictions.

### Q8. What is the difference between Snowpipe and COPY INTO?

**Answer:**

COPY INTO is explicitly executed to load files.

Snowpipe automates continuous file ingestion triggered by cloud event notifications.

COPY INTO is useful for scheduled bulk loads and Snowpipe for continuous file arrival.

### Q9. What is the difference between Streams and Dynamic Tables?

**Answer:**

Streams expose tracked changes to supported source objects.

Dynamic Tables maintain a declarative transformation result according to a target-lag requirement.

Streams and Tasks provide more explicit control over procedural change processing, while Dynamic Tables provide managed refresh for supported SQL transformations.

### Q10. How do you troubleshoot a slow query?

**Answer:**

1. Open Query Profile.
2. Inspect bytes scanned.
3. Check partitions scanned versus total.
4. Inspect join operators.
5. Identify spilling.
6. Check queue time.
7. Review warehouse utilization.
8. Optimize SQL.
9. Compare execution metrics before and after.

### Q11. How do you handle warehouse concurrency?

**Answer:**

- Separate ETL and BI warehouses.
- Use multi-cluster warehouses.
- Set appropriate auto-suspend.
- Control workload concurrency.
- Monitor queued overload time.
- Schedule heavy jobs outside peak periods.

### Q12. What is Time Travel?

**Answer:**

Time Travel allows access to historical table states within the configured retention period. It can be used for historical queries, recovery and cloning.

### Q13. What is zero-copy cloning?

**Answer:**

It creates a new database object using shared underlying storage references initially, reducing the need to immediately copy all physical data.

### Q14. How do you secure PII?

**Answer:**

Use RBAC, masking policies, row access policies, secure views, least-privilege access, encryption and auditing.

---

## B. DBT – ADVANCED ENGINEERING

### Q15. What is the difference between `ref()` and `source()`?

**Answer:**

`source()` references an externally defined source table.

`ref()` references another dbt model, seed or supported dbt resource.

Both create dependency relationships in the dbt DAG.

### Q16. What is an incremental model?

**Answer:**

An incremental model processes new or changed records without rebuilding the entire target table.

It reduces processing cost for large datasets.

### Q17. What is a unique key?

**Answer:**

The unique key identifies the logical record being updated or inserted.

It enables dbt's supported incremental strategies to identify existing records.

A declared unique key does not automatically guarantee uniqueness. Source deduplication is still necessary.

### Q18. How do you handle late-arriving records in dbt?

**Answer:**

Use:

- A configurable lookback window.
- Source event timestamps.
- Ingestion watermarks.
- Reprocessing of affected keys.
- Historical replay.
- Idempotent MERGE logic.

A fixed lookback window alone cannot guarantee completeness for arbitrarily late events.

### Q19. What is the difference between a snapshot and an incremental model?

**Answer:**

An incremental model maintains a transformed target dataset.

A snapshot records historical versions of mutable source records according to configured change-detection logic.

### Q20. How do you handle hard deletes?

**Answer:**

Options include:

- dbt snapshot hard-delete handling.
- Explicit CDC delete events.
- Soft-delete flags.
- Explicit deletion processing in incremental models.
- Rebuilding affected keys or partitions.

The choice depends on whether deletion history must be retained.

### Q21. What are ephemeral models?

**Answer:**

Ephemeral models do not create standalone warehouse objects. Their SQL is injected into dependent models as CTEs.

They are useful for small reusable transformations but can create very large compiled queries if overused.

### Q22. What is the difference between `dbt run` and `dbt build`?

**Answer:**

`dbt run` executes selected models.

`dbt build` executes applicable selected resources, including models, tests, snapshots and seeds according to the selected graph and execution configuration.

### Q23. How do you manage schema changes?

**Answer:**

Use:

- Explicit source contracts.
- Model contracts where appropriate.
- `on_schema_change`.
- Version-controlled model changes.
- Compatibility testing.
- Controlled backfills.

Never assume automatic schema synchronization resolves semantic changes.

### Q24. How do you optimize dbt projects?

**Answer:**

- Use incremental models for large tables.
- Avoid unnecessary full refreshes.
- Minimize expensive joins.
- Push selective filters early.
- Use appropriate materializations.
- Avoid excessive ephemeral nesting.
- Selectively run modified models.
- Inspect compiled SQL and Snowflake Query Profile.

---

## C. CDC AND SCD

### Q25. What is the difference between event time and ingestion time?

**Answer:**

Event time is when the source change occurred.

Ingestion time is when the data platform received it.

Event time determines historical business ordering, while ingestion time supports pipeline operations and latency measurement.

### Q26. How do you handle out-of-order events?

**Answer:**

Use source transaction ordering metadata or a deterministic event version.

For current-state processing, reject stale updates.

For historical processing, reconstruct the affected entity's timeline and recalculate validity intervals.

### Q27. What is the difference between SCD Type 1 and Type 2?

**Answer:**

Type 1 overwrites old attributes.

Type 2 preserves historical versions with effective start and end dates and a current indicator.

### Q28. What happens if two CDC events have identical timestamps?

**Answer:**

Timestamp alone is insufficient for deterministic ordering.

Use a source sequence, transaction log position, version number or another source-defined ordering field.

### Q29. How do you handle CDC deletes?

**Answer:**

Represent deletes explicitly using tombstones or soft-delete events. Maintain the source sequence so an older event cannot resurrect a deleted record.

### Q30. How do you ensure SCD Type 2 does not have overlapping records?

**Answer:**

Use:

- Effective interval validation.
- One-current-record checks.
- Ordered event reconstruction.
- Transaction-controlled replacement.
- Deterministic ordering.

---

## D. PYSPARK AND SPARK

### Q31. Explain Spark architecture.

**Answer:**

The driver creates and coordinates the application. Executors perform distributed processing on data partitions.

The DAG scheduler divides work into stages, while tasks execute on individual partitions.

### Q32. What is Catalyst Optimizer?

**Answer:**

Catalyst is Spark SQL's query optimization framework.

It performs logical analysis and optimization and helps select physical execution plans.

### Q33. What is Tungsten?

**Answer:**

Tungsten refers to Spark's execution-engine optimizations, including more efficient memory and CPU utilization and code-generation capabilities.

### Q34. What is AQE?

**Answer:**

Adaptive Query Execution adjusts execution plans using runtime statistics.

It can coalesce shuffle partitions, handle certain skewed joins and change join strategies.

### Q35. What is a shuffle?

**Answer:**

A shuffle redistributes data between partitions, usually across executors.

It commonly occurs in joins, aggregations, distinct operations and repartitioning.

### Q36. What is the difference between repartition and coalesce?

**Answer:**

Repartition redistributes records and generally triggers a shuffle.

Coalesce reduces partitions and may avoid a full shuffle.

### Q37. How do you handle data skew?

**Answer:**

- Identify skewed keys.
- Enable AQE skew handling.
- Salt hot keys.
- Pre-aggregate.
- Broadcast small dimensions.
- Repartition appropriately.

### Q38. What is checkpointing?

**Answer:**

Checkpointing persists execution or streaming state to durable storage to support recovery and manage long lineage.

Streaming checkpoints also track progress and state for supported streaming queries.

### Q39. What is the difference between cache and persist?

**Answer:**

`cache()` uses the default persistence level.

`persist()` allows the developer to select a storage level.

Use them when the same expensive dataset is reused.

### Q40. Why should `collect()` be avoided?

**Answer:**

It transfers distributed data to the driver and can exhaust driver memory.

Use distributed transformations and aggregations instead.

---

## E. KAFKA AND STREAMING

### Q41. What is a Kafka partition?

**Answer:**

A partition is an ordered sequence of messages and a unit of parallelism.

### Q42. What is a Kafka offset?

**Answer:**

An offset identifies a message's position within a partition.

### Q43. What is a consumer group?

**Answer:**

A consumer group distributes topic partitions across its members for parallel consumption.

### Q44. What is a watermark?

**Answer:**

A watermark is a streaming engine's mechanism for tracking event-time progress and managing state for late-arriving data.

### Q45. What is a dead-letter queue?

**Answer:**

A dead-letter queue stores messages that cannot be successfully processed after the defined recovery policy.

It enables investigation and replay without blocking all valid traffic.

### Q46. Can Kafka guarantee exactly-once processing across Snowflake?

**Answer:**

Not automatically.

Kafka transactional semantics operate within defined Kafka boundaries. End-to-end delivery across Kafka, object storage and Snowflake needs coordinated offsets, durable checkpoints, event identity, idempotent writes and recovery logic.

### Q47. How do you handle duplicate streaming events?

**Answer:**

Use stable event IDs, deduplication state, source sequence numbers, idempotent writes and a replay-safe sink.

---

## F. AIRFLOW

### Q48. What is a DAG?

**Answer:**

A DAG is a directed acyclic graph representing task dependencies and execution order.

### Q49. What is catchup?

**Answer:**

Catchup controls whether eligible historical scheduling intervals are created from the DAG's start date.

### Q50. What is XCom?

**Answer:**

XCom is Airflow's mechanism for passing small metadata values between tasks.

Do not use it to transport large datasets.

### Q51. What is idempotency?

**Answer:**

A task is idempotent when repeating it produces the same intended final state without introducing duplicate or inconsistent data.

### Q52. How do you handle task failures?

**Answer:**

- Retries.
- Failure callbacks.
- Timeouts.
- Alerts.
- Idempotent processing.
- Controlled backfills.
- Execution audit logs.

### Q53. What is the difference between a sensor and an operator?

**Answer:**

An operator performs work.

A sensor waits for a condition, such as a file arriving or an upstream job completing.

---

## G. AWS

### Q54. What is the difference between S3 and Snowflake?

**Answer:**

S3 is object storage for files and data lake workloads.

Snowflake is a managed analytical database and compute platform.

### Q55. What is AWS Glue?

**Answer:**

Glue is a managed data integration service that includes ETL execution, cataloging and related data integration capabilities.

### Q56. What is Glue Data Catalog?

**Answer:**

It stores metadata describing datasets, schemas, partitions and storage locations.

### Q57. What is AWS DMS?

**Answer:**

DMS supports database migration and ongoing change replication for supported database engines.

### Q58. What is IAM least privilege?

**Answer:**

Grant only the permissions necessary for a role or service to perform its defined responsibility.

### Q59. How do you optimize S3 data layout?

**Answer:**

- Use Parquet for analytical workloads.
- Partition on common filters.
- Avoid tiny files.
- Use compression.
- Apply lifecycle policies.
- Use manifests and ingestion metadata.
- Avoid excessive partition cardinality.

---

# PART 15 – REAL-WORLD SENIOR ENGINEER SCENARIOS

## Scenario 1: Daily Pipeline Suddenly Takes Four Hours Instead of Thirty Minutes

**Investigation:**

1. Compare query execution history.
2. Inspect warehouse queueing.
3. Compare bytes scanned.
4. Inspect micro-partition pruning.
5. Review recent schema changes.
6. Inspect joins and aggregations.
7. Check upstream data volume.

**Solution:**

- Optimize filters.
- Improve join logic.
- Remove unnecessary columns.
- Scale warehouse if compute-bound.
- Isolate competing workloads.
- Add clustering only if measurements justify it.

## Scenario 2: Duplicate Records After Pipeline Retry

**Root cause:**

The extraction completed but the pipeline failed before recording success. Retry reprocessed the same source interval.

**Solution:**

- Use immutable batch IDs.
- Deduplicate using event IDs.
- Use idempotent MERGE.
- Commit watermarks after successful writes.
- Maintain batch audit metadata.

## Scenario 3: Customer City Changes Three Times in One Day

**Requirement:**

Preserve all three changes.

**Solution:**

Use SCD Type 2 with source sequence ordering.

Do not reduce the entire batch to the latest record if the intermediate changes are required for history.

## Scenario 4: CDC Delete Arrives Before an Older Update

**Root cause:**

Out-of-order source delivery.

**Solution:**

- Preserve tombstone.
- Track source sequence.
- Reject older updates.
- Maintain event ledger.
- Prevent deleted records from being resurrected.

## Scenario 5: Snowflake Queries Are Waiting in Queue

**Root cause:**

Concurrent workload saturation.

**Solution:**

- Separate workloads.
- Multi-cluster warehouse.
- Query scheduling.
- Review warehouse sizing.
- Monitor queue metrics.

## Scenario 6: PySpark Job Has One Extremely Slow Task

**Root cause:**

Potential skew, oversized partition or uneven input distribution.

**Solution:**

- Inspect Spark UI.
- Identify skewed keys.
- Use AQE.
- Apply salting selectively.
- Tune partitions.
- Pre-aggregate if possible.

## Scenario 7: API Is Returning HTTP 429

**Root cause:**

API rate limit exceeded.

**Solution:**

- Respect Retry-After.
- Implement exponential backoff with jitter.
- Limit concurrency.
- Track request budgets.
- Persist extraction checkpoints.
- Resume from the last successfully processed page.

## Scenario 8: dbt Incremental Model Misses a Late Record

**Root cause:**

The event arrived outside the configured lookback window.

**Solution:**

- Increase the lookback if appropriate.
- Track ingestion watermarks.
- Reprocess affected keys.
- Add periodic reconciliation.
- Provide source-history replay.

## Scenario 9: SCD2 Fact Joins Return Multiple Customers

**Root cause:**

Overlapping effective date intervals.

**Solution:**

- Validate interval boundaries.
- Reconstruct affected histories.
- Enforce one valid dimension version at fact time.
- Add tests for overlapping records.

## Scenario 10: Production Schema Changes Unexpectedly

**Root cause:**

An upstream application introduced or modified a field.

**Solution:**

- Detect schema drift.
- Capture raw records.
- Validate schema compatibility.
- Alert data owners.
- Update staging models.
- Test downstream dependencies.
- Deploy using a controlled release.

---

# PART 16 – ADDITIONAL ADVANCED TOPICS TO MASTER

A senior engineer should also understand the following areas.

## 16.1 Data Lakehouse and Open Table Formats

Learn:

- Apache Iceberg.
- Apache Hudi.
- Delta Lake.
- Parquet.
- ORC.
- Schema evolution.
- Snapshot isolation.
- ACID transactions.
- Compaction.
- Time travel.

Understand when to use Snowflake-managed tables versus open-format lakehouse storage.

## 16.2 Snowflake Advanced Features

Learn:

- Snowpark Python.
- Snowpark stored procedures.
- Snowflake Cortex.
- External tables.
- Iceberg tables.
- Hybrid tables.
- Secure data sharing.
- Replication and failover.
- Resource monitors.
- Query acceleration.
- Data masking.
- Row access policies.
- Account usage and organization usage views.

## 16.3 Advanced dbt

Learn:

- Model contracts.
- Unit tests.
- Exposures.
- Metrics and semantic modeling capabilities.
- State-based selection.
- Slim CI.
- Source freshness.
- Snapshot strategies.
- Custom materializations.
- Packages.
- dbt artifacts.
- Documentation generation.
- Environment promotion.

## 16.4 Advanced Streaming

Learn:

- Kafka partition strategy.
- Schema Registry.
- Avro and Protobuf.
- Event sourcing.
- Event-time windows.
- Session windows.
- Watermarks.
- Stateful processing.
- Backpressure.
- Exactly-once boundaries.
- Replay and recovery.
- Dead-letter topics.
- Stream-table joins.

## 16.5 Enterprise Data Governance

Learn:

- Data lineage.
- Data ownership.
- Data contracts.
- PII classification.
- Retention policies.
- Data masking.
- Access auditing.
- Metadata management.
- Business glossary.
- Data observability.
- Regulatory compliance.

## 16.6 Production Observability

Every important pipeline should expose:

| Metric | Meaning |
| --- | --- |
| Pipeline duration | Total processing time |
| Source record count | Extracted records |
| Target record count | Loaded records |
| CDC lag | Source-to-target delay |
| Event lateness | Difference between arrival and event time |
| Duplicate rate | Duplicate event percentage |
| Rejected records | Failed validation count |
| Query duration | SQL execution time |
| Shuffle spill | Spark resource pressure |
| Retry count | Reliability indicator |
| SLA status | Business delivery health |

---

# PART 17 – SENIOR DATA ENGINEER PRODUCTION CHECKLIST

## Architecture

- [ ]Batch and streaming requirements documented.
- [ ]Source and target ownership identified.
- [ ]Latency SLA defined.
- [ ]Recovery and replay design documented.
- [ ]Security boundaries defined.

## CDC

- [ ]Event identity available.
- [ ]Source sequence captured.
- [ ]Source commit timestamp captured.
- [ ]Ingestion timestamp captured.
- [ ]Duplicate events handled.
- [ ]Hard deletes handled.
- [ ]Late events handled.
- [ ]Watermarks persisted.

## Snowflake

- [ ]Warehouse workloads separated.
- [ ]Query Profile reviewed.
- [ ]Pruning validated.
- [ ]Clustering justified.
- [ ]RBAC configured.
- [ ]Resource monitoring enabled.
- [ ]Time Travel retention understood.

## dbt

- [ ]Source definitions maintained.
- [ ]Model dependencies documented.
- [ ]Incremental logic idempotent.
- [ ]Unique keys validated.
- [ ]Tests implemented.
- [ ]Snapshots configured where required.
- [ ]CI validation enabled.

## PySpark

- [ ]Explicit schemas.
- [ ]AQE configured where appropriate.
- [ ]Skew monitored.
- [ ]Shuffle operations optimized.
- [ ]Small files controlled.
- [ ]Checkpoints configured for streaming.
- [ ]Dead-letter processing implemented.

## Airflow

- [ ]DAG parsing lightweight.
- [ ]Retries configured.
- [ ]Execution timeouts configured.
- [ ]Backfills tested.
- [ ]Alerts enabled.
- [ ]Tasks idempotent.
- [ ]Secrets managed securely.

## Production Operations

- [ ]Audit logging.
- [ ]Reconciliation.
- [ ]Data quality.
- [ ]Failure recovery.
- [ ]SLA monitoring.
- [ ]Documentation.
- [ ]Rollback strategy.

---

# FINAL ENGINEERING PRINCIPLE

A senior data engineer is not simply someone who writes complex SQL or PySpark transformations.

A senior data engineer must be able to design and operate a complete, reliable data platform.

The essential capabilities are:

1. Build reliable batch and streaming ingestion.
2. Understand CDC ordering, duplicates, deletes and replay.
3. Implement correct SCD Type 1 and Type 2 history.
4. Optimize Snowflake storage access and compute usage.
5. Design scalable dbt projects and incremental transformations.
6. Build distributed PySpark pipelines.
7. Orchestrate workflows with Airflow.
8. Design secure AWS data lake architectures.
9. Implement reconciliation and observability.
10. Build automated CI/CD and infrastructure deployment.
11. Troubleshoot production failures independently.
12. Explain architectural trade-offs and defend technical decisions.

**The ultimate objective is not just to make a pipeline work. It is to make it correct, scalable, cost-efficient, recoverable, observable, secure and maintainable.**

## Important implementation notes

Before using these examples in production, keep these five distinctions in mind:

1. SCD Type 1 vs SCD Type 2: Current-state deduplication is not the same as historical event reconstruction. Never discard intermediate CDC events when complete history is required.
2. dbt incremental lookback: A four- or six-hour lookback is a mitigation, not a guarantee against arbitrarily late data. Maintain replay and reconciliation capabilities.
3. Snowflake performance: Bigger warehouses do not automatically fix inefficient SQL. Check Query Profile before increasing compute.
4. Streaming exactly-once: Design for replay-safe, idempotent processing. End-to-end exactly-once behavior requires coordination across system boundaries.
5. Production SCD2: Account for same-timestamp events, deletes, overlapping batches, concurrent execution and fact-table surrogate-key implications.

## Recommended next learning sequence

| Priority | Subject | What to master |
| --- | --- | --- |
| 1 | Advanced SQL | Window functions, MERGE, CTEs, query optimization |
| 2 | Snowflake | Architecture, performance, Streams, Tasks, Snowpipe |
| 3 | dbt | Incremental, snapshots, macros, testing, CI/CD |
| 4 | CDC | Debezium, Kafka, DMS, event ordering |
| 5 | SCD | Type 1, Type 2, late-arriving history |
| 6 | PySpark | Joins, shuffles, AQE, skew, streaming |
| 7 | Airflow | DAGs, backfills, retries, idempotency |
| 8 | AWS | S3, Glue, IAM, DMS, catalog |
| 9 | Architecture | Lakehouse, batch, streaming, recovery |
| 10 | Production | Monitoring, reconciliation, incident management |

My recommendation: Treat this as your master reference and build a hands-on project around it: Oracle CDC → Kafka → S3 Bronze → PySpark/Glue Silver → Snowflake → dbt SCD2 and incremental facts → Airflow → BI.

That single project will give you practical experience across almost every major responsibility of a Senior Data Engineer.