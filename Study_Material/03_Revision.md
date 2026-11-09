# Standard Operating Procedure (SOP): Senior Data Engineer (Snowflake & dbt)

---

## Document Control & Scope

* **Target Role:** Senior Data Engineer (Snowflake, dbt, Python, PySpark, Airflow, AWS, Enterprise Architecture)
* **Purpose:** Single-source reference and technical revision manual covering production architecture, operational patterns, code implementations, and performance optimizations.

---

## Table of Contents

1. Late-Arriving & Out-of-Order CDC Processing
2. Snowflake Performance Tuning & Optimization
3. dbt Core Engineering & Materialization Strategies
4. Production Python Standards, Exception Handling & APIs
5. PySpark Distributed Processing & Optimization
6. Apache Airflow Orchestration & DAG Design
7. AWS Data Architecture (S3, Glue, IAM & Data Catalog)
8. Data Warehousing & Dimensional Modeling (SCD Type 2)
9. Data Quality Frameworks, Reconciliation & Troubleshooting
10. Git, CI/CD Practices & Infrastructure Deployment
11. End-to-End Enterprise Architecture Blueprint
12. Senior L2 Technical Interview Scenarios & Benchmark Solutions

---

## 1. Late-Arriving & Out-of-Order CDC Processing

When Change Data Capture (CDC) events arrive out of order, pipelines must distinguish between the **Source Commit Timestamp** (when the change happened) and the **Pipeline Arrival Timestamp** (when the record landed in the warehouse).

### 1.1 Current-State Table (SCD Type 1) Implementation

Uses a `MERGE` statement with a strict timestamp guardrail to prevent older out-of-order events from overwriting newer target states.

```sql
MERGE INTO CUSTOMER_CURRENT AS target
USING CUSTOMER_STAGING AS source
    ON target.customer_id = source.customer_id
-- Guardrail: Update only if the incoming change is strictly newer than current target state
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

### 1.2 SCD Type 2 Timeline Re-stitching

Out-of-order records cannot be discarded in SCD Type 2; they must be inserted into their correct historical sequence. The re-stitching pattern combines active history with incoming staging data, re-orders chronologically by source timestamp, and uses `LEAD()` to recalculate validity windows.

```sql
WITH all_timeline_events AS (
    -- Pull existing history for the affected entity
    SELECT 
        customer_id,
        city,
        start_time AS event_timestamp
    FROM DIM_CUSTOMER_SCD2
    WHERE customer_id = 101

    UNION ALL

    -- Pull late-arriving staging records
    SELECT 
        customer_id,
        city,
        commit_timestamp AS event_timestamp
    FROM CUSTOMER_STAGING
    WHERE customer_id = 101
),

restitched_timeline AS (
    SELECT 
        customer_id,
        city,
        event_timestamp AS start_time,
        COALESCE(
            LEAD(event_timestamp, 1) OVER (PARTITION BY customer_id ORDER BY event_timestamp ASC),
            '9999-12-31 23:59:59'::TIMESTAMP_NTZ
        ) AS end_time,
        CASE 
            WHEN LEAD(event_timestamp, 1) OVER (PARTITION BY customer_id ORDER BY event_timestamp ASC) IS NULL 
            THEN TRUE 
            ELSE FALSE 
        END AS is_current
    FROM all_timeline_events
)
SELECT * FROM restitched_timeline;

```

---

## 2. Snowflake Performance Tuning & Optimization

### 2.1 Optimization Matrix

| Mechanism | Target Query Pattern | Operating Mechanism | Trade-Offs / Notes |
| --- | --- | --- | --- |
| **Micro-Partition Pruning** | Range scans, dates | Metadata range checks (`min`/`max` per 50–500 MB partition). | Automatic; requires sargable predicates. |
| **Automatic Clustering** | Large range queries (>100 GB) | Background re-sorting of physical micro-partitions by cluster key. | Consumes background maintenance credits. |
| **Search Optimization Service (SOS)** | Point lookups, exact string matches | Builds persistent secondary inverted search indexes. | Storage & maintenance cost overhead. |
| **Materialized Views** | Heavy aggregation re-querying | Pre-computed query results auto-maintained by Snowflake. | Maintenance cost; limited SQL syntax support. |

### 2.2 Sargable SQL Optimization

#### Non-Sargable Query (Fails Pruning)

```sql
-- YEAR() function breaks partition pruning by masking min/max metadata bounds
SELECT customer_id, SUM(order_amount) AS total_spent
FROM sales_fact
WHERE YEAR(order_date) = 2025
GROUP BY customer_id;

```

#### Optimized Sargable Query

```sql
-- Range predicate exposes explicit values for micro-partition pruning
SELECT customer_id, SUM(order_amount) AS total_spent
FROM sales_fact
WHERE order_date >= '2025-01-01' 
  AND order_date < '2026-01-01'
GROUP BY customer_id;

```

### 2.3 Automatic Clustering Implementation

```sql
-- 1. Table creation with clustering key
CREATE OR REPLACE TABLE TRANSACTION_FACT (
    transaction_id VARCHAR(36),
    transaction_date DATE,
    region_id INT,
    amount DECIMAL(18,2)
)
CLUSTER BY (transaction_date, region_id);

-- 2. Alter existing table
ALTER TABLE TRANSACTION_FACT CLUSTER BY (transaction_date);

-- 3. Check clustering health
SELECT SYSTEM$CLUSTERING_INFORMATION('TRANSACTION_FACT', '(transaction_date)');

```

### 2.4 Search Optimization Service (SOS) Implementation

```sql
-- Target specific high-value point-lookup and substring columns
ALTER TABLE CUSTOMER_SUPPORT_TICKETS ADD SEARCH OPTIMIZATION ON 
    EQUALITY(customer_email),
    SUBSTRING(ticket_notes);

-- Verify progress
SHOW TABLES LIKE 'CUSTOMER_SUPPORT_TICKETS';

```

---

## 3. dbt Core Engineering & Materialization Strategies

### 3.1 Layered Architecture Overview

1. **Staging (`view`)**: Source abstraction, renaming, data type casting. Zero storage cost.
2. **Intermediate (`ephemeral`)**: Reusable transformation logic injected as CTEs. No physical warehouse object.
3. **Core Fact (`incremental`)**: Large tables updated via `MERGE` and lookback windows.
4. **Mart Summary (`table`)**: Aggregated BI tables optimized for dashboard response times.

### 3.2 Pipeline Code Implementation

#### Step 1: Staging Layer (`models/staging/stg_orders.sql`)

```sql
{{ config(materialized='view') }}

SELECT
    TRIM(ORDER_ID) AS order_id,
    CAST(CUSTOMER_ID AS INT) AS customer_id,
    CAST(ORDER_DATE AS TIMESTAMP_NTZ) AS order_date,
    UPPER(TRIM(STATUS)) AS order_status,
    CAST(TOTAL_AMOUNT AS DECIMAL(18,2)) AS order_amount,
    CAST(_ETL_LOADED_AT AS TIMESTAMP_NTZ) AS updated_at
FROM {{ source('raw_erp', 'orders_raw') }}
WHERE ORDER_ID IS NOT NULL

```

#### Step 2: Intermediate Layer (`models/intermediate/int_order_adjustments.sql`)

```sql
{{ config(materialized='ephemeral') }}

SELECT
    order_id,
    customer_id,
    order_date,
    order_status,
    CASE 
        WHEN order_status = 'REFUNDED' THEN 0.00
        ELSE order_amount 
    END AS adjusted_order_amount,
    updated_at
FROM {{ ref('stg_orders') }}

```

#### Step 3: Incremental Model with Lookback Window (`models/marts/fct_orders_incremental.sql`)

```sql
{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge',
        cluster_by=['order_date'],
        on_schema_change='sync_all_columns'
    )
}}

SELECT
    order_id,
    customer_id,
    order_date,
    order_status,
    adjusted_order_amount AS order_amount,
    updated_at
FROM {{ ref('int_order_adjustments') }}

{% if is_incremental() %}
  -- LOOKBACK WINDOW: Process last 4 hours to capture late-arriving CDC records safely
  WHERE updated_at >= (
      SELECT DATEADD('hour', -4, MAX(updated_at)) 
      FROM {{ this }}
  )
{% endif %}

```

#### Step 4: Aggregate Mart (`models/marts/fart_monthly_revenue_summary.sql`)

```sql
{{
    config(
        materialized='table',
        cluster_by=['order_month']
    )
}}

SELECT
    DATE_TRUNC('month', order_date) AS order_month,
    customer_id,
    COUNT(order_id) AS total_orders,
    SUM(order_amount) AS total_revenue,
    AVG(order_amount) AS average_order_value,
    CURRENT_TIMESTAMP() AS last_refreshed_at
FROM {{ ref('fct_orders_incremental') }}
GROUP BY 1, 2

```

#### Step 5: Data Quality Configuration (`models/marts/schema.yml`)

```yaml
version: 2

models:
  - name: fct_orders_incremental
    description: "Core orders incremental fact table with lookback window"
    columns:
      - name: order_id
        tests:
          - unique
          - not_null
      - name: order_status
        tests:
          - accepted_values:
              values: ['PENDING', 'COMPLETED', 'SHIPPED', 'REFUNDED', 'CANCELLED']

```

---

## 4. Production Python Standards, Exception Handling & APIs

Production Python ingestion requires exponential backoff, structured log contexts, typed exceptions, and safe file writing via `pathlib`.

```python
import logging
import sys
import time
import json
from pathlib import Path
from typing import Dict, Any, List, Optional
import requests
from requests.exceptions import RequestException, Timeout, HTTPError

# 1. Structured Logging Setup
logger = logging.getLogger("DataPipelineLogger")
logger.setLevel(logging.INFO)
if not logger.handlers:
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] [%(filename)s:%(lineno)d] - %(message)s"
    ))
    logger.addHandler(stream_handler)

class APIExtractionError(Exception):
    """Raised when API extraction fails permanently."""
    pass

class VendorAPIClient:
    def __init__(self, base_url: str, api_key: str, timeout: int = 10):
        self.base_url = base_url
        self.headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        self.timeout = timeout

    def fetch_with_retry(self, endpoint: str, params: Optional[Dict[str, Any]] = None, max_retries: int = 3) -> Dict[str, Any]:
        url = f"{self.base_url}/{endpoint}"
        backoff_factor = 2

        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"API Request: {url} (Attempt {attempt}/{max_retries})")
                response = requests.get(url, headers=self.headers, params=params, timeout=self.timeout)
                response.raise_for_status()
                return response.json()
            except Timeout as te:
                logger.warning(f"Timeout at {url}: {te}")
            except HTTPError as he:
                status_code = he.response.status_code
                logger.error(f"HTTP Error {status_code}: {he}")
                if 400 <= status_code < 500:
                    raise APIExtractionError(f"Client error {status_code}. Aborting retries.")
            except RequestException as re:
                logger.warning(f"Network error: {re}")

            if attempt == max_retries:
                raise APIExtractionError(f"Failed to fetch {url} after {max_retries} attempts.")

            sleep_time = backoff_factor ** attempt
            logger.info(f"Retrying in {sleep_time} seconds...")
            time.sleep(sleep_time)

def save_payload_to_staging(data: List[Dict[str, Any]], output_dir: str, filename: str) -> Path:
    try:
        target_dir = Path(output_dir)
        target_dir.mkdir(parents=True, exist_ok=True)
        file_path = target_dir / filename
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
            
        logger.info(f"Staged {len(data)} records to {file_path}")
        return file_path
    except Exception as e:
        logger.error(f"Failed staging file: {e}")
        raise

```

---

## 5. PySpark Distributed Processing & Optimization

### 5.1 Optimization Principles

* **Narrow vs. Wide Transformations:** Narrow (`filter`, `select`) executes in-memory per node; Wide (`groupBy`, `join`) forces an expensive network shuffle.
* **Broadcast Join (`broadcast()`):** Avoids shuffles by sending small lookup tables (<100 MB) to all worker nodes.
* **Adaptive Query Execution (AQE):** Set `spark.sql.adaptive.enabled = true` to coalesce shuffle partitions and mitigate runtime data skew dynamically.

### 5.2 Production PySpark Pipeline Implementation

```python
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_date, broadcast
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

def process_clickstream():
    spark = SparkSession.builder \
        .appName("ClickstreamEnrichment") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

    # Explicit schema definition avoids two-pass schema inference overhead
    clickstream_schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("user_id", StringType(), True),
        StructField("campaign_id", StringType(), True),
        StructField("event_timestamp", TimestampType(), True)
    ])

    campaign_schema = StructType([
        StructField("campaign_id", StringType(), False),
        StructField("campaign_name", StringType(), True)
    ])

    df_clicks = spark.read.schema(clickstream_schema).parquet("./data/raw/clicks")
    df_campaigns = spark.read.schema(campaign_schema).parquet("./data/raw/campaigns")

    # Early filter reduces shuffle data volume
    df_filtered = df_clicks \
        .filter(col("user_id").isNotNull()) \
        .withColumn("event_date", to_date(col("event_timestamp")))

    # Broadcast Join eliminates network shuffle for df_campaigns
    df_enriched = df_filtered.join(
        broadcast(df_campaigns),
        on="campaign_id",
        how="left"
    ).withColumn("processed_at", current_timestamp())

    df_enriched.write \
        .mode("overwrite") \
        .partitionBy("event_date") \
        .parquet("./data/curated/clicks")

    spark.stop()

if __name__ == "__main__":
    process_clickstream()

```

---

## 6. Apache Airflow Orchestration & DAG Design

### 6.1 Architectural Rules

* **No Top-Level Execution:** Never place DB connections or API calls at the root level of a DAG file; Airflow parses DAG files every 30 seconds, causing scheduler locking.
* **Idempotency:** Parameterize date windows using Airflow macros (`{{ ds }}`) instead of hardcoding `current_date()`.

### 6.2 Production DAG Implementation (`dags/daily_pipeline.py`)

```python
from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

def task_failure_alert(context):
    logging.error(
        f"CRITICAL: Task Failed! DAG: {context.get('dag').dag_id}, "
        f"Task: {context.get('task_instance').task_id}, Date: {context.get('execution_date')}"
    )

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_failure_alert,
}

with DAG(
    dag_id='daily_enterprise_data_pipeline',
    default_args=default_args,
    schedule_interval='0 2 * * *', # Daily at 02:00 AM UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    t1_extract = PythonOperator(
        task_id='extract_vendor_data',
        python_callable=lambda **c: logging.info(f"Extracting for date: {c['ds']}"),
    )

    t2_dbt_run = BashOperator(
        task_id='run_dbt_transformations',
        bash_command='cd /opt/airflow/dbt_project && dbt run --target prod',
    )

    t3_dbt_test = BashOperator(
        task_id='run_dbt_data_quality_tests',
        bash_command='cd /opt/airflow/dbt_project && dbt test --target prod',
    )

    t1_extract >> t2_dbt_run >> t3_dbt_test

```

---

## 7. AWS Data Architecture (S3, Glue, IAM & Data Catalog)

### 7.1 Key Infrastructure Rules

* **S3 Prefixes:** Organize data by layer and partition key (`s3://bucket/curated/sales/year=2026/month=11/`). Set lifecycle rules to transition old raw files to `S3 Glacier`.
* **AWS Glue ETL:** Serverless PySpark runtime. Uses DynamicFrames to convert to DataFrames seamlessly and update Glue Data Catalog schemas automatically.

### 7.2 Production AWS Glue Script (`aws_glue_etl_job.py`)

```python
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, current_timestamp, to_date

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH', 'S3_OUTPUT_PATH', 'GLUE_DATABASE', 'GLUE_TABLE'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 1. Read Raw JSON via DynamicFrame
datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [args['S3_INPUT_PATH']], "recurse": True},
    format="json"
)

# 2. Convert to PySpark DataFrame for transformations
df = datasource.toDF() \
    .filter(col("transaction_id").isNotNull()) \
    .withColumn("transaction_date", to_date(col("timestamp"))) \
    .withColumn("processed_at", current_timestamp())

# 3. Write back to S3 Parquet and update Catalog Metadata
output_dynamic_frame = DynamicFrame.fromDF(df, glueContext, "output_df")

glueContext.write_dynamic_frame.from_options(
    frame=output_dynamic_frame,
    connection_type="s3",
    connection_options={
        "path": args['S3_OUTPUT_PATH'],
        "partitionKeys": ["transaction_date"]
    },
    format="parquet"
)

job.commit()

```

---

## 8. Data Warehousing & Dimensional Modeling (SCD Type 2)

### 8.1 Schema DDL Definitions

```sql
-- SCD Type 2 Dimension Table
CREATE OR REPLACE TABLE DIM_CUSTOMER (
    customer_sk INT IDENTITY(1,1),
    customer_id INT NOT NULL,
    email VARCHAR(150),
    city VARCHAR(100),
    tier VARCHAR(50),
    effective_start_date TIMESTAMP_NTZ NOT NULL,
    effective_end_date TIMESTAMP_NTZ,
    is_current BOOLEAN NOT NULL
);

-- Fact Table with Foreign Key to Surrogate Key
CREATE OR REPLACE TABLE FACT_SALES (
    sale_id VARCHAR(36) NOT NULL,
    customer_sk INT NOT NULL,
    order_date TIMESTAMP_NTZ NOT NULL,
    total_amount DECIMAL(18,2) NOT NULL,
    CONSTRAINT fk_customer FOREIGN KEY (customer_sk) REFERENCES DIM_CUSTOMER(customer_sk)
)
CLUSTER BY (order_date);

```

### 8.2 Production SCD Type 2 Load SQL Pattern

```sql
-- Step 1: Expire existing active records where attributes changed
MERGE INTO DIM_CUSTOMER AS target
USING STG_CUSTOMERS AS source
    ON target.customer_id = source.customer_id
    AND target.is_current = TRUE
WHEN MATCHED AND (
    target.city <> source.city OR 
    target.tier <> source.tier OR
    target.email <> source.email
) 
THEN UPDATE SET
    target.effective_end_date = source.updated_at,
    target.is_current = FALSE;

-- Step 2: Insert new customers AND new active versions of expired records
INSERT INTO DIM_CUSTOMER (
    customer_id, email, city, tier, effective_start_date, effective_end_date, is_current
)
SELECT 
    s.customer_id, s.email, s.city, s.tier, s.updated_at, NULL, TRUE
FROM STG_CUSTOMERS s
LEFT JOIN DIM_CUSTOMER target
    ON s.customer_id = target.customer_id AND target.is_current = TRUE
WHERE target.customer_id IS NULL
   OR (target.city <> s.city OR target.tier <> s.tier OR target.email <> s.email);

-- Step 3: Load Fact Table with Point-in-Time Surrogate Key Matching
INSERT INTO FACT_SALES (sale_id, customer_sk, order_date, total_amount)
SELECT 
    stg.sale_id,
    dim.customer_sk,
    stg.order_date,
    stg.total_amount
FROM STG_ORDERS stg
JOIN DIM_CUSTOMER dim
    ON stg.customer_id = dim.customer_id
    AND stg.order_date >= dim.effective_start_date 
    AND stg.order_date < COALESCE(dim.effective_end_date, '9999-12-31 23:59:59'::TIMESTAMP_NTZ);

```

---

## 9. Data Quality Frameworks, Reconciliation & Troubleshooting

### 9.1 Data Reconciliation Script (`reconciliation_engine.py`)

```python
import logging
import sys
import snowflake.connector

def reconcile_batch(sf_config: dict, execution_date: str) -> bool:
    conn = snowflake.connector.connect(**sf_config)
    cursor = conn.cursor()
    try:
        # Check source count & financial checksum
        cursor.execute(f"SELECT COUNT(*), COALESCE(SUM(amount), 0) FROM STG_ORDERS WHERE CAST(created_at AS DATE) = '{execution_date}'")
        src_count, src_sum = cursor.fetchone()

        # Check target count & financial checksum
        cursor.execute(f"SELECT COUNT(*), COALESCE(SUM(total_amount), 0) FROM FACT_SALES WHERE CAST(order_date AS DATE) = '{execution_date}'")
        tgt_count, tgt_sum = cursor.fetchone()

        if src_count != tgt_count or abs(float(src_sum) - float(tgt_sum)) > 1.00:
            logging.error(f"❌ RECONCILIATION FAILED! Src: ({src_count}, ${src_sum}), Tgt: ({tgt_count}, ${tgt_sum})")
            return False

        logging.info("✅ Reconciliation Passed.")
        return True
    finally:
        cursor.close()
        conn.close()

```

---

## 10. Git, CI/CD Practices & Infrastructure Deployment

### 10.1 GitHub Actions Workflow (`.github/workflows/pipeline_ci_cd.yml`)

```yaml
name: Data Platform CI/CD

on:
  push:
    branches: [ "main" ]
  pull_request:
    branches: [ "main" ]

jobs:
  validate_and_test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.10"
      - run: pip install dbt-snowflake pytest sqlfluff
      - run: sqlfluff lint models/ --dialect snowflake
      - run: pytest tests/
      - run: cd dbt_project && dbt parse --profiles-dir .

  deploy_production:
    needs: validate_and_test
    if: github.ref == 'refs/heads/main' && github.event_name == 'push'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.10"
      - run: pip install dbt-snowflake
      - run: cd dbt_project && dbt run --profiles-dir . --target prod
        env:
          SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
          SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}

```

---

## 11. End-to-End Enterprise Architecture Blueprint

```text
[Operational Systems (Oracle / APIs)]
       │
       ├── (CDC / Debezium & Kafka) ──► [AWS S3 Landing / Bronze (Raw JSON)]
       │                                         │
       │ (Batch Fallback)                        ▼
       │                                [AWS Glue / PySpark (Cleaning & Partitioning)]
       │                                         │
       ▼                                         ▼
[Apache Airflow] ──────────────────────► [AWS S3 Curated / Silver (Parquet)]
       │                                         │
       │ (Triggers dbt Execution)                ▼
       └──────────────────────────────► [Snowflake Data Warehouse]
                                                 ├── Staging Views (Raw clean)
                                                 ├── Incremental Fact Models (dbt)
                                                 └── Executive Dimensional Marts
                                                 │
                                                 ▼
                                        [Automated Reconciliation & Tests]
                                                 │
                                                 ▼
                                        [Tableau / BI Analytics]

```

---

## 12. Senior L2 Technical Interview Scenarios & Benchmark Solutions

### Question 1: Snowflake Warehouse Queueing Under High Concurrency

* **Diagnosis:** Distinguish between **Single Query Resource Constraints** (which cause disk spilling and require scaling *Up* to a larger warehouse size) and **Concurrency Queuing** (multiple queries waiting for execution threads).
* **Fix:** Configure an **Auto-Scaling Multi-Cluster Warehouse** (`MIN_CLUSTERS=1`, `MAX_CLUSTERS=4`, `STANDARD` or `ECONOMY` policy) to provision additional compute nodes horizontally on demand.

### Question 2: Handling Hard Deletes in dbt Incremental Models

* **Problem:** Standard dbt `merge` strategies do not remove target rows deleted in the source table.
* **Fix:** Implement **Soft Deletes** by updating an `is_deleted` flag via snapshot invalidation (`invalidate_hard_deletes=true`), or use an incremental `delete+insert` strategy across affected partitions.

### Question 3: PySpark Catalyst Optimizer & Logical Plans

* **Mechanism:** Catalyst transforms unresolved logical plans through four stages: Analysis (catalog resolution), Logical Optimization (**Predicate Pushdown**, Projection Pruning), Physical Planning (selecting join strategies like Broadcast vs. Sort-Merge), and Code Generation (Project Tungsten). Inspect via `df.explain(True)`.

### Question 4: Handling Overlapping Batches & Duplicate Event Arrivals

* **Problem:** Flaky upstream APIs recovery flushes duplicate events across batch files, causing unique key constraint violations in warehouse loads.
* **Fix:**
1. **Landing/Staging De-duplication:** Use `ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY source_timestamp DESC) = 1` inside staging views or PySpark ingestion jobs before warehouse ingestion.
2. **Idempotent Merge:** Ensure dbt/Snowflake uses an explicit `MERGE` on `unique_key='event_id'` so re-ingested records overwrite existing target rows cleanly instead of raising duplicate errors.



---