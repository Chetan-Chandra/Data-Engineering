SENIOR DATA ENGINEER
MASTER HANDBOOK & SOP
Snowflake • dbt • GCP / GCS • Python • PySpark • Airflow • Kafka / Pub/Sub
Consolidated source handbook + production completion chapters + advanced interview bank
 
PART A — SUPPLIED HANDBOOK (SOURCE CONTENT)

The following section preserves the uploaded handbook as supplied. It contains overlapping drafts and some repeated sections; consult the completion edition after it for additional operating standards, implementation patterns and interview answers.
finish this SOP, add missing chapters and concepts and codes. also add advance level interview questions and answers be it conceptula or scenario based with codes. 
Senior Data Engineer Master Handbook
Snowflake + dbt + GCP (GCS) + Python + PySpark + Airflow + Real-Time Streaming
Advanced technical handbook Production architecture Interview preparation
This handbook is designed as a practical reference for a Senior Data Engineer working in an enterprise environment, particularly one using Google Cloud Storage instead of Amazon S3.

It covers three dimensions of senior-level engineering:
1.	Conceptual knowledge: Understanding how systems work internally, why particular architectures are selected, and their limitations.
2.	Implementation knowledge: Writing production-oriented SQL, Python, PySpark, dbt, Airflow and GCP code.
3.	Interview knowledge: Explaining architecture decisions, debugging failures, handling difficult scenarios and justifying trade-offs.

The examples use one consistent enterprise scenario: an operational business platform generating customer, order, payment and transaction data.

1. Technology stack and learning roadmap
Enterprise technology ecosystem
Streamlit & Google Cloud Storage: Your Easy File Upload Guide | by Allan Alfonso | Google Cloud - Community | Medium
GCS
Data lake and object storage
AVEVA Announces Collaboration with Snowflake to Accelerate Industrial AI and Unify IT/OT Data Ecosystems
Snowflake
Data warehouse and processing
VCX by Fundrise
dbt Core
Transformation and testing
File:Apache Spark logo.svg - Wikimedia Commons
PySpark
Distributed processing
Apache Airflow 3.0: Key New Features
Airflow
Orchestration
Cloud Schedulerを使用して、定期的にPub/Subトピックにメッセージを送信する仕組みを構築する | DevelopersIO
Pub/Sub and Kafka
Event ingestion
Learn about data integration, migration, replication, and strategic data practices.
Dataproc / Dataflow
Batch and streaming compute
【Terraform】terraform planの結果を各リソース1行でコンパクトに表示する - かわいちのエンジニアぶろぐ！
Terraform and CI/CD
Infrastructure deployment
Handbook structure
Chapter	Technical area	Level
1	Enterprise data architecture	Advanced
2	Batch, streaming and CDC	Advanced
3	GCS data lake architecture	Moderate–Advanced
4	Snowflake architecture and SQL	Advanced
5	Snowflake performance and administration	Advanced
6	Snowflake Streams, Tasks and Snowpipe	Advanced
7	dbt engineering and incremental processing	Advanced
8	SCD1, SCD2 and dimensional modeling	Advanced
9	Python production engineering	Moderate–Advanced
10	PySpark distributed computing	Advanced
11	Airflow and orchestration	Advanced
12	Data quality and reconciliation	Advanced
13	Security, governance and observability	Advanced
14	CI/CD and Terraform	Advanced
15	Production troubleshooting scenarios	Advanced
16	Senior-level interview questions and answers	Moderate–Advanced
Chapter 1. Enterprise Data Engineering Architecture
1.1 Complete architecture
The architecture below supports both scheduled batch pipelines and continuous CDC/event processing.
Data Platforms vs Large Datasets— Dremio & Snowflake match | by Insightify.io | Medium
[medium.com](<https://medium.com/%40tech_8949/data-platforms-vs-large-datasets-dremio-snowflake-match-94a48f7e85e6?utm_source=chatgpt.com>)
text
                  SOURCE SYSTEMS
          Oracle / SQL Server / MySQL / APIs
                         |
             +-----------+-----------+
             |                       |
         BATCH LOAD               CDC LOAD
         Python/API          Debezium/Datastream
             |                       |
             v                       v
        AIRFLOW                 KAFKA/PUBSUB
             |                       |
             |                       v
             |                  DATAFLOW
             |                 / PYSPARK
             |                       |
             +-----------+-----------+
                         |
                         v
               GOOGLE CLOUD STORAGE
                   BRONZE LAYER
                 Raw JSON / Avro
                         |
                         v
               DATAPROC / PYSPARK
                Data Cleansing
                Deduplication
                Validation
                Schema Evolution
                         |
                         v
               GOOGLE CLOUD STORAGE
                   SILVER LAYER
                 Partitioned Parquet
                         |
                         v
                     SNOWFLAKE
                External Stage / COPY
                         |
                         v
                     DBT CORE
               Staging / Intermediate
                Incremental / Snapshots
                         |
                 +-------+-------+
                 |               |
                 v               v
               SCD1             SCD2
                 |               |
                 +-------+-------+
                         |
                         v
                  GOLD DATA MARTS
                         |
                         v
                  TABLEAU / POWER BI
Airflow/Cloud Composer orchestrates scheduled work. Streaming jobs normally run continuously and have their own checkpoint and recovery mechanisms; Airflow can deploy, monitor and coordinate them rather than restarting them for every event.
1.2 Bronze, Silver and Gold architecture
Layer	Responsibility	Example
Bronze	Preserve original source events	Original CDC JSON
Silver	Standardize, validate and deduplicate	Clean Parquet
Gold	Business-level data models	Revenue by customer
Quarantine	Invalid data	Missing customer ID
Archive	Historical raw retention	Older source files
Interview question: Why do you need Bronze if you already have Silver?
Answer:
Bronze preserves the original source data and allows us to replay pipelines.
Suppose a transformation has a bug that incorrectly calculates transaction amounts.
If I retain only the transformed Silver data, I may need to re-extract from the source system.
With Bronze, I can reprocess the original events using corrected business logic.
Bronze also helps with:
•	Auditability.
•	Schema evolution.
•	Historical reconstruction.
•	Data reconciliation.
•	Regulatory investigations.
•	Recovery after downstream failures.
Chapter 2. Batch, Streaming and CDC Processing
2.1 Batch processing
Batch processing collects records over a defined interval and processes them together.
Examples:
•	Daily customer extraction.
•	Monthly financial reporting.
•	Historical data migration.
•	Daily sales aggregation.
Typical architecture:
text
Oracle Database
      |
      v
Python Extraction
      |
      v
GCS Bronze
      |
      v
PySpark / Dataproc
      |
      v
GCS Silver
      |
      v
Snowflake COPY INTO
      |
      v
dbt Incremental Models
Example: Batch ingestion using Python
python
import json
import logging
from pathlib import Path
 
import requests
from google.cloud import storage
 
logger = logging.getLogger(__name__)
 
 
def extract_orders(api_url, api_key):
 
    response = requests.get(
        api_url,
        headers={"Authorization": f"Bearer {api_key}"},
        timeout=30
    )
 
    response.raise_for_status()
 
    return response.json()
 
 
def upload_orders_to_gcs(
    records,
    bucket_name,
    batch_date
):
 
    client = storage.Client()
    bucket = client.bucket(bucket_name)
 
    object_name = (
        f"bronze/orders/"
        f"batch_date={batch_date}/orders.json"
    )
 
    blob = bucket.blob(object_name)
 
    payload = json.dumps(records)
 
    blob.upload_from_string(
        payload,
        content_type="application/json"
    )
 
    logger.info(
        "Uploaded %s orders to gs://%s/%s",
        len(records),
        bucket_name,
        object_name
    )
 
 
def main():
 
    orders = extract_orders(
        "https://vendor.example.com/orders",
        "API_KEY_FROM_SECRET_MANAGER"
    )
 
    upload_orders_to_gcs(
        orders,
        "company-data-platform",
        "2026-11-09"
    )
 
 
if __name__ == "__main__":
    main()
Explanation:
•	Python calls the source API.
•	`raise_for_status()` catches unsuccessful HTTP responses.
•	GCS stores the extracted JSON.
•	The batch date makes the data discoverable and traceable.
Production additions should include API pagination, secret retrieval, retry policies, rate limiting, atomic batch manifests and duplicate-safe object naming. Do not overwrite an existing batch object without a controlled replay policy.
2.2 Streaming processing
Streaming processes records continuously as they arrive.
Examples:
•	Banking transactions.
•	Payment events.
•	Customer profile updates.
•	Website clickstreams.
•	Inventory changes.
Important concepts:
Concept	Explanation
Producer	Sends events
Topic	Logical event stream
Partition	Parallel processing unit
Consumer	Reads events
Offset	Position in Kafka partition
Checkpoint	Processing recovery state
Watermark	Event-time progress estimate
Window	Groups events over a time interval
Dead-letter queue	Stores unprocessable events
2.3 Event time vs processing time vs arrival time
This is particularly important for senior interviews.
Consider:
Event	Timestamp
Customer changes city	10:00 AM
CDC event reaches Kafka	10:04 AM
Spark processes event	10:06 AM
Event lands in GCS	10:07 AM
Three different times exist:
•	Event/commit time: when the source change occurred.
•	Arrival time: when the event reached the ingestion platform.
•	Processing time: when the processing engine handled it.
Do not use ingestion time as a substitute for source event time when building historical dimensions.
Interview question: What happens when a 10 AM event arrives at 2 PM?
Answer:
It depends on the processing requirements.
For current-state processing, I compare the source event timestamp against the existing target state.
For historical processing, I insert the event into its correct event-time position and recalculate the validity interval.
For streaming aggregations, I use event-time windows and watermarks. Events arriving within the accepted lateness threshold can update previous window results. Events arriving beyond that threshold require a late-data recovery path.
2.4 CDC architecture
CDC captures inserts, updates and deletes from operational databases.
Common approaches:
•	Database transaction-log CDC.
•	Debezium.
•	Google Cloud Datastream.
•	Application-level event publishing.
•	Timestamp-based incremental extraction.
A CDC event should ideally include:
json
{
  "event_id": "evt-10001",
  "customer_id": 101,
  "operation": "UPDATE",
  "before": {
    "city": "Delhi"
  },
  "after": {
    "city": "Kolkata"
  },
  "source_commit_timestamp": "2026-11-09T10:00:00",
  "source_sequence": "00000001234",
  "arrival_timestamp": "2026-11-09T10:04:00"
}
The sequence/transaction metadata helps distinguish events that share timestamps.
Interview question: Why is the source timestamp not always sufficient?
Answer:
Two events may have identical timestamps due to timestamp precision, transaction behavior or source-system implementation.
For deterministic ordering, I prefer a composite ordering key:
text
source_commit_timestamp
source_transaction_sequence
source_event_sequence
event_id
The exact fields depend on the CDC provider.
Chapter 3. Google Cloud Storage – Advanced Engineering
3.1 Storage classes
Storage class	Typical purpose
Standard	Frequently accessed Bronze and Silver
Nearline	Infrequently accessed data
Coldline	Rarely accessed archive
Archive	Long-term archival
Choose storage classes based on access patterns, retrieval charges, minimum storage duration and recovery requirements.
3.2 GCS lifecycle configuration
Example lifecycle configuration:
json
{
  "rule": [
    {
      "action": {
        "type": "SetStorageClass",
        "storageClass": "NEARLINE"
      },
      "condition": {
        "age": 90,
        "matchesPrefix": [
          "bronze/"
        ]
      }
    },
    {
      "action": {
        "type": "SetStorageClass",
        "storageClass": "ARCHIVE"
      },
      "condition": {
        "age": 365,
        "matchesPrefix": [
          "archive/"
        ]
      }
    }
  ]
}
Apply with:
bash
gcloud storage buckets update \
  gs://company-data-platform \
  --lifecycle-file=lifecycle.json
This example is illustrative. Validate the lifecycle conditions against your retention policy before applying them. Lifecycle transitions do not replace backup and recovery planning.
3.3 GCS file discovery and manifests
A common production problem is identifying which files belong to a completed batch.
Use a manifest:
json
{
  "batch_id": "orders_20261109_001",
  "source": "ERP",
  "record_count": 500000,
  "files": [
    "bronze/orders/part-0001.json",
    "bronze/orders/part-0002.json"
  ],
  "status": "COMPLETED"
}
A manifest helps with:
•	File completeness.
•	Duplicate batch prevention.
•	Reprocessing.
•	Record-count reconciliation.
•	Auditability.
3.4 Interview questions: GCS
Q1. What happens if a Spark job fails after writing five of ten Parquet files?
Answer: I do not assume that the output folder represents a completed batch. I use batch-specific staging prefixes, a commit protocol and a manifest written only after successful completion. Consumers process only committed batches. Failed output is isolated for cleanup or retry.
Q2. How do you avoid small-file problems?
Answer: I tune Spark output partitions, use coalesce() when reducing output files, compact existing files and target sensible file sizes. I avoid forcing a single partition because that can create a bottleneck.
Q3. How do you protect GCS data?
Answer: I use least-privilege IAM, service accounts, uniform bucket-level access, encryption, retention policies and versioning where appropriate. I also separate operational permissions between Bronze, Silver and Gold.
Chapter 4. Snowflake – Advanced Architecture and SQL Engineering
Snowflake is one of the most important areas for a Senior Data Engineer interview. You should understand not only how to write SQL, but also how Snowflake executes queries, manages storage, allocates compute and handles concurrent workloads.
4.1 Snowflake architecture
Snowflake Deep Dive: Architecture, Performance, and How It Compares to BigQuery & Redshift
[daisuke.masuda.tokyo](<https://daisuke.masuda.tokyo/article-2026-05-25-1132?utm_source=chatgpt.com>)
Snowflake architecture has three major layers.
Layer	Responsibility
Cloud Services	Authentication, metadata, query optimization, access control
Compute	Virtual warehouses executing SQL
Storage	Compressed, columnar micro-partitioned data
Cloud Services
Responsible for:
•	Query parsing and optimization.
•	Authentication and authorization.
•	Metadata management.
•	Transaction coordination.
•	Infrastructure management.
Compute layer
A virtual warehouse executes queries.
Warehouse sizes:
Warehouse	Relative compute
X-Small	1 credit/hour baseline
Small	2
Medium	4
Large	8
X-Large	16
2X-Large	32
These are the standard relative credit multipliers for a running warehouse, excluding other billing considerations.
Storage layer
Snowflake stores data in compressed columnar micro-partitions.
Important characteristics:
•	Automatic micro-partition creation.
•	Metadata for partition pruning.
•	Columnar storage.
•	Compression.
•	Automatic management of physical storage.
4.2 Snowflake table types
Table type	Usage
Permanent	Production tables
Transient	Intermediate tables without Fail-safe
Temporary	Session-specific temporary data
External table	Query files stored outside Snowflake
Dynamic table	Declarative, automatically refreshed transformations
Interview question: Permanent vs transient vs temporary?
Answer:
Permanent tables are suitable for important production datasets and have Time Travel and Fail-safe according to the applicable retention configuration.
Transient tables are suitable for rebuildable intermediate data. They have no Fail-safe.
Temporary tables exist only for the session in which they were created.
I choose based on retention, recovery and storage requirements rather than using permanent tables for everything.
4.3 Snowflake MERGE – Advanced CDC implementation
Consider these incoming events:
Customer	City	Commit timestamp
101	Delhi	10:00
101	Mumbai	11:00
101	Kolkata	09:00
The target currently has Mumbai at 11:00.
The Kolkata event must not overwrite Mumbai.
Production-style SCD1 MERGE
First deduplicate the source:
sql
CREATE OR REPLACE TEMP TABLE STG_CUSTOMER_LATEST AS
 
SELECT
    customer_id,
    city,
    commit_timestamp,
    arrival_timestamp,
    event_id
 
FROM STG_CUSTOMER_CDC
 
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY customer_id
    ORDER BY
        commit_timestamp DESC,
        arrival_timestamp DESC,
        event_id DESC
) = 1;
Now perform the merge:
sql
MERGE INTO CUSTOMER_CURRENT T
 
USING STG_CUSTOMER_LATEST S
 
ON T.customer_id = S.customer_id
 
WHEN MATCHED
AND S.commit_timestamp > T.updated_at
 
THEN UPDATE SET
 
    T.city = S.city,
    T.updated_at = S.commit_timestamp,
    T.last_event_id = S.event_id
 
WHEN NOT MATCHED THEN
 
INSERT (
    customer_id,
    city,
    updated_at,
    last_event_id
)
 
VALUES (
    S.customer_id,
    S.city,
    S.commit_timestamp,
    S.event_id
);
Explanation:
4.	`ROW_NUMBER()` removes duplicate source records.
5.	The newest source event is selected.
6.	`MERGE` matches the business key.
7.	The timestamp guard prevents an old event from replacing newer state.
8.	New customers are inserted.
For production, also consider deletes, null-safe comparisons, equal-timestamp conflict resolution and concurrent writers. If two valid changes share a timestamp, the source sequence number should be part of the ordering and comparison logic.
4.4 Snowflake window functions
You should be comfortable with:
•	ROW\_NUMBER()
•	RANK()
•	DENSE\_RANK()
•	LEAD()
•	LAG()
•	FIRST\_VALUE()
•	LAST\_VALUE()
•	SUM() OVER()
•	COUNT() OVER()
Example 1: Find duplicate records
sql
SELECT *
 
FROM STG_ORDERS
 
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_id
    ORDER BY updated_at DESC
) > 1;
Example 2: Compare current and previous transaction
sql
SELECT
    customer_id,
    transaction_date,
    amount,
 
    LAG(amount) OVER (
        PARTITION BY customer_id
        ORDER BY transaction_date
    ) AS previous_amount,
 
    amount -
    LAG(amount) OVER (
        PARTITION BY customer_id
        ORDER BY transaction_date
    ) AS amount_difference
 
FROM CUSTOMER_TRANSACTIONS;
Example 3: Running customer expenditure
sql
SELECT
    customer_id,
    transaction_date,
    amount,
 
    SUM(amount) OVER (
        PARTITION BY customer_id
        ORDER BY transaction_date
        ROWS BETWEEN UNBOUNDED PRECEDING
        AND CURRENT ROW
    ) AS running_total
 
FROM CUSTOMER_TRANSACTIONS;
Example 4: Top three orders per customer
sql
SELECT *
 
FROM CUSTOMER_ORDERS
 
QUALIFY DENSE_RANK() OVER (
    PARTITION BY customer_id
    ORDER BY order_amount DESC
) <= 3;
4.5 Snowflake Time Travel
Time Travel allows querying historical versions of supported objects within their retention period.
Example:
sql
SELECT *
 
FROM CUSTOMER_CURRENT
AT (
    OFFSET => -3600
);
This queries the table approximately one hour earlier.
Recover a dropped table:
sql
UNDROP TABLE CUSTOMER_CURRENT;
Create a clone from an earlier version:
sql
CREATE TABLE CUSTOMER_RECOVERY
 
CLONE CUSTOMER_CURRENT
AT (
    TIMESTAMP => '2026-11-09 10:00:00'::TIMESTAMP_NTZ
);
Interview question: What is the difference between Time Travel and Fail-safe?
Answer:
•	Time Travel provides a user-accessible historical recovery capability during the configured retention period.
•	Fail-safe is a separate disaster-recovery mechanism operated by Snowflake for eligible permanent data after Time Travel expires.
•	Fail-safe is not intended as a normal user-controlled backup or query mechanism.
4.6 Zero-copy cloning
sql
CREATE TABLE DEV_CUSTOMER_CLONE
CLONE PROD_CUSTOMER;
Snowflake initially shares underlying micro-partitions rather than copying all physical data.
When either clone or original changes, additional storage is used for changed data.
Use cases:
•	Development environments.
•	Testing migrations.
•	Release validation.
•	Data recovery.
•	Isolated experimentation.
Interview question: Why use zero-copy cloning instead of copying a 5 TB table?
Answer:
Cloning avoids immediately duplicating the entire physical dataset, making environment creation much faster and potentially cheaper. Storage diverges as changes occur, so I still monitor clone retention and subsequent modifications.
Chapter 5. Snowflake Performance Optimization
5.1 Micro-partition pruning
Snowflake maintains metadata such as minimum and maximum values for columns in micro-partitions.
Consider:
sql
SELECT *
FROM SALES_FACT
WHERE ORDER_DATE >= '2026-01-01'
  AND ORDER_DATE < '2026-02-01';
Snowflake can skip partitions whose metadata proves that they cannot contain matching dates.
Avoid wrapping filtered columns in functions when a range predicate is possible.
Bad:
sql
WHERE TO_CHAR(order_date, 'YYYY-MM') = '2026-01'
Better:
sql
WHERE order_date >= '2026-01-01'
  AND order_date < '2026-02-01'
5.2 Query Profile analysis
Boost Snowflake Speed with Query Profile Mastery
[articles.analytics.today](<https://articles.analytics.today/improving-snowflake-performance-by-mastering-the-query-profile?utm_source=chatgpt.com>)
When investigating a slow query, check:
Metric	Possible issue
Partitions scanned	Poor pruning
Partitions total	Table size
Bytes scanned	Excessive scanning
Local spill	Insufficient memory or large operations
Remote spill	More severe resource pressure
Queue time	Concurrency
Execution time	Actual query processing
Network transfer	Large distributed joins or data movement
Example: Query History
sql
SELECT
    QUERY_ID,
    QUERY_TEXT,
    WAREHOUSE_NAME,
    EXECUTION_TIME,
    QUEUED_OVERLOAD_TIME,
    BYTES_SCANNED,
    PARTITIONS_SCANNED,
    PARTITIONS_TOTAL,
    BYTES_SPILLED_TO_LOCAL_STORAGE,
    BYTES_SPILLED_TO_REMOTE_STORAGE
 
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
 
WHERE START_TIME >= DATEADD(
    day,
    -7,
    CURRENT_TIMESTAMP()
)
 
ORDER BY EXECUTION_TIME DESC
 
LIMIT 50;
Note that Account Usage views have ingestion latency. For immediate troubleshooting, use the appropriate Information Schema query-history functions or Snowsight.
5.3 Warehouse scaling
There are two different problems.
Problem A: Single query is slow.
Possible fixes:
•	Increase warehouse size.
•	Improve join conditions.
•	Reduce data scanned.
•	Improve partition pruning.
•	Reduce unnecessary sorting.
•	Optimize aggregation.
Problem B: Multiple queries are queued.
Possible fixes:
•	Multi-cluster warehouses.
•	Workload isolation.
•	Separate ETL and BI warehouses.
•	Query prioritization and scheduling.
•	Resource monitoring.
Example:
sql
CREATE OR REPLACE WAREHOUSE BI_WH
 
WITH
    WAREHOUSE_SIZE = 'MEDIUM',
    MIN_CLUSTER_COUNT = 1,
    MAX_CLUSTER_COUNT = 4,
    SCALING_POLICY = 'STANDARD',
    AUTO_SUSPEND = 60,
    AUTO_RESUME = TRUE;
Interview question: Should you increase the warehouse size when queries are queued?
Answer:
Not automatically.
First I distinguish queueing from execution bottlenecks.
If queries are queued because all warehouse clusters are busy, adding clusters can improve concurrency.
If one query is spilling heavily, increasing warehouse size may help that query.
If SQL scans unnecessary data, I fix the SQL first.
5.4 Clustering
sql
CREATE TABLE SALES_FACT (
    SALE_ID VARCHAR,
    CUSTOMER_ID NUMBER,
    ORDER_DATE DATE,
    REGION_ID NUMBER,
    AMOUNT NUMBER(18,2)
)
CLUSTER BY (ORDER_DATE, REGION_ID);
Check clustering:
sql
SELECT SYSTEM$CLUSTERING_INFORMATION(
    'SALES_FACT',
    '(ORDER_DATE, REGION_ID)'
);
Clustering is useful when natural loading patterns produce poor physical locality for frequently filtered columns.
It is not mandatory for every large table.
5.5 Search Optimization Service
sql
ALTER TABLE CUSTOMER_SUPPORT
ADD SEARCH OPTIMIZATION ON EQUALITY(EMAIL);
This can help supported selective point-lookup queries.
For example:
sql
SELECT *
FROM CUSTOMER_SUPPORT
WHERE EMAIL = 'customer@example.com';
Search optimization is not a replacement for clustering. Its usefulness depends on the query pattern, selectivity and maintenance cost.
5.6 Materialized Views
sql
CREATE MATERIALIZED VIEW MV_DAILY_SALES AS
 
SELECT
    ORDER_DATE,
    REGION_ID,
    SUM(AMOUNT) AS TOTAL_SALES
 
FROM SALES_FACT
 
GROUP BY
    ORDER_DATE,
    REGION_ID;
Materialized views can improve repeated supported query patterns, but Snowflake restricts the SQL allowed in their definitions and manages maintenance separately.
5.7 Snowflake Streams
A Stream tracks changes to a source table using change-tracking metadata.
Example:
sql
CREATE OR REPLACE STREAM CUSTOMER_STREAM
ON TABLE CUSTOMER_RAW;
Inspect changed records:
sql
SELECT *
FROM CUSTOMER_STREAM;
Important metadata:
•	`METADATA$ACTION`
•	`METADATA$ISUPDATE`
•	`METADATA$ROW_ID`
Example processing task:
sql
CREATE OR REPLACE TABLE CUSTOMER_CHANGES AS
 
SELECT
    CUSTOMER_ID,
    CITY,
    METADATA$ACTION AS ACTION_TYPE,
    METADATA$ISUPDATE AS IS_UPDATE
 
FROM CUSTOMER_STREAM;
Important: Streams track changes, not a complete business-event history. Their offset advances when the stream is consumed in a committed DML transaction. Use appropriate transaction boundaries and retention monitoring.
5.8 Snowflake Tasks
Tasks execute scheduled or triggered SQL.
sql
CREATE OR REPLACE TASK LOAD_CUSTOMER_TASK
 
WAREHOUSE = ETL_WH
 
SCHEDULE = '5 MINUTE'
 
AS
 
MERGE INTO CUSTOMER_CURRENT T
 
USING CUSTOMER_STREAM S
 
ON T.CUSTOMER_ID = S.CUSTOMER_ID
 
WHEN MATCHED THEN
UPDATE SET
    T.CITY = S.CITY
 
WHEN NOT MATCHED THEN
INSERT (
    CUSTOMER_ID,
    CITY
)
VALUES (
    S.CUSTOMER_ID,
    S.CITY
);
Start the task:
sql
ALTER TASK LOAD_CUSTOMER_TASK RESUME;
A production implementation must account for update and delete semantics, source deduplication, timestamp ordering and stream consumption behavior. The abbreviated example above is not a complete CDC implementation.
5.9 Snowpipe
Snowpipe supports continuous file-based ingestion from cloud storage.
For GCS, Snowflake's supported GCS notification integration can be configured with the required cloud notification infrastructure.
Conceptually:
text
GCS Bucket
    |
    v
Cloud Notification
    |
    v
Snowflake Notification Integration
    |
    v
Snowpipe
    |
    v
Snowflake Raw Table
Example pipe:
sql
CREATE OR REPLACE PIPE ORDERS_PIPE
 
AUTO_INGEST = TRUE
 
AS
 
COPY INTO STG_ORDERS
 
FROM @GCS_ORDERS_STAGE
 
FILE_FORMAT = (
    TYPE = PARQUET
);
The notification integration and cloud-side notification setup are required for automatic ingestion; creating the pipe alone does not establish the complete GCS event flow.
Snowpipe vs Snowpipe Streaming
Snowpipe	Snowpipe Streaming
File-based ingestion	Row/event-oriented ingestion
Cloud object storage	Streaming clients
Files trigger ingestion	Clients send records
Suitable for micro-batches	Suitable for low-latency ingestion
Senior interview question: When would you use Kafka, GCS and Snowpipe together?
Answer:
I would use Kafka as the event transport, Spark or Dataflow for transformation, GCS for durable raw and curated storage, and Snowpipe for file-based ingestion into Snowflake.
If the requirement is very low-latency ingestion and I don't need to land intermediate files first, I would evaluate Snowpipe Streaming.
Chapter 6. dbt Core – Advanced Engineering
dbt is responsible for transforming raw warehouse data into governed, tested business datasets.
6.1 dbt layered architecture
text
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
BI / ANALYTICS
Layer	Materialization	Purpose
Staging	View	Clean source fields
Intermediate	Ephemeral / View	Reusable business logic
Core	Incremental / Table	Fact and dimension models
Mart	Table / Incremental	Reporting datasets
6.2 dbt project structure
text
dbt_project/
│
├── dbt_project.yml
├── packages.yml
├── models/
│   ├── staging/
│   │   ├── stg_customers.sql
│   │   ├── stg_orders.sql
│   │   └── sources.yml
│   │
│   ├── intermediate/
│   │   ├── int_orders_enriched.sql
│   │   └── int_customer_history.sql
│   │
│   ├── marts/
│   │   ├── core/
│   │   │   ├── fct_orders.sql
│   │   │   └── dim_customer.sql
│   │   └── reporting/
│   │       └── mart_monthly_sales.sql
│
├── snapshots/
│   └── customer_snapshot.sql
│
├── macros/
│   └── generate_surrogate_key.sql
│
├── tests/
│   └── assert_no_negative_sales.sql
│
└── seeds/
    └── country_mapping.csv
6.3 dbt source configuration
yaml
version: 2
 
sources:
  - name: raw_erp
    database: RAW_DB
    schema: ERP
    tables:
      - name: customers
      - name: orders
      - name: payments
Model:
sql
SELECT
    CUSTOMER_ID,
    CUSTOMER_NAME,
    CITY,
    UPDATED_AT
 
FROM {{ source('raw_erp', 'customers') }}
6.4 dbt ref()
sql
SELECT *
 
FROM {{ ref('stg_orders') }}
Difference:
•	`source()` references externally loaded raw source tables.
•	`ref()` references another dbt model.
dbt uses these dependencies to build its DAG
___________________________________________________________________________________________________________________________________________________________________
Senior Data Engineer Master SOP – Part 2
Advanced Snowflake, dbt, GCS, CDC, Real-Time Streaming, PySpark & Production Engineering
This continues the previous handbook, using Google Cloud Storage (GCS), not Amazon S3, throughout the architecture and code examples.
This part focuses on the advanced subjects expected of a Senior Data Engineer who must design, build, troubleshoot and operate enterprise data platforms.
The emphasis is on:
•	Production-ready SQL and Python implementations.
•	Architecture decisions and trade-offs.
•	Batch versus streaming processing.
•	Snowflake advanced capabilities.
•	dbt engineering patterns.
•	CDC and SCD Type 1/2.
•	GCS and Google Cloud services.
•	Interview questions with practical benchmark answers.
•	Failure recovery and operational troubleshooting.
15. Advanced Snowflake Engineering
15.1 Snowflake architecture
Snowflake has three major architectural layers.
Layer	Responsibility	Examples
Database storage	Persistent data storage	Micro-partitions, compressed columnar data
Compute	Query execution	Virtual warehouses
Cloud services	Metadata and coordination	Authentication, optimization, transaction management
Interview question: How does Snowflake separate storage and compute?
Answer:
Snowflake stores data independently of virtual warehouses.
This allows multiple warehouses to query the same datasets without maintaining separate physical copies.
For example:
•	ETL warehouse processes ingestion.
•	BI warehouse serves dashboards.
•	Data science warehouse runs analytical queries.
•	Each can scale independently.
This architecture reduces workload contention and provides independent workload management.
15.2 Virtual warehouse sizing
Warehouse	Typical workload
X-Small	Development and small queries
Small	Light transformations
Medium	Moderate ETL
Large	Heavy transformations
X-Large	Large-scale processing
2X-Large and above	Very large workloads
Warehouse size increases compute resources, but query performance does not necessarily double with every size increase.
Create a warehouse
sql
CREATE WAREHOUSE ETL_WH
WITH
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 3
    SCALING_POLICY = 'STANDARD';
Explanation:
•	`AUTO_SUSPEND`: Stops idle compute.
•	`AUTO_RESUME`: Automatically starts when a query arrives.
•	`MIN_CLUSTER_COUNT`: Minimum cluster count for a multi-cluster warehouse.
•	`MAX_CLUSTER_COUNT`: Maximum cluster count.
•	`STANDARD`: More responsive scaling under concurrency pressure.
•	`ECONOMY`: Prioritizes resource utilization and can reduce credit consumption.
Interview question: When should you scale up versus scale out?
Answer:
•	Scale up when an individual query is resource-intensive, spilling to local or remote storage, or taking too long due to insufficient compute.
•	Scale out when multiple queries are queuing because of concurrency.
•	Optimize SQL first, then choose the appropriate warehouse configuration.
15.3 Snowflake query performance troubleshooting
Start with query history:
sql
SELECT
    QUERY_ID,
    QUERY_TEXT,
    EXECUTION_STATUS,
    TOTAL_ELAPSED_TIME,
    EXECUTION_TIME,
    QUEUED_PROVISIONING_TIME,
    QUEUED_OVERLOAD_TIME,
    BYTES_SCANNED,
    PARTITIONS_SCANNED,
    PARTITIONS_TOTAL,
    BYTES_SPILLED_TO_LOCAL_STORAGE,
    BYTES_SPILLED_TO_REMOTE_STORAGE
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME >= DATEADD('DAY', -1, CURRENT_TIMESTAMP())
ORDER BY TOTAL_ELAPSED_TIME DESC
LIMIT 50;
How to interpret the metrics
Metric	What it tells you
`BYTES_SCANNED`	Amount of data scanned
`PARTITIONS_SCANNED`	Number of scanned micro-partitions
`PARTITIONS_TOTAL`	Total micro-partitions considered
`QUEUED_OVERLOAD_TIME`	Time waiting for available compute
`BYTES_SPILLED_TO_LOCAL_STORAGE`	Data spilled to local disk
`BYTES_SPILLED_TO_REMOTE_STORAGE`	Data spilled to remote storage
A useful pruning indicator is:
$$
Pruning\ Ratio = 1 - \frac{Partitions\ Scanned}{Partitions\ Total}
$$
For example, if 100 partitions exist and only 10 are scanned, the pruning ratio is 90%.
This is a diagnostic indicator, not a universal performance target.
15.4 Snowflake caching
Snowflake has multiple caching mechanisms:
9.	Persisted query result cache.
10.	Warehouse local data cache.
11.	Metadata caching and optimization.
Example:
sql
SELECT
    customer_id,
    SUM(order_amount)
FROM FACT_ORDERS
WHERE order_date >= '2026-01-01'
GROUP BY customer_id;
If the same eligible query runs again and the underlying data and query conditions satisfy result-cache requirements, Snowflake may return the cached result.
Interview question: Why might a query run faster the second time?
Possible reasons include:
•	Result cache reuse.
•	Warehouse local data cache.
•	Warmed-up compute.
•	Metadata reuse.
Do not assume every repeated query uses the result cache.
16. Advanced Snowflake Features
16.1 Snowflake Streams – CDC implementation
Streams track changes made to a source table using change-tracking metadata.
Example:
sql
CREATE OR REPLACE TABLE CUSTOMER_SOURCE (
    CUSTOMER_ID NUMBER,
    CUSTOMER_NAME VARCHAR,
    CITY VARCHAR,
    UPDATED_AT TIMESTAMP_NTZ
);
Create a stream:
sql
CREATE OR REPLACE STREAM CUSTOMER_STREAM
ON TABLE CUSTOMER_SOURCE
APPEND_ONLY = FALSE;
Insert sample data:
sql
INSERT INTO CUSTOMER_SOURCE
VALUES
(101, 'John', 'London', CURRENT_TIMESTAMP()),
(102, 'David', 'New York', CURRENT_TIMESTAMP());
Read CDC changes:
sql
SELECT *
FROM CUSTOMER_STREAM;
Important metadata columns include:
•	`METADATA$ACTION`
•	`METADATA$ISUPDATE`
•	`METADATA$ROW_ID`
Process stream changes into a target table
sql
MERGE INTO CUSTOMER_TARGET T
USING (
    SELECT *
    FROM CUSTOMER_STREAM
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY CUSTOMER_ID
        ORDER BY UPDATED_AT DESC
    ) = 1
) S
ON T.CUSTOMER_ID = S.CUSTOMER_ID
 
WHEN MATCHED AND S.METADATA$ACTION = 'DELETE'
THEN DELETE
 
WHEN MATCHED AND S.METADATA$ACTION = 'INSERT'
THEN UPDATE SET
    T.CUSTOMER_NAME = S.CUSTOMER_NAME,
    T.CITY = S.CITY,
    T.UPDATED_AT = S.UPDATED_AT
 
WHEN NOT MATCHED AND S.METADATA$ACTION = 'INSERT'
THEN INSERT (
    CUSTOMER_ID,
    CUSTOMER_NAME,
    CITY,
    UPDATED_AT
)
VALUES (
    S.CUSTOMER_ID,
    S.CUSTOMER_NAME,
    S.CITY,
    S.UPDATED_AT
);
Important: This is an illustrative pattern. A production stream consumer must correctly account for update pairs, deletes, event ordering and the fact that stream offsets advance when the consuming transaction commits. Do not independently select the latest row and assume it preserves all CDC semantics.
16.2 Snowflake Tasks
Tasks execute SQL automatically according to a schedule or dependency.
sql
CREATE OR REPLACE TASK LOAD_CUSTOMER_TASK
WAREHOUSE = ETL_WH
SCHEDULE = '5 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('CUSTOMER_STREAM')
AS
MERGE INTO CUSTOMER_TARGET T
USING CUSTOMER_STREAM S
ON T.CUSTOMER_ID = S.CUSTOMER_ID
 
THEN DELETE
 
THEN UPDATE SET
    T.CITY = S.CITY,
    T.UPDATED_AT = S.UPDATED_AT
 
THEN INSERT (
    CUSTOMER_ID, CITY, UPDATED_AT
)
VALUES (
    S.CUSTOMER_ID, S.CITY, S.UPDATED_AT
);
Start the task:
sql
ALTER TASK LOAD_CUSTOMER_TASK RESUME;
Monitor:
sql
SELECT *
FROM TABLE(
    INFORMATION_SCHEMA.TASK_HISTORY(
        SCHEDULED_TIME_RANGE_START =>
            DATEADD('DAY', -1, CURRENT_TIMESTAMP())
    )
)
ORDER BY SCHEDULED_TIME DESC;
16.3 Snowpipe
Snowpipe supports continuous file ingestion from supported cloud storage integrations.
For GCS, Snowpipe uses cloud event notifications delivered through the supported notification integration mechanism.
Typical architecture:
text
SOURCE SYSTEM
     |
     v
GCS BRONZE
     |
     v
GCS OBJECT CREATED EVENT
     |
     v
GOOGLE CLOUD PUB/SUB
     |
     v
SNOWFLAKE NOTIFICATION INTEGRATION
     |
     v
SNOWPIPE
     |
     v
SNOWFLAKE RAW TABLE
     |
     v
DBT TRANSFORMATION
Example pipe:
sql
CREATE OR REPLACE PIPE CUSTOMER_PIPE
AUTO_INGEST = TRUE
INTEGRATION = 'GCS_NOTIFICATION_INTEGRATION'
AS
COPY INTO RAW_CUSTOMERS
FROM @GCS_CUSTOMER_STAGE
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
The notification integration must be configured with the required GCP Pub/Sub topic and Snowflake subscription permissions. The exact configuration depends on your cloud region and integration setup.
Snowpipe versus Snowpipe Streaming
Feature	Snowpipe	Snowpipe Streaming
Input	Files	Rows/events
Typical source	GCS, supported cloud storage	Streaming applications
Latency	File notification and ingestion dependent	Low-latency row ingestion
Best use	Micro-batch files	Continuous event ingestion
17. Advanced dbt Engineering
dbt is not just a SQL transformation tool. In enterprise environments, it provides a transformation framework with dependency management, testing, documentation, lineage and deployment capabilities.
17.1 Complete dbt project structure
text
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
│   │   ├── int_customer_orders.sql
│   │   └── int_order_enrichment.sql
│   │
│   └── marts/
│       ├── dimensions/
│       │   ├── dim_customer.sql
│       │   └── dim_product.sql
│       │
│       ├── facts/
│       │   ├── fct_orders.sql
│       │   └── fct_sales.sql
│       │
│       └── reporting/
│           └── rpt_monthly_revenue.sql
│
├── snapshots/
│   └── customer_snapshot.sql
│
├── macros/
│   ├── generate_surrogate_key.sql
│   └── audit_columns.sql
│
├── tests/
│   ├── assert_positive_amount.sql
│   └── assert_valid_customer.sql
│
├── seeds/
│   └── country_codes.csv
│
└── analyses/
    └── customer_analysis.sql
Explanation of directories
Directory	Purpose
staging	Clean and standardize raw data
intermediate	Reusable business logic
marts	Facts, dimensions and reporting models
snapshots	Historical change tracking
macros	Reusable Jinja SQL
tests	Custom data quality checks
seeds	Small reference datasets
analyses	Ad hoc analytical SQL
17.2 dbt source configuration
models/staging/sources.yml
yaml
version: 2
 
sources:
  - name: snowflake_raw
    database: RAW_DB
    schema: ERP
 
    tables:
      - name: customers
        description: Raw customer records
 
      - name: orders
        description: Raw order records
 
      - name: products
        description: Raw product records
Use these sources in models:
sql
SELECT *
FROM {{ source('snowflake_raw', 'customers') }}
This creates a dependency between the dbt model and its declared source.
17.3 dbt snapshots – SCD Type 2
Snapshots capture changes in mutable source records.
Create snapshots/customer_snapshot.sql:
sql
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
 
SELECT
    customer_id,
    customer_name,
    email,
    city,
    tier,
    updated_at
 
 
{% endsnapshot %}
Execute:
bash
dbt snapshot --target prod
How does the timestamp strategy work?
Suppose the source contains:
Customer	City	Updated timestamp
101	London	1 Jan
101	New York	10 Jan
101	Paris	20 Jan
The snapshot maintains historical versions and tracks their validity.
Typical snapshot metadata:
•	`dbt_valid_from`
•	`dbt_valid_to`
•	`dbt_scd_id`
•	`dbt_updated_at`
When a new version is detected, dbt closes the previous version and creates a new one.
Important: invalidate_hard_deletes=True is available in supported dbt snapshot configurations. It invalidates missing source records when a snapshot runs, but it does not physically delete historical snapshot records.
Interview question: When do you use dbt snapshots versus custom SCD2?
Answer:
I use snapshots when I need to preserve changes to mutable source records and the snapshot execution frequency is sufficient for the business requirement.
I use custom SCD2 processing when I need:
•	Full CDC event history.
•	Source commit timestamp accuracy.
•	Late-arriving event reconstruction.
•	Multiple changes between snapshot executions.
•	Complex deletion and reactivation logic.
•	Explicit business-effective dates.
Snapshots cannot reconstruct changes that happened between runs if the source only exposes its latest state.
17.4 dbt incremental strategies
dbt supports multiple incremental strategies, depending on the adapter.
Strategy	Explanation	Suitable workload
append	Insert new records	Immutable events
merge	Insert/update matching keys	Current-state facts
delete+insert	Delete matching keys and insert replacements	Partition or key-based reloads
insert\_overwrite	Replace affected partitions where supported	Partitioned datasets
Example: Append-only event table
sql
{{
    config(
        materialized='incremental',
        incremental_strategy='append'
    )
}}
 
SELECT
    event_id,
    customer_id,
    event_type,
    event_timestamp,
    arrival_timestamp
 
FROM {{ source('snowflake_raw', 'customer_events') }}
 
{% if is_incremental() %}
 
WHERE arrival_timestamp > (
    SELECT COALESCE(
        MAX(arrival_timestamp),
        '1900-01-01'::TIMESTAMP_NTZ
    )
    FROM {{ this }}
)
 
{% endif %}
This is suitable only when replay and duplicate handling are addressed separately. An arrival-time watermark by itself can miss records arriving with an older timestamp or arriving during a concurrent execution.
Example: Merge-based current-state model
sql
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='customer_id'
    )
}}
 
WITH ranked AS (
 
    SELECT
        customer_id,
        customer_name,
        city,
        commit_timestamp,
        arrival_timestamp
 
 
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY customer_id
        ORDER BY
            commit_timestamp DESC,
            arrival_timestamp DESC
    ) = 1
 
)
 
SELECT *
FROM ranked
 
{% if is_incremental() %}
 
WHERE commit_timestamp >= DATEADD(
    DAY,
    -2,
    (SELECT COALESCE(MAX(commit_timestamp),
                     '1900-01-01'::TIMESTAMP_NTZ)
     FROM {{ this }})
)
 
{% endif %}
For a production model, add an explicit timestamp guardrail to the generated merge logic or implement a custom materialization so that older events cannot overwrite newer states.
17.5 dbt macros
Macros allow reusable SQL logic.
Example: create an audit timestamp macro.
macros/audit_columns.sql
sql
{% macro audit_columns() %}
 
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS processed_at
 
{% endmacro %}
Use it:
sql
SELECT
    customer_id,
    customer_name,
    {{ audit_columns() }}
 
Another example: reusable date filtering.
sql
{% macro date_filter(column_name, start_date, end_date) %}
 
    {{ column_name }} >= '{{ start_date }}'
    AND {{ column_name }} < '{{ end_date }}'
 
{% endmacro %}
Usage:
sql
SELECT *
FROM {{ ref('fct_orders') }}
WHERE {{ date_filter(
    'order_date',
    '2026-01-01',
    '2027-01-01'
) }}
17.6 dbt custom data quality tests
Create tests/assert_positive_amount.sql:
sql
SELECT
    order_id,
    order_amount
 
FROM {{ ref('fct_orders') }}
 
WHERE order_amount < 0
A dbt singular test passes when it returns zero rows.
Generic reusable test
macros/test_non_negative.sql
sql
{% test non_negative(model, column_name) %}
 
SELECT *
FROM {{ model }}
 
WHERE {{ column_name }} < 0
 
{% endtest %}
Configure:
yaml
models:
  - name: fct_orders
    columns:
      - name: order_amount
        data_tests:
          - non_negative
Run:
bash
dbt test
Or run transformations and tests together:
bash
dbt build --target prod
17.7 dbt commands every senior engineer should know
Command	Purpose
`dbt debug`	Validate project and connection
`dbt deps`	Install package dependencies
`dbt parse`	Parse project and validate configuration
`dbt compile`	Generate compiled SQL
`dbt run`	Execute models
`dbt test`	Execute tests
`dbt build`	Build selected resources and run applicable tests
`dbt snapshot`	Execute snapshots
`dbt docs generate`	Generate documentation
`dbt source freshness`	Check source freshness
`dbt ls`	List project resources
`dbt retry`	Retry eligible failed nodes from a prior invocation
Selective execution
bash
# Run one model
dbt run --select fct_orders
 
# Run a model and its upstream dependencies
dbt run --select +fct_orders
 
# Run a model and downstream dependencies
dbt run --select fct_orders+
 
# Run a model and all upstream/downstream dependencies
dbt build --select +fct_orders+
 
# Run only changed models relative to a state manifest
dbt build --select state:modified+ --state ./previous_run
The state-based command requires a valid previous manifest and an appropriate comparison environment.
18. CDC Architecture – Advanced Production Patterns
CDC is one of the most important topics for senior data engineering interviews.
18.1 CDC event structure
A typical CDC event should carry more than just the business data.
Example:
json
{
  "event_id": "EVT10001",
  "operation": "UPDATE",
  "customer_id": 101,
  "before": {
    "city": "London"
  },
  "after": {
    "city": "New York"
  },
  "source_commit_timestamp": "2026-11-09T10:15:00Z",
  "arrival_timestamp": "2026-11-09T10:18:00Z",
  "source_transaction_id": "TXN10001",
  "source_lsn": "0000000000001234",
  "schema_version": 1
}
Why are these fields important?
Field	Purpose
event\_id	Duplicate detection
operation	INSERT, UPDATE, DELETE
before	Previous state
after	New state
source\_commit\_timestamp	Source ordering
arrival\_timestamp	Pipeline latency
source\_transaction\_id	Transaction tracking
source\_lsn	Source log ordering
schema\_version	Schema evolution
A source-specific sequence number or log position is generally a stronger ordering key than timestamps alone.
18.2 CDC processing architecture
text
       SOURCE DATABASE
              |
              v
       DATABASE LOGS
        WAL / REDO LOG
              |
              v
        DEBEZIUM / CDC
              |
              v
          KAFKA
       CUSTOMER TOPIC
              |
              v
       STREAM PROCESSING
         PYSPARK / BEAM
              |
              v
       GCS BRONZE LAYER
         RAW CDC EVENTS
              |
              v
       GCS SILVER LAYER
      VALIDATED CDC EVENTS
              |
              v
          SNOWFLAKE
              |
              v
       dbt CDC TRANSFORM
              |
       +------+------+
       |             |
       v             v
   SCD TYPE 1    SCD TYPE 2
   CURRENT       HISTORY
18.3 SCD Type 1 with CDC deletes
Consider a source event table:
sql
CREATE OR REPLACE TABLE STG_CUSTOMER_CDC (
    EVENT_ID VARCHAR,
    CUSTOMER_ID NUMBER,
    OPERATION VARCHAR,
    CUSTOMER_NAME VARCHAR,
    CITY VARCHAR,
    SOURCE_COMMIT_TIMESTAMP TIMESTAMP_NTZ,
    ARRIVAL_TIMESTAMP TIMESTAMP_NTZ
);
Deduplicate and apply current-state changes:
sql
MERGE INTO DIM_CUSTOMER_CURRENT T
 
USING (
 
    SELECT *
    FROM STG_CUSTOMER_CDC
 
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY EVENT_ID
        ORDER BY ARRIVAL_TIMESTAMP DESC
    ) = 1
 
) S
 
ON T.CUSTOMER_ID = S.CUSTOMER_ID
 
WHEN MATCHED
 AND S.OPERATION = 'DELETE'
 AND S.SOURCE_COMMIT_TIMESTAMP > T.UPDATED_AT
THEN DELETE
 
WHEN MATCHED
 AND S.OPERATION IN ('INSERT', 'UPDATE')
 AND S.SOURCE_COMMIT_TIMESTAMP > T.UPDATED_AT
THEN UPDATE SET
 
    T.CUSTOMER_NAME = S.CUSTOMER_NAME,
    T.CITY = S.CITY,
    T.UPDATED_AT = S.SOURCE_COMMIT_TIMESTAMP
 
WHEN NOT MATCHED
 AND S.OPERATION IN ('INSERT', 'UPDATE')
THEN INSERT (
    CUSTOMER_ID,
    CUSTOMER_NAME,
    CITY,
    UPDATED_AT
)
VALUES (
    S.CUSTOMER_ID,
    S.CUSTOMER_NAME,
    S.CITY,
    S.SOURCE_COMMIT_TIMESTAMP
);
Important production correction: The example illustrates the merge logic, but ROW_NUMBER() by event ID only removes duplicate deliveries of the same event. It does not resolve multiple different events for the same customer. You must also resolve ordering and multiple events per key, and handle deletes for keys not yet present in the target. If your source uses LSNs or transaction sequence numbers, use those as the ordering authority.
For robust processing, retain a CDC event ledger and a separate current-state table.
18.4 Correct handling of out-of-order events
Suppose the source produces:
Event	City	Commit timestamp	Arrival timestamp
E1	London	10:00	10:01
E2	Paris	10:05	10:06
E3	New York	10:03	10:10
Arrival order:
text
E1 → E2 → E3
Source order:
text
E1 → E3 → E2
The correct current state is Paris.
A simple MERGE without a source timestamp guardrail could incorrectly overwrite Paris with New York.
The senior-level answer is:
•	Preserve all events in the immutable event ledger.
•	Deduplicate by event ID.
•	Order events using source log sequence or commit ordering.
•	Apply only valid newer events to current state.
•	Reconstruct historical timelines for SCD2.
•	Periodically reconcile against the source.
19. SCD Type 1, Type 2, Type 3 and Hybrid Dimensions
19.1 SCD comparison
Type	Behavior	History
Type 0	Never change original value	Original only
Type 1	Overwrite existing value	No attribute history
Type 2	Create a new row/version	Full retained history
Type 3	Keep previous value in another column	Limited history
Type 4	Store current data and separate history table	Separate history
Type 6	Combination of Types 1, 2 and 3	Hybrid history
19.2 SCD Type 2 – Production implementation
Create dimension:
sql
CREATE OR REPLACE TABLE DIM_CUSTOMER_SCD2 (
    CUSTOMER_SK NUMBER AUTOINCREMENT,
    CUSTOMER_ID NUMBER,
    CUSTOMER_NAME VARCHAR,
    CITY VARCHAR,
    TIER VARCHAR,
    EFFECTIVE_START_DATE TIMESTAMP_NTZ,
    EFFECTIVE_END_DATE TIMESTAMP_NTZ,
    IS_CURRENT BOOLEAN,
    IS_DELETED BOOLEAN,
    RECORD_HASH VARCHAR
);
Step 1: Prepare deduplicated source
sql
 
SELECT
    CUSTOMER_ID,
    CUSTOMER_NAME,
    CITY,
    TIER,
    UPDATED_AT,
 
    HASH(
        CUSTOMER_NAME,
        CITY,
        TIER
    ) AS RECORD_HASH
 
FROM STG_CUSTOMERS
 
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CUSTOMER_ID
    ORDER BY UPDATED_AT DESC
) = 1;
Step 2: Expire changed records
sql
UPDATE DIM_CUSTOMER_SCD2 T
 
SET
    EFFECTIVE_END_DATE = S.UPDATED_AT,
    IS_CURRENT = FALSE
 
FROM STG_CUSTOMER_LATEST S
 
WHERE T.CUSTOMER_ID = S.CUSTOMER_ID
  AND T.IS_CURRENT = TRUE
  AND T.RECORD_HASH <> S.RECORD_HASH;
Step 3: Insert new versions
sql
INSERT INTO DIM_CUSTOMER_SCD2 (
    CUSTOMER_ID,
    CUSTOMER_NAME,
    CITY,
    TIER,
    EFFECTIVE_START_DATE,
    EFFECTIVE_END_DATE,
    IS_CURRENT,
    IS_DELETED,
    RECORD_HASH
)
 
SELECT
    S.CUSTOMER_ID,
    S.CUSTOMER_NAME,
    S.CITY,
    S.TIER,
    S.UPDATED_AT,
    NULL,
    TRUE,
    FALSE,
    S.RECORD_HASH
 
FROM STG_CUSTOMER_LATEST S
 
LEFT JOIN DIM_CUSTOMER_SCD2 T
 
    ON S.CUSTOMER_ID = T.CUSTOMER_ID
   AND T.IS_CURRENT = TRUE
 
WHERE T.CUSTOMER_ID IS NULL;
Important: What is missing from this simple implementation?
This is a basic forward-only SCD2 load, not a complete late-arriving CDC solution.
For production, additionally implement:
•	Null-safe attribute comparisons.
•	Multiple source events per customer.
•	Same-timestamp event ordering.
•	Late-arriving event restitching.
•	Source deletes and reactivation.
•	Transactional execution and concurrency protection.
•	Historical fact-key restatement where required.
•	Validation that each customer has exactly one current row.
For a late event that belongs in the middle of an existing timeline, use the timeline reconstruction approach rather than the simple expire-and-insert logic.
19.3 SCD Type 2 late-arriving event reconstruction
Suppose customer 101 has these events:
City	Source effective timestamp
London	1 January
Paris	10 January
New York	20 January
Now a late event arrives:
City	Source effective timestamp
Berlin	15 January
Correct history:
City	Start date	End date	Current
London	1 Jan	10 Jan	No
Paris	10 Jan	15 Jan	No
Berlin	15 Jan	20 Jan	No
New York	20 Jan	Open	Yes
Reconstruction SQL:
sql
WITH EVENT_HISTORY AS (
 
    SELECT
        CUSTOMER_ID,
        CITY,
        SOURCE_COMMIT_TIMESTAMP AS START_DATE,
        EVENT_ID
 
    FROM CUSTOMER_CDC_LEDGER
 
    WHERE OPERATION IN ('INSERT', 'UPDATE')
 
),
 
DEDUPLICATED_EVENTS AS (
 
    SELECT *
 
    FROM EVENT_HISTORY
 
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY CUSTOMER_ID, SOURCE_COMMIT_TIMESTAMP
        ORDER BY EVENT_ID DESC
    ) = 1
 
),
 
TIMELINE AS (
 
    SELECT
        CUSTOMER_ID,
        CITY,
        START_DATE,
 
        LEAD(START_DATE) OVER (
            PARTITION BY CUSTOMER_ID
            ORDER BY START_DATE
        ) AS NEXT_START_DATE
 
    FROM DEDUPLICATED_EVENTS
 
)
 
SELECT
    CUSTOMER_ID,
    CITY,
    START_DATE,
    COALESCE(
        NEXT_START_DATE,
        '9999-12-31'::TIMESTAMP_NTZ
    ) AS END_DATE,
 
    NEXT_START_DATE IS NULL AS IS_CURRENT
 
FROM TIMELINE;
This assumes one valid state transition per timestamp. If multiple changes occur at the same timestamp, use a source sequence number to establish a deterministic order. Also, a delete event must be represented explicitly as a state transition rather than simply removed from the event history.
20. Real-Time Data Engineering – Kafka, Pub/Sub and Streaming
This is a critical area for Senior Data Engineer roles because modern enterprise systems frequently combine batch ingestion with continuous CDC and event processing.
20.1 Batch versus micro-batch versus real-time streaming
Feature	Batch	Micro-batch	Streaming
Processing	Scheduled	Small periodic batches	Continuous/event-driven
Latency	Minutes/hours	Seconds/minutes	Milliseconds/seconds, depending on system
Volume	Large datasets	Continuous increments	Continuous event flow
Example	Daily ERP load	Spark every 30 seconds	Kafka event processing
Tools	Airflow, PySpark	Spark Structured Streaming	Kafka, Pub/Sub, Dataflow
Example enterprise use cases
•	Batch: Daily financial reconciliation.
•	Micro-batch: Orders loaded into Snowflake every five minutes.
•	Streaming: Fraud detection and transaction alerts.
•	CDC: Customer database changes continuously replicated to the analytical platform.
20.2 Kafka architecture
All things Kafka - DEV Community
[dev.to](<https://dev.to/alamfatima1999/all-things-kafka-3cdb?utm_source=chatgpt.com>)
Important Kafka components:
Component	Purpose
Producer	Publishes events
Broker	Stores and serves events
Topic	Logical event category
Partition	Parallelism and ordering unit
Offset	Event position in a partition
Consumer	Reads events
Consumer group	Coordinates parallel consumers
Replication factor	Fault tolerance
Interview question: Why do Kafka partitions matter?
Answer:
Partitions provide parallel processing.
For example, suppose an orders topic contains six partitions and a consumer group has three consumers.
Each consumer can process two partitions.
However, events with the same customer key should generally be routed to the same partition if per-customer ordering is required.
Kafka only guarantees ordering within a partition, not across all partitions.
20.3 Python Kafka producer
Install:
bash
pip install confluent-kafka
Implementation:
python
import json
import logging
 
from confluent_kafka import Producer
 
 
logging.basicConfig(level=logging.INFO)
 
producer_config = {
    "bootstrap.servers": "localhost:9092",
    "client.id": "customer-producer",
    "enable.idempotence": True,
    "acks": "all"
}
 
producer = Producer(producer_config)
 
 
def delivery_report(error, message):
 
    if error:
        logging.error("Delivery failed: %s", error)
 
    else:
        logging.info(
            "Delivered topic=%s partition=%s offset=%s",
            message.topic(),
            message.partition(),
            message.offset()
        )
 
 
def publish_customer_event(event):
 
    producer.produce(
        topic="customer_events",
        key=str(event["customer_id"]),
        value=json.dumps(event),
        callback=delivery_report
    )
 
    producer.poll(0)
 
 
event = {
    "event_id": "EVT001",
    "customer_id": 101,
    "city": "London",
    "operation": "UPDATE"
}
 
publish_customer_event(event)
 
producer.flush(30)
Explanation
•	`enable.idempotence=True` enables producer-side duplicate protection for supported retry scenarios.
•	`acks=all` waits for acknowledgment from the required in-sync replicas.
•	`key` controls partition selection and helps preserve ordering for a given customer.
•	`delivery_report` reports the broker delivery result.
•	`flush()` waits for outstanding messages.
Producer idempotence does not automatically make the entire downstream data pipeline exactly-once.
20.4 Python Kafka consumer
python
import json
import logging
 
from confluent_kafka import Consumer, KafkaException
 
 
consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "customer-consumer-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
}
 
consumer = Consumer(consumer_config)
 
consumer.subscribe(["customer_events"])
 
 
def process_event(event):
 
    logging.info(
        "Processing customer: %s",
        event["customer_id"]
    )
 
    # Business transformation goes here
 
 
try:
 
    while True:
 
        message = consumer.poll(1.0)
 
        if message is None:
            continue
 
        if message.error():
            logging.error("Kafka error: %s", message.error())
            continue
 
        try:
 
            event = json.loads(
                message.value().decode("utf-8")
            )
 
            process_event(event)
 
            # Commit only after successful processing
            consumer.commit(
                message=message,
                asynchronous=False
            )
 
        except Exception:
 
            logging.exception("Event processing failed")
 
finally:
 
    consumer.close()
In production, a failed message should be routed through a controlled retry or dead-letter mechanism. Do not silently skip it and commit its offset.
For stronger processing guarantees, coordinate the destination write and offset checkpoint, or use an idempotent destination with replay-safe writes.
21. Google Cloud Pub/Sub – Real-Time Ingestion
Pub/Sub is Google's managed messaging service.
It is commonly used when applications publish events directly into Google Cloud without operating Kafka infrastructure.
21.1 Pub/Sub architecture
text
APPLICATION / CDC SOURCE
          |
          v
     PUB/SUB TOPIC
          |
          +------------------+
          |                  |
          v                  v
   SUBSCRIPTION 1       SUBSCRIPTION 2
          |                  |
          v                  v
    DATAFLOW JOB        AUDIT SERVICE
          |
          v
     GCS BRONZE
          |
          v
      SNOWFLAKE
original SOP below which we are bettering above chapterwise:
Your existing SOP is already a strong foundation. However, to make it genuinely suitable for a Senior Data Engineer / Lead Data Engineer (L2/L3) working in an enterprise production environment, I would expand it in several important areas.
The biggest architectural improvement is to establish a proper multi-layer Snowflake data processing architecture rather than treating RAW, STAGING, CORE and MARTS as interchangeable concepts.
I would also make AWS S3 and Google Cloud Storage (GCS) interchangeable ingestion-storage options, so the same engineering framework can support AWS-based, GCP-based and hybrid enterprise environments.
Major improvements in the revised SOP
12.	Expanded enterprise architecture
Major architectural improvement- GCS and S3 as alternative raw object-storage platforms.
•	Snowflake multi-layer architecture.
•	Separate ingestion, processing, modeling and consumption responsibilities.
•	Tableau and Power BI integration.
13.	Additional Snowflake engineering
•	Advanced SQL and window functions.
•	Dynamic Tables, Streams and Tasks.
•	Snowflake stored procedures and Snowpark.
•	Advanced incremental loading.
•	Query Profile and cost optimization.
•	Data sharing and governance.
14.	Advanced CDC and data modeling
•	Transaction log ordering.
•	Event deduplication.
•	SCD Type 1, 2 and 6.
•	Late-arriving dimensions.
•	Fact table corrections.
•	Hard deletes and tombstones.
•	Historical timeline reconstruction.
15.	Production engineering
•	Idempotency.
•	Watermark management.
•	Data contracts.
•	Schema evolution.
•	Dead-letter processing.
•	Reconciliation and observability.
•	Disaster recovery.
16.	Advanced development
•	Python ingestion framework.
•	PySpark optimization.
•	Kafka streaming.
•	Airflow orchestration.
•	dbt CI/CD.
•	Terraform and deployment automation.
Below is the expanded master SOP, organized as a technical implementation handbook.
Writing
SENIOR DATA ENGINEER – ENTERPRISE MASTER SOP
Production Implementation, Architecture, Coding, Optimization and Interview Handbook
Technology Stack: Snowflake | dbt Core | Advanced SQL | Python | PySpark | Apache Spark | Apache Kafka | Airflow | AWS S3 | Google Cloud Storage | AWS Glue | GCP Data Services | CDC | Tableau | Power BI | Terraform | GitHub Actions
Target Role: Senior Data Engineer / Lead Data Engineer / Data Platform Engineer
Experience Level: Intermediate to Advanced / L2 / L3
Document Purpose: Enterprise production implementation handbook, architecture reference, coding guide, troubleshooting manual and interview preparation reference.
---
PART 1 – ENTERPRISE DATA ENGINEERING ARCHITECTURE
1.1 Overall Enterprise Architecture
The proposed enterprise architecture supports two object-storage alternatives:
•	AWS S3 for AWS-based environments.
•	Google Cloud Storage (GCS) for GCP-based environments.
Both can serve as durable raw-data storage before data is ingested into Snowflake.
The downstream Snowflake processing architecture remains substantially the same.
Reference architecture
text
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
1.2 Important: Understanding the Snowflake Layers
The proposed layer names are useful as an enterprise naming convention. They should be implemented as clearly defined schemas with separate responsibilities.
Layer	Purpose	Typical processing
RAW	Preserve ingested source data	Minimal modification
LANDING	Standardize ingestion structure	Metadata, file and schema validation
PREPARE	Clean and validate records	Null checks, deduplication, type casting
NORMALIZED	Standardize business entities	Conformed entities, relationships, reusable models
SCHEMATIZED	Apply dimensional and analytical modeling	Facts, dimensions, SCD
MARKETPLACE	Publish business-ready datasets	Aggregates, data marts, governed datasets
SEMANTIC	Business definitions and measures	KPI definitions, calculations, relationships
BI	Reporting and visualization	Tableau, Power BI
Important architectural distinction:
RAW and LANDING are not necessarily separate physical copies of the same data.
A recommended implementation is:
•	RAW: Immutable source payloads and ingestion metadata.
•	LANDING: Validated, typed and consistently structured source records.
•	PREPARE: Cleaned and deduplicated records.
•	NORMALIZED: Conformed business entities.
•	SCHEMATIZED: Facts, dimensions and historical models.
•	MARKETPLACE: Business-facing analytical datasets.
•	SEMANTIC: Business metrics and consumption definitions.
Depending on volume and governance requirements, some layers can be views or incremental tables rather than separate copies.
1.3 Recommended Snowflake Database Structure
text
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
Schema creation
sql
CREATE DATABASE IF NOT EXISTS ENTERPRISE_DW;
 
CREATE SCHEMA IF NOT EXISTS ENTERPRISE_DW.RAW;
CREATE SCHEMA IF NOT EXISTS ENTERPRISE_DW.LANDING;
CREATE SCHEMA IF NOT EXISTS ENTERPRISE_DW.PREPARE;
CREATE SCHEMA IF NOT EXISTS ENTERPRISE_DW.NORMALIZED;
CREATE SCHEMA IF NOT EXISTS ENTERPRISE_DW.SCHEMATIZED;
CREATE SCHEMA IF NOT EXISTS ENTERPRISE_DW.MARKETPLACE;
CREATE SCHEMA IF NOT EXISTS ENTERPRISE_DW.SEMANTIC;
CREATE SCHEMA IF NOT EXISTS ENTERPRISE_DW.AUDIT;
1.4 AWS S3 vs Google Cloud Storage
Both platforms provide durable object storage.
Feature	AWS S3	Google Cloud Storage
Storage	Object storage	Object storage
Storage path	s3://	gs://
Security	IAM and bucket policies	IAM and bucket permissions
Encryption	SSE-S3 / SSE-KMS	Google-managed / CMEK
Notifications	S3 Event Notifications	Pub/Sub notifications
ETL integration	Glue, EMR	Dataflow, Dataproc, Data Fusion
Snowflake integration	S3 storage integration	GCS storage integration
Data format	Parquet, JSON, CSV, Avro	Parquet, JSON, CSV, Avro
The engineering principles remain the same.
Option A: AWS S3 architecture
text
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
Example storage:
text
s3://enterprise-data/
    bronze/
    cdc/
    rejected/
    manifests/
    checkpoints/
Option B: GCS architecture
text
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
Example storage:
text
gs://enterprise-data/
    bronze/
    cdc/
    rejected/
    manifests/
    checkpoints/
Recommendation
Use S3 when the enterprise primarily operates on AWS.
Use GCS when the enterprise primarily operates on Google Cloud.
Use either as the landing/object-storage platform when Snowflake is the central analytical warehouse.
A hybrid architecture is also possible, but cross-cloud networking, security, egress costs and operational complexity should be evaluated before implementing it.
---
PART 2 – GCS AND S3 TO SNOWFLAKE IMPLEMENTATION
2.1 GCS Bucket Setup
Example bucket:
text
gs://enterprise-data-lake/
Recommended folder structure:
text
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
Recommended partitioning:
text
bronze/orders/
    ingestion_date=2026-11-09/
        part-00001.parquet
        part-00002.parquet
Use ingestion date for operational partitioning and retain source event time as a separate column.
Do not partition every dataset by a high-cardinality identifier such as customer ID.
2.2 Configure GCS Permissions
Create a dedicated service identity for Snowflake external storage access.
The service identity should have only the permissions required for the bucket and prefix.
Typical responsibilities include:
•	Listing permitted objects.
•	Reading permitted objects.
•	Accessing the required bucket metadata.
•	Writing only if the chosen workflow requires it.
The exact IAM roles depend on the integration and Snowflake's generated service identity.
Avoid granting project-wide administrator permissions.
2.3 Create Snowflake GCS Storage Integration
sql
CREATE STORAGE INTEGRATION GCS_INTEGRATION
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = GCS
ENABLED = TRUE
STORAGE_ALLOWED_LOCATIONS = (
    'gcs://enterprise-data-lake/bronze/'
);
Inspect the integration:
sql
DESC INTEGRATION GCS_INTEGRATION;
Retrieve the generated Snowflake cloud-service identity information and configure the corresponding GCS bucket permissions according to the Snowflake integration setup procedure.
The cloud identity and bucket authorization must be configured before the stage can access objects.
2.4 Create GCS File Format
sql
CREATE OR REPLACE FILE FORMAT FF_PARQUET
TYPE = PARQUET;
For JSON:
sql
CREATE OR REPLACE FILE FORMAT FF_JSON
TYPE = JSON
STRIP_OUTER_ARRAY = TRUE;
For CSV:
sql
CREATE OR REPLACE FILE FORMAT FF_CSV
TYPE = CSV
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
NULL_IF = ('NULL', 'null', '')
EMPTY_FIELD_AS_NULL = TRUE
ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE;
2.5 Create External Stage for GCS
sql
CREATE OR REPLACE STAGE GCS_ORDERS_STAGE
URL = 'gcs://enterprise-data-lake/bronze/orders/'
STORAGE_INTEGRATION = GCS_INTEGRATION
FILE_FORMAT = FF_PARQUET;
Validate access:
sql
LIST @GCS_ORDERS_STAGE;
2.6 Load GCS Data into Snowflake RAW
Assume the Parquet source contains:
•	ORDER\_ID
•	CUSTOMER\_ID
•	ORDER\_DATE
•	ORDER\_AMOUNT
•	STATUS
Create a raw table:
sql
CREATE TABLE IF NOT EXISTS RAW.RAW_ORDERS (
    ORDER_ID NUMBER,
    CUSTOMER_ID NUMBER,
    ORDER_DATE TIMESTAMP_NTZ,
    ORDER_AMOUNT NUMBER(18,2),
    STATUS VARCHAR,
 
    SOURCE_FILE VARCHAR,
    INGESTED_AT TIMESTAMP_NTZ
);
Load data:
sql
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
The column extraction shown assumes the Parquet fields are exposed with these names and compatible types. Validate the actual file schema before deployment.
Inspect load history:
sql
SELECT *
FROM TABLE(
    INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'RAW.RAW_ORDERS',
        START_TIME => DATEADD('DAY', -1, CURRENT_TIMESTAMP())
    )
);
2.7 Snowpipe with GCS
Snowpipe can continuously load files from supported external cloud storage using cloud event notifications.
Conceptual architecture:
text
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
Example:
sql
CREATE OR REPLACE PIPE GCS_ORDERS_PIPE
AUTO_INGEST = TRUE
INTEGRATION = GCS_NOTIFICATION_INTEGRATION
AS
COPY INTO RAW.RAW_ORDERS
FROM @GCS_ORDERS_STAGE
FILE_FORMAT = (FORMAT_NAME = FF_PARQUET);
The notification integration and cloud event subscription must be configured for the selected Snowflake-supported GCS ingestion architecture.
Use Snowpipe for continuous file ingestion. For direct low-latency event ingestion, evaluate Snowpipe Streaming or a Kafka integration.
2.8 AWS S3 Alternative
Create storage integration:
sql
CREATE STORAGE INTEGRATION S3_INTEGRATION
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_ALLOWED_LOCATIONS = (
    's3://enterprise-data/bronze/'
);
Inspect:
sql
DESC INTEGRATION S3_INTEGRATION;
Configure the AWS IAM trust relationship and bucket access using the Snowflake-generated identity information.
Create stage:
sql
CREATE OR REPLACE STAGE S3_ORDERS_STAGE
URL = 's3://enterprise-data/bronze/orders/'
STORAGE_INTEGRATION = S3_INTEGRATION
FILE_FORMAT = FF_PARQUET;
Load:
sql
COPY INTO RAW.RAW_ORDERS
FROM @S3_ORDERS_STAGE
FILE_FORMAT = (FORMAT_NAME = FF_PARQUET);
Important implementation principle
Do not use AWS access keys or GCP service-account private keys directly in Python scripts, Airflow DAGs or SQL files.
Use supported cloud identities, integrations and managed secrets.
---
PART 3 – SNOWFLAKE MULTI-LAYER TRANSFORMATION IMPLEMENTATION
This section defines the complete transformation journey after ingestion.
3.1 RAW Layer
Purpose:
•	Preserve source information.
•	Retain ingestion metadata.
•	Support replay.
•	Support auditing.
•	Maintain source lineage.
Recommended metadata:
sql
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
Avoid modifying source business attributes in this layer.
3.2 LANDING Layer
Purpose:
•	Convert incoming data to a consistent structure.
•	Validate basic schema compatibility.
•	Attach ingestion metadata.
•	Preserve traceability.
Example:
sql
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
For a large production dataset, use an incremental model or a view instead of rebuilding the entire table.
3.3 PREPARE Layer
Purpose:
•	Deduplicate.
•	Handle nulls.
•	Standardize values.
•	Validate data types.
•	Identify invalid records.
Example:
sql
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
This creates a latest-record representation. Use an event-level deduplication strategy separately when multiple valid CDC events per customer must be retained.
3.4 NORMALIZED Layer
Purpose:
•	Establish consistent business entities.
•	Remove unnecessary duplication.
•	Separate related entities.
•	Standardize business relationships.
•	Create reusable entity models.
Example normalized entities:
text
NORMALIZED.CUSTOMER
NORMALIZED.CUSTOMER_ADDRESS
NORMALIZED.PRODUCT
NORMALIZED.ORDER_HEADER
NORMALIZED.ORDER_DETAIL
NORMALIZED.PAYMENT
Example:
sql
CREATE OR REPLACE TABLE NORMALIZED.CUSTOMER AS
 
SELECT
    CUSTOMER_ID,
    CUSTOMER_NAME,
    EMAIL,
    CITY,
    SOURCE_SYSTEM
 
FROM PREPARE.PREP_CUSTOMER;
A normalized operational representation may use third-normal-form principles.
However, do not force every analytical warehouse model into a highly normalized design. The next layer can use dimensional modeling.
3.5 SCHEMATIZED Layer
Purpose:
•	Create fact tables.
•	Create dimension tables.
•	Apply surrogate keys.
•	Implement SCD.
•	Define analytical grain.
•	Create reusable dimensional structures.
Example dimension:
sql
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
Example fact:
sql
CREATE TABLE IF NOT EXISTS SCHEMATIZED.FACT_ORDERS (
    ORDER_ID NUMBER,
    CUSTOMER_SK NUMBER,
    ORDER_DATE DATE,
    ORDER_AMOUNT NUMBER(18,2),
    ORDER_STATUS VARCHAR,
    LOAD_BATCH_ID VARCHAR
);
Define the fact grain explicitly:
One row per order.
If the source contains one record per order line, use one row per order line instead. Never mix grains without a deliberate design.
3.6 MARKETPLACE Layer
Purpose:
•	Publish business-facing datasets.
•	Aggregate facts.
•	Apply approved business definitions.
•	Create department-specific data products.
•	Support BI and self-service analytics.
Example:
sql
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
3.7 SEMANTIC Layer
Purpose:
•	Define business KPIs.
•	Standardize calculations.
•	Provide consistent consumption views.
•	Avoid different BI teams calculating the same metric differently.
Example:
sql
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
The semantic layer should be the governed location for approved KPI definitions, whether implemented in Snowflake views or a BI semantic model.
3.8 Tableau and Power BI Integration
Recommended connection:
text
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
Recommended practices:
•	Use a dedicated BI role.
•	Grant SELECT only on approved consumption schemas.
•	Avoid exposing raw PII.
•	Use masking policies where required.
•	Configure appropriate warehouse sizing.
•	Prefer governed views for shared business logic.
•	Validate refresh and concurrency requirements.
For Power BI, choose between Import and DirectQuery according to latency, data volume, model complexity and concurrency.
For Tableau, choose between live connections and extracts according to the reporting workload.
---
PART 4 – ADVANCED SNOWFLAKE ENGINEERING
4.1 Advanced SQL Window Functions
Latest record per business key
sql
SELECT *
FROM LANDING.LND_CUSTOMER
 
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CUSTOMER_ID
    ORDER BY
        SOURCE_COMMIT_TS DESC,
        SOURCE_SEQUENCE DESC
) = 1;
Previous and current values
sql
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
Running revenue
sql
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
Rolling 30-day revenue
sql
SELECT
    ORDER_DATE,
 
    SUM(DAILY_REVENUE) OVER (
        ORDER BY ORDER_DATE
        RANGE BETWEEN INTERVAL '29 DAYS' PRECEDING
        AND CURRENT ROW
    ) AS ROLLING_30_DAY_REVENUE
 
FROM MARKETPLACE.DAILY_REVENUE;
For dates with missing days, build a calendar spine if the reporting requirement expects every calendar day.
4.2 Advanced MERGE
A MERGE source must contain at most one applicable row per target key.
sql
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
This example assumes that the source sequence and timestamp are comparable under a documented ordering rule. A production implementation should compare a complete source version, not just timestamps.
4.3 Snowflake Streams and Tasks
sql
CREATE OR REPLACE STREAM RAW_CUSTOMER_STREAM
ON TABLE RAW.RAW_CUSTOMER;
Create task:
sql
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
Enable:
sql
ALTER TASK PROCESS_CUSTOMER_STREAM RESUME;
This is an illustrative upsert task, not a complete CDC implementation. Add explicit delete handling, source-version checks, transaction-safe audit logging and monitoring.
4.4 Dynamic Tables
sql
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
Choose Dynamic Tables when declarative, managed refresh meets the workload's freshness and SQL requirements.
Use dbt incremental models when transformation deployment, version control and model-level orchestration are primary concerns.
4.5 Snowflake Query Optimization
Investigate the following:
17.	Bytes scanned.
18.	Partitions scanned.
19.	Partitions total.
20.	Local spilling.
21.	Remote spilling.
22.	Join cardinality.
23.	Queue time.
24.	Warehouse load.
25.	Compilation time.
Example:
sql
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
Optimize SQL before automatically increasing warehouse size.
---
PART 5 – ADVANCED CDC AND SCD ENGINEERING
5.1 CDC Event Structure
A robust CDC event should contain:
json
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
For Oracle, capture a suitable log position such as an SCN or connector-specific source offset. For PostgreSQL, use the appropriate WAL/LSN metadata. These values are source-specific and should not be compared across unrelated sources.
5.2 Event Deduplication
sql
CREATE OR REPLACE TEMP TABLE CDC_CANONICAL AS
 
SELECT *
 
FROM RAW.RAW_CDC_EVENTS
 
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY SOURCE_SYSTEM, EVENT_ID
    ORDER BY INGESTION_TS DESC
) = 1;
Event-level deduplication must not remove distinct updates to the same customer.
5.3 SCD Type 1
Use when only the latest state matters.
sql
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
Use a proper source ordering representation rather than a simple string comparison if the sequence is not fixed-width or lexically sortable.
5.4 SCD Type 2 – Historical Timeline
Required columns:
text
CUSTOMER_SK
CUSTOMER_ID
CITY
EMAIL
EFFECTIVE_START_TS
EFFECTIVE_END_TS
IS_CURRENT
RECORD_HASH
SOURCE_SEQUENCE
Use half-open validity intervals:
text
effective_start <= business_timestamp < effective_end
For the current record, effective\_end is NULL.
Change detection
sql
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
In production, use unambiguous canonical serialization and explicit null handling to prevent hash collisions caused by delimiters or formatting.
Identify changed records
sql
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
Expire old records
sql
UPDATE SCHEMATIZED.DIM_CUSTOMER T
 
SET
    EFFECTIVE_END_TS = S.SOURCE_COMMIT_TS,
    IS_CURRENT = FALSE
 
FROM PREPARE.PREP_CUSTOMER S
 
WHERE T.CUSTOMER_ID = S.CUSTOMER_ID
  AND T.IS_CURRENT = TRUE
  AND T.RECORD_HASH <> S.RECORD_HASH;
Insert new versions
sql
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
The expire-and-insert operations must be executed as a controlled atomic unit.
This example handles current-state changes. A full historical SCD2 implementation must process every distinct source event in order and reconstruct affected timelines when late events arrive.
5.5 Late-Arriving CDC
Example:
Event	Source time	Arrival
A	10:00	10:01
B	11:00	11:01
C	10:30	12:00
Correct historical sequence:
text
10:00 - Kolkata
10:30 - Delhi
11:00 - Mumbai
Do not discard event C just because event B has already arrived.
Recommended strategy:
26.	Persist the complete immutable CDC event log.
27.	Identify affected business keys.
28.	Retrieve their existing canonical history.
29.	Merge newly received events.
30.	Deduplicate by event identity.
31.	Sort by source ordering metadata.
32.	Remove no-op changes where appropriate.
33.	Recalculate effective start and end timestamps.
34.	Replace affected timelines atomically.
35.	Validate history and fact relationships.
5.6 CDC Hard Deletes
Represent deletion explicitly.
sql
UPDATE SCHEMATIZED.DIM_CUSTOMER
 
SET
    IS_CURRENT = FALSE,
    EFFECTIVE_END_TS = :DELETE_TIMESTAMP
 
WHERE CUSTOMER_ID = :CUSTOMER_ID
  AND IS_CURRENT = TRUE;
Maintain a deletion ledger or tombstone with the source sequence.
An older update must never resurrect a record deleted by a newer event.
5.7 SCD Type 3
Type 3 preserves limited historical values.
Example:
sql
UPDATE DIM_CUSTOMER
 
SET
    PREVIOUS_CITY = CITY,
    CITY = :NEW_CITY
 
WHERE CUSTOMER_ID = :CUSTOMER_ID;
Use when only the previous and current attribute values are required.
5.8 SCD Type 6
SCD Type 6 combines Type 1, Type 2 and Type 3 characteristics.
Typical design:
•	Historical versions retained.
•	Current attributes available across historical rows.
•	Previous attribute values maintained for selected fields.
This is useful for certain reporting requirements but increases complexity. Do not implement it unless the business requires those semantics.
---
PART 6 – DBT ADVANCED ENGINEERING
6.1 Recommended dbt Architecture
text
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
6.2 Advanced Incremental Model
sql
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
A lookback is a recovery optimization, not a complete late-arrival guarantee.
For high-reliability processing, use a persisted source watermark, source offset, replay mechanism and periodic reconciliation.
6.3 dbt Model Contracts
Contracts help define expected output schemas.
yaml
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
Validate supported contract features against the installed dbt version and Snowflake adapter.
6.4 dbt Unit Tests
Example:
yaml
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
Unit tests validate transformation behavior using controlled input data.
They complement, rather than replace, integration and data-quality tests.
6.5 dbt Incremental Strategies
Strategy	Usage
Append	Immutable event data
Merge	Upsert records
Delete + Insert	Replace affected business keys
Microbatch	Supported event-time batch processing
Use incremental strategies based on source change semantics, not simply table size.
6.6 dbt CI/CD
Example:
bash
dbt deps
dbt parse
dbt compile
dbt build --select state:modified+
dbt source freshness
dbt docs generate
Use:
•	Development target.
•	Test target.
•	Production target.
•	State-based selection.
•	Environment-specific variables.
•	Protected deployment approvals.
•	Version-controlled packages.
---
PART 7 – PYTHON ADVANCED DATA ENGINEERING
7.1 Production Ingestion Framework
Recommended components:
text
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
Responsibilities:
•	Connector: API, database or file connectivity.
•	Extractor: Incremental data extraction.
•	Validator: Schema and business validation.
•	Transformer: Standardization.
•	Loader: S3, GCS or Snowflake writing.
•	Audit: Counts, watermarks and execution logs.
•	Retry: Transient failure handling.
7.2 Database Extraction Using Incremental Watermark
python
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
For a production CDC pipeline, prefer transaction-log positions or a source-supported consistent snapshot boundary when available.
Timestamp extraction alone can miss changes if timestamps are not reliable or multiple records share the same timestamp.
7.3 Python Data Validation
python
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
Separate rejected records from valid records. Never silently discard invalid business data.
7.4 Python Structured Logging
python
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
Add execution IDs, source identifiers, batch IDs and error classifications to production logs.
7.5 Python Idempotency
Use a deterministic batch identity.
python
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
Store the batch ID in audit tables and target metadata.
A repeated batch should either:
•	Recognize that it already completed.
•	Safely reprocess the same input.
•	Replace the previous batch output atomically.
---
PART 8 – PYSPARK ADVANCED ENGINEERING
8.1 Spark Execution Concepts
Master:
•	Driver.
•	Executors.
•	Jobs.
•	Stages.
•	Tasks.
•	Partitions.
•	DAG.
•	Catalyst.
•	Tungsten.
•	AQE.
•	Shuffle.
•	Broadcast.
•	Persistence.
•	Checkpointing.
8.2 Advanced PySpark Data Processing
python
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
For historical CDC, do not use latest-record deduplication as the entire transformation. Retain all valid ordered changes.
The same processing code can use an S3 path:
python
s3_path = (
    "s3://enterprise-data/bronze/customers/"
)
8.3 Advanced Spark Join Optimization
python
from pyspark.sql.functions import broadcast
 
result = fact_orders.join(
    broadcast(dim_customer),
    on="customer_id",
    how="left"
)
Broadcast only when the build-side dataset is small enough for the executor memory budget.
8.4 Explain Query Plan
python
result.explain(True)
Inspect:
•	Scan operations.
•	Exchange operations.
•	Sort operations.
•	Join strategy.
•	Partition pruning.
•	Whole-stage code generation.
8.5 Handling Data Skew
python
from pyspark.sql.functions import (
    col,
    floor,
    rand
)
 
salted_fact = fact_orders.withColumn(
    "salt",
    floor(rand() * 10)
)
For a corresponding salted join, replicate the relevant dimension rows across salt values and join using both the business key and salt.
Use this only when actual skew is identified in the Spark UI.
8.6 Spark File Optimization
Avoid producing thousands of tiny Parquet files.
python
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
Choose partition count and file sizes based on data volume and downstream access patterns.
---
PART 9 – KAFKA AND REAL-TIME STREAMING
9.1 Kafka Architecture
text
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
9.2 Kafka Event Identity
Every event should contain:
•	event\_id
•	business\_key
•	operation
•	source\_timestamp
•	source\_sequence
•	transaction\_id
•	schema\_version
Use a business key as the Kafka message key when per-entity ordering is important.
9.3 PySpark Structured Streaming
python
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
For S3, replace the paths with the corresponding S3 locations.
For production, configure schema validation, rejected-event output, checkpoint lifecycle, durable replay and a supported transactional or idempotent sink strategy.
---
PART 10 – AIRFLOW ADVANCED ORCHESTRATION
10.1 Enterprise Pipeline DAG
text
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
10.2 Airflow TaskFlow Example
python
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
This example demonstrates dependency flow. In production, tasks should call actual operators or service clients, pass only small metadata through XCom and obtain credentials from Airflow connections or a secrets backend.
10.3 Advanced Airflow Concepts
Master:
•	Dynamic task mapping.
•	TaskFlow API.
•	Task groups.
•	Deferrable operators.
•	Sensors.
•	Pools.
•	Priority weights.
•	Dataset scheduling.
•	Backfills.
•	Catchup.
•	Logical dates.
•	Data intervals.
•	SLA and deadline monitoring.
•	Kubernetes executors.
•	Secrets backends.
Never use Airflow XCom to transfer large dataframes or entire source datasets.
---
PART 11 – DATA QUALITY, DATA CONTRACTS AND OBSERVABILITY
11.1 Data Quality Framework
Every pipeline should validate:
Dimension	Validation
Completeness	Required columns are populated
Uniqueness	Business keys are not duplicated
Validity	Data conforms to expected rules
Accuracy	Business values are correct
Consistency	Related datasets agree
Timeliness	Data arrives within SLA
Referential integrity	Fact references resolve
11.2 Data Contract Example
yaml
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
Data contracts should define ownership, schema compatibility, business semantics, freshness expectations and change-notification responsibilities.
11.3 Reconciliation Framework
sql
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
In production, reconcile by batch, source interval, business date or another consistent grain. Ensure the source and target apply identical filters and business rules.
11.4 Observability Metrics
Capture:
•	Total source records.
•	Total target records.
•	Rejected records.
•	Duplicate events.
•	CDC lag.
•	Pipeline duration.
•	Query execution duration.
•	Warehouse queue duration.
•	Spark shuffle spill.
•	Kafka consumer lag.
•	API retry count.
•	Data freshness.
•	SLA success rate.
Build operational dashboards for engineering teams separately from business dashboards.
---
PART 12 – ADVANCED DATA MODELING
12.1 Dimensional Modeling
Master:
•	Star schema.
•	Snowflake schema.
•	Fact tables.
•	Dimension tables.
•	Conformed dimensions.
•	Role-playing dimensions.
•	Degenerate dimensions.
•	Junk dimensions.
•	Bridge tables.
•	Accumulating snapshots.
•	Periodic snapshots.
•	Transaction facts.
12.2 Fact Table Types
Type	Example
Transaction	One row per sales transaction
Periodic snapshot	One row per account per day
Accumulating snapshot	One row per loan application lifecycle
Factless fact	Customer attendance or eligibility event
12.3 Late-Arriving Dimensions
A fact can arrive before its corresponding customer dimension.
Solution:
36.	Create an inferred dimension record.
37.	Use a designated unknown or inferred surrogate key.
38.	Update the inferred dimension when the real attributes arrive.
39.	Preserve historical validity.
40.	Reconcile unresolved references.
Example:
sql
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
Inferred-member creation must be idempotent and protected against concurrent duplicate inserts.
12.4 Fact Correction and Restatement
When a historical order changes:
•	Identify affected business facts.
•	Determine whether the change is an update or reversal.
•	Recalculate financial measures.
•	Re-resolve dimensional surrogate keys if necessary.
•	Reprocess affected aggregate partitions.
•	Reconcile the corrected period.
Never assume that updating a dimension automatically fixes historical fact relationships.
---
PART 13 – PERFORMANCE AND COST OPTIMIZATION
13.1 Snowflake
•	Use independent warehouses for independent workloads.
•	Optimize scanned data.
•	Avoid SELECT \* in production transformations.
•	Monitor spilling.
•	Use clustering selectively.
•	Evaluate search optimization for suitable point lookups.
•	Monitor warehouse credits.
•	Set resource monitors.
•	Review materialization decisions.
•	Avoid unnecessary full refreshes.
13.2 dbt
•	Incremental models for large changing datasets.
•	Selective execution.
•	Avoid excessive nested ephemeral models.
•	Reuse intermediate transformations.
•	Apply tests at meaningful business grains.
•	Optimize source filters.
•	Inspect compiled SQL.
•	Use Slim CI where appropriate.
13.3 PySpark
•	Filter early.
•	Project required columns.
•	Use broadcast joins selectively.
•	Enable AQE where appropriate.
•	Avoid unnecessary shuffles.
•	Control partition counts.
•	Avoid driver collection.
•	Avoid unnecessary Python UDFs.
•	Compact small files.
•	Monitor executor memory and skew.
13.4 Object Storage
•	Use Parquet for analytical datasets.
•	Apply appropriate compression.
•	Partition on query-relevant fields.
•	Avoid excessive small files.
•	Use immutable source object keys.
•	Configure lifecycle policies.
•	Maintain manifests and ingestion metadata.
---
PART 14 – SECURITY AND GOVERNANCE
14.1 Snowflake RBAC
Create separate roles for:
•	Ingestion.
•	Transformation.
•	Data engineering.
•	Analytics.
•	BI consumption.
•	Security administration.
Example:
sql
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
Also configure future grants, appropriate warehouse usage and schema-level privileges.
14.2 PII Management
Classify:
•	Public data.
•	Internal data.
•	Confidential data.
•	Restricted PII.
Implement:
•	Dynamic data masking.
•	Row access policies.
•	Secure views.
•	Encryption.
•	Audit trails.
•	Retention controls.
•	Least privilege.
14.3 Cloud Security
For AWS:
•	IAM roles.
•	S3 bucket policies.
•	KMS.
•	CloudTrail.
•	Secrets Manager.
•	VPC endpoints where appropriate.
For GCP:
•	Service accounts.
•	IAM.
•	Cloud KMS.
•	Cloud Audit Logs.
•	Secret Manager.
•	Private connectivity where appropriate.
---
PART 15 – CI/CD AND INFRASTRUCTURE AS CODE
15.1 Git Workflow
text
MAIN
 |
 +--- FEATURE
 |
 +--- DEVELOPMENT
 |
 +--- TESTING
 |
 +--- PRODUCTION
Recommended promotion:
41.	Pull request.
42.	Python unit tests.
43.	SQL linting.
44.	dbt parsing.
45.	dbt compilation.
46.	Integration testing.
47.	Development deployment.
48.	Approval.
49.	Production deployment.
50.	Post-deployment validation.
15.2 Terraform
Use Terraform to manage:
•	S3 buckets.
•	GCS buckets.
•	IAM.
•	Cloud identities.
•	Glue jobs.
•	Snowflake warehouses.
•	Snowflake integrations.
•	Database and schema permissions.
•	Airflow infrastructure.
Keep infrastructure state protected and separate development and production workspaces or state configurations.
15.3 GitHub Actions
yaml
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
Use protected deployment environments and managed secrets. Pin dependencies and action versions according to organizational security policy.
---
PART 16 – PRODUCTION INCIDENT MANAGEMENT
Scenario 1: GCS Files Are Not Loading into Snowflake
Investigate:
51.	Bucket path.
52.	Snowflake stage configuration.
53.	Storage integration.
54.	GCS IAM permissions.
55.	File format.
56.	File compatibility.
57.	COPY INTO history.
58.	Cloud notification configuration if Snowpipe is used.
Scenario 2: S3 and GCS Both Contain Different Record Counts
Investigate:
•	Source extraction consistency.
•	File manifests.
•	Partition completeness.
•	Duplicate files.
•	Object arrival timing.
•	Time-zone differences.
•	Schema evolution.
Use a source watermark and immutable batch ID to compare equivalent extraction intervals.
Scenario 3: Snowflake RAW Is Correct but PREPARE Is Missing Records
Investigate:
•	Filtering logic.
•	Deduplication ordering.
•	Type conversion.
•	Null handling.
•	MERGE conditions.
•	Late-arriving events.
•	Invalid-record routing.
Scenario 4: SCD2 Has Multiple Current Records
Investigate:
•	Concurrent runs.
•	Duplicate source keys.
•	Same-timestamp events.
•	Transaction ordering.
•	Non-atomic expire-and-insert processing.
Reconstruct affected histories and add uniqueness and interval validation.
Scenario 5: Tableau and Power BI Show Different Revenue
Investigate:
•	KPI definitions.
•	Time-zone conversion.
•	Refund handling.
•	Duplicate fact records.
•	Semantic-model filters.
•	Data refresh timing.
•	Different aggregation grains.
Create one governed metric definition and reconcile both tools against it.
Scenario 6: Kafka Consumer Lag Increases
Investigate:
•	Partition count.
•	Consumer parallelism.
•	Processing throughput.
•	Downstream write latency.
•	Retry loops.
•	Poison messages.
•	Broker health.
Scale consumers only when there are available partitions and the downstream system can support the increased concurrency.
Scenario 7: Airflow Retries Create Duplicate Data
Investigate:
•	Batch identity.
•	Task commit point.
•	Watermark updates.
•	Partial writes.
•	MERGE behavior.
•	External API side effects.
Make the task replay-safe and ensure the watermark advances only after successful durable processing.
---
PART 17 – ADDITIONAL ADVANCED CONCEPTS
A Senior Data Engineer should develop working knowledge of the following.
Cloud Data Engineering
•	AWS Glue.
•	AWS DMS.
•	AWS EMR.
•	Google Cloud Dataflow.
•	Google Cloud Dataproc.
•	BigQuery architecture.
•	Pub/Sub.
•	Cloud Composer.
•	Cloud Storage lifecycle management.
•	Cross-cloud connectivity.
Snowflake
•	Snowpark Python.
•	Stored procedures.
•	JavaScript procedures.
•	Dynamic Tables.
•	Snowpipe Streaming.
•	Hybrid Tables.
•	Iceberg Tables.
•	Secure Data Sharing.
•	Replication and Failover.
•	Resource Monitors.
•	Masking and Row Access Policies.
Streaming
•	Debezium.
•	Kafka Connect.
•	Schema Registry.
•	Avro.
•	Protobuf.
•	Event sourcing.
•	Exactly-once boundaries.
•	Stateful processing.
•	Backpressure.
•	Watermarks.
•	Replay and recovery.
Data Architecture
•	Medallion architecture.
•	Data mesh.
•	Data fabric.
•	Lakehouse.
•	Open table formats.
•	Data contracts.
•	Domain-oriented data products.
•	Master data management.
•	Data lineage.
•	Data governance.
Engineering Practices
•	Unit testing.
•	Integration testing.
•	Contract testing.
•	CI/CD.
•	Infrastructure as Code.
•	Observability.
•	Incident response.
•	Capacity planning.
•	Cost governance.
•	Disaster recovery.
---
PART 18 – SENIOR DATA ENGINEER INTERVIEW PREPARATION
Architecture Questions
59.	Design a complete enterprise data platform using Snowflake.
60.	Explain GCS versus S3 for data lake architecture.
61.	How would you implement multi-source ingestion?
62.	How do you design a replayable CDC architecture?
63.	Explain the responsibilities of RAW, LANDING, PREPARE, NORMALIZED and MARKETPLACE.
64.	How would you support both batch and streaming?
65.	How would you design a data platform for multiple business domains?
66.	How would you implement disaster recovery?
Snowflake Questions
67.	Explain micro-partition pruning.
68.	Explain warehouse scaling.
69.	How do you investigate spilling?
70.	Explain Streams versus Dynamic Tables.
71.	How do you implement Snowpipe?
72.	How do you optimize large MERGE statements?
73.	How do you implement row-level security?
74.	How do you handle schema evolution?
75.	Explain Time Travel and cloning.
76.	How do you manage query concurrency?
dbt Questions
77.	Explain incremental models.
78.	Explain snapshots.
79.	How do you handle late-arriving records?
80.	How do you build an SCD2 model?
81.	Explain model contracts.
82.	Explain state-based selection.
83.	How do you manage schema changes?
84.	How do you implement Slim CI?
85.	How do you test incremental transformations?
86.	How do you manage production full refreshes?
CDC Questions
87.	How do you deduplicate CDC events?
88.	How do you handle out-of-order events?
89.	How do you handle hard deletes?
90.	How do you reconstruct an SCD2 timeline?
91.	What is the difference between source commit time and ingestion time?
92.	How do you prevent stale updates?
93.	What happens when multiple events have the same timestamp?
94.	How do you replay historical events?
PySpark Questions
95.	Explain Catalyst.
96.	Explain AQE.
97.	What is a shuffle?
98.	How do you resolve data skew?
99.	When should broadcast joins be used?
100.	Explain repartition versus coalesce.
101.	What is checkpointing?
102.	How do you troubleshoot executor failures?
103.	How do you optimize file output?
104.	Explain Structured Streaming recovery.
Production Scenario Questions
105.	Your pipeline fails after loading RAW but before PREPARE. How do you recover?
106.	A source system sends duplicate updates. How do you handle them?
107.	A customer changes address three times in one day. How do you preserve history?
108.	A delete arrives before an older update. How do you prevent resurrection?
109.	Snowflake costs increase by 40%. How do you investigate?
110.	A dbt incremental model misses a record. How do you recover?
111.	A Power BI report shows different results from Tableau. How do you troubleshoot?
112.	Your Spark job has one task running much longer than the others. What do you investigate?
113.	GCS ingestion suddenly stops. What do you inspect?
114.	An Airflow retry processes an already completed batch. How do you prevent duplicate writes?
---
PART 19 – COMPLETE HANDS-ON CAPSTONE PROJECT
Project: Enterprise Customer and Sales Data Platform
Business requirement
An enterprise receives data from:
•	Oracle customer database.
•	PostgreSQL order database.
•	REST APIs.
•	Kafka order events.
•	Daily CSV and Parquet files.
The platform must support:
•	Batch ingestion.
•	CDC ingestion.
•	Historical customer tracking.
•	Sales reporting.
•	Customer analytics.
•	Tableau and Power BI consumption.
Proposed implementation
text
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
Project implementation sequence
Phase	Implementation
1	Source database setup
2	Batch extraction using Python
3	CDC extraction using DMS or Debezium
4	Kafka event processing
5	GCS or S3 object storage
6	Snowflake storage integration
7	COPY INTO / Snowpipe
8	RAW and LANDING development
9	PREPARE transformations
10	NORMALIZED entity models
11	SCD Type 1 and Type 2
12	FACT and DIMENSION models
13	dbt incremental processing
14	MARKETPLACE development
15	SEMANTIC KPI development
16	Airflow orchestration
17	Data quality and reconciliation
18	Tableau and Power BI integration
19	CI/CD deployment
20	Production monitoring and recovery
Expected project deliverables
115.	Complete architecture diagram.
116.	Cloud storage configuration.
117.	Python ingestion framework.
118.	CDC event-processing framework.
119.	Snowflake database and schema design.
120.	dbt transformation project.
121.	SCD Type 2 implementation.
122.	PySpark processing jobs.
123.	Airflow DAGs.
124.	Data-quality framework.
125.	Audit and watermark tables.
126.	BI marketplace datasets.
127.	Tableau or Power BI dashboard.
128.	CI/CD workflow.
129.	Production troubleshooting documentation.
---
PART 20 – FINAL SENIOR DATA ENGINEER MASTERY CHECKLIST
Architecture
•	[ ]Understand batch, micro-batch and streaming.
•	[ ]Design S3 and GCS storage architectures.
•	[ ]Design Snowflake multi-layer processing.
•	[ ]Understand lakehouse and data warehouse concepts.
•	[ ]Define ingestion and recovery boundaries.
Snowflake
•	[ ]Advanced SQL.
•	[ ]Query Profile.
•	[ ]Micro-partition pruning.
•	[ ]Clustering.
•	[ ]Streams and Tasks.
•	[ ]Dynamic Tables.
•	[ ]Snowpipe.
•	[ ]Snowpipe Streaming.
•	[ ]Snowpark.
•	[ ]RBAC and governance.
dbt
•	[ ]Sources and refs.
•	[ ]Incremental models.
•	[ ]Snapshots.
•	[ ]Macros.
•	[ ]Model contracts.
•	[ ]Unit tests.
•	[ ]State-based selection.
•	[ ]CI/CD.
CDC and Data Modeling
•	[ ]Event deduplication.
•	[ ]Source sequence ordering.
•	[ ]Late-arriving events.
•	[ ]Hard deletes.
•	[ ]SCD Type 1.
•	[ ]SCD Type 2.
•	[ ]SCD Type 3 and 6.
•	[ ]Late-arriving dimensions.
•	[ ]Fact restatement.
•	[ ]Timeline reconstruction.
Python and PySpark
•	[ ]Production ingestion framework.
•	[ ]API pagination.
•	[ ]Retry and timeout.
•	[ ]Structured logging.
•	[ ]Schema validation.
•	[ ]Spark joins.
•	[ ]AQE.
•	[ ]Skew handling.
•	[ ]Shuffle optimization.
•	[ ]Structured Streaming.
Airflow and Kafka
•	[ ]DAG design.
•	[ ]TaskFlow.
•	[ ]Dynamic mapping.
•	[ ]Backfills.
•	[ ]Idempotency.
•	[ ]Kafka partitions.
•	[ ]Consumer groups.
•	[ ]Offsets.
•	[ ]Watermarks.
•	[ ]Replay and recovery.
Production Engineering
•	[ ]Audit logging.
•	[ ]Data quality.
•	[ ]Reconciliation.
•	[ ]Monitoring.
•	[ ]Alerting.
•	[ ]Data contracts.
•	[ ]Security.
•	[ ]CI/CD.
•	[ ]Terraform.
•	[ ]Disaster recovery.
---
FINAL ENGINEERING PRINCIPLE
A Senior Data Engineer must be able to design, develop, optimize, deploy and operate an entire data platform.
The objective is not simply to move data from Oracle to Snowflake or to write a complex PySpark transformation.
The objective is to build a platform that is:
130.	Correct – Business data is accurate.
131.	Reliable – Failures can be recovered.
132.	Idempotent – Reprocessing does not corrupt data.
133.	Scalable – Processing supports increasing volumes.
134.	Performant – Queries and pipelines meet SLAs.
135.	Cost-efficient – Compute and storage are optimized.
136.	Secure – Sensitive data is appropriately protected.
137.	Observable – Every important operation is measurable.
138.	Governed – Data ownership and definitions are clear.
139.	Maintainable – Other engineers can understand and operate it.
The recommended learning and implementation project is:
Oracle / PostgreSQL CDC → Kafka → AWS S3 or GCS → Snowflake RAW → LANDING → PREPARE → NORMALIZED → SCHEMATIZED → MARKETPLACE → SEMANTIC → Tableau / Power BI.
Build the platform end to end, implement historical CDC correctly, automate it through Airflow and dbt, and demonstrate recovery from real production failure scenarios.
That is the level of practical engineering capability expected from a strong Senior Data Engineer.
My additional recommendation: how to master this SOP
Don't try to learn all the technologies simultaneously. Work through the following progression.
Stage	Focus	Practical outcome
1	Advanced SQL + Snowflake	Build all Snowflake layers
2	Python + APIs	Build reusable ingestion framework
3	S3 + GCS	Implement both storage alternatives
4	CDC + Kafka	Build event-driven ingestion
5	dbt	Build incremental models and SCD2
6	PySpark	Build distributed transformations
7	Airflow	Orchestrate end-to-end pipelines
8	Data modeling	Build facts, dimensions and marts
9	BI	Publish Tableau and Power BI datasets
10	Production engineering	Monitoring, recovery, CI/CD and optimization
One particularly important distinction: Your RAW → LANDING → PREPARE → NORMALIZED → SCHEMATIZED → MARKETPLACE design is a valid enterprise convention, but it is not a mandatory Snowflake architecture. The value comes from defining clear contracts and responsibilities for every layer, not from creating more physical copies of data.
I recommend implementing the same project twice at the storage-ingestion boundary: once using S3 and once using GCS. Keep the Snowflake transformation and dbt layers identical. This demonstrates genuine cloud-independent data engineering architecture.
 
PART B — COMPLETION EDITION AND ADDITIONAL CHAPTERS
SENIOR DATA ENGINEER MASTER SOP — COMPLETION EDITION
Editorial note and operating assumptions
This completion edition retains the supplied handbook as its foundation and adds the missing production-operating material, implementation patterns, and advanced interview answers. The supplied text contains overlapping drafts and repeated chapters. Treat the later enterprise SOP (Parts 1–20) as the consolidated baseline; earlier chapters are useful background, not a second set of requirements.
Primary cloud path: Google Cloud Storage (GCS), Pub/Sub, Dataflow/Dataproc, Cloud Composer (managed Airflow), and Snowflake. AWS S3, DMS, Glue and EMR are optional equivalents for cross-cloud projects—not simultaneous mandatory dependencies.
Version discipline: SQL, provider integrations, dbt adapter behavior, Airflow operators and cloud IAM evolve. Pin versions in a real project, validate examples against those versions, and test in a non-production account. Code below is implementation-oriented reference code; replace identifiers, secrets, project IDs and policies with environment-specific values.
Consolidated chapter map
Area	Existing coverage	Completion focus
Architecture, GCS, Snowflake, dbt, CDC/SCD	Parts 1–6	Contracts, boundaries, failure semantics
Python, PySpark, Kafka, Airflow	Parts 7–10	Deployable patterns, recovery and testing
Quality, modeling, cost, security, CI/CD	Parts 11–15	SLOs, policy-as-code, release gates
Incidents, interviews, capstone	Parts 16–20	Runbooks, answer frameworks and executable scenarios
Additional	New Parts 21–30	GCP operations, governance, DR, advanced code and interview bank
---
PART 21 — GCP DATA PLATFORM OPERATIONS
21.1 Choose the right GCP processing service
Service	Best fit	Senior-level trade-off
Cloud Storage	Durable object storage and landing zone	Object storage is not a transactional table format
Pub/Sub	Managed event transport	Delivery is at-least-once; consumers must tolerate duplicates
Dataflow / Apache Beam	Managed batch and streaming pipelines	Runner-managed scaling; Beam windowing and watermark semantics must be understood
Dataproc / Spark	Existing Spark workloads, custom libraries, large batch transformations	Cluster lifecycle and Spark tuning are operational responsibilities
Cloud Composer	Airflow orchestration	Orchestrates work; it is not the streaming engine
BigQuery	Serverless analytical warehouse and GCP-native analytics	May complement Snowflake; avoid duplicating data without a clear product need
Secret Manager	Secret storage and rotation	Access should be granted to workload identities, not human-shared credentials
Cloud Monitoring / Logging	Metrics, logs, alerts	Instrument business-level freshness and completeness, not only infrastructure health
Decision rule: use Dataflow when Beam's event-time, windowing, autoscaling and managed streaming model fit; use Dataproc when Spark compatibility, custom Spark code or an existing Spark estate is decisive. Benchmark representative workloads before choosing based on theoretical throughput.
21.2 GCS object naming and immutable batch layout
Use immutable, batch-scoped object names. Do not let concurrent retries overwrite the same object.
gs://company-lake/
  bronze/
    source=erp/
      entity=orders/
        ingest_date=2026-11-09/
          batch_id=orders_20261109T010000Z_8f3a/
            part-00000-<uuid>.jsonl
            part-00001-<uuid>.jsonl
            manifest.json
  silver/
    entity=orders/
      event_date=2026-11-09/
        run_id=<orchestrator-run-id>/
          part-*.parquet
          _SUCCESS
Recommended manifest fields:
{
  "batch_id": "orders_20261109T010000Z_8f3a",
  "source": "erp",
  "entity": "orders",
  "schema_version": 3,
  "created_at": "2026-11-09T01:00:00Z",
  "record_count": 500000,
  "files": [
    {"uri": "gs://company-lake/bronze/.../part-00000.jsonl", "generation": "12345"}
  ],
  "status": "COMMITTED",
  "checksum_algorithm": "sha256"
}
The manifest is the batch commit marker. Consumers must ignore prefixes without a valid committed manifest. Record GCS object generation/metageneration where useful so that a manifest identifies the exact object version, not merely a mutable name.
21.3 Safe GCS upload with generation precondition
import json
from google.cloud import storage
 
def upload_immutable_json(bucket_name: str, object_name: str, payload: dict) -> str:
    client = storage.Client()
    blob = client.bucket(bucket_name).blob(object_name)
    body = json.dumps(payload, separators=(",", ":"), default=str)
 
    # if_generation_match=0 means create only if the object does not exist.
    blob.upload_from_string(
        body,
        content_type="application/json",
        if_generation_match=0,
        checksum="auto",
    )
    blob.reload()
    return str(blob.generation)
If the precondition fails because the object already exists, compare its batch identity/checksum. Treat an identical prior object as an idempotent retry; treat a different payload at the same immutable key as a conflict requiring investigation.
21.4 GCP IAM operating model
•	Use one service account per workload or trust boundary (ingestion, transformation, deployment).
•	Grant bucket-level roles only on the required buckets/prefix strategy; avoid project-wide Storage Admin for pipeline identities.
•	Prefer Workload Identity Federation or attached service-account identity over downloaded long-lived service-account keys.
•	Separate deployment identity from runtime identity.
•	Enable audit logging and review privileged role bindings.
•	Use CMEK only where key ownership, separation of duties or regulatory requirements justify the operational overhead.
•	Restrict Pub/Sub publisher/subscriber roles to the relevant topic/subscription.
•	Test denied access as part of security validation; a policy that only succeeds with Owner permissions is not production-ready.
21.5 Interview questions — GCP
Q: Why is GCS not a database transaction boundary?  
A: Object writes are atomic at the individual-object level, but a directory/prefix containing many files is not a multi-object transaction. I publish to a run-specific prefix, validate all files, and write a manifest/commit marker last. Downstream consumers read only committed manifests.
Q: Pub/Sub redelivers a message. What prevents duplicate facts?  
A: The consumer uses a stable event ID and an idempotent sink operation. I persist the event ID or use a unique business/event key in a deduplication stage, and I acknowledge only after the durable sink operation succeeds. I do not claim end-to-end exactly-once merely because the broker offers delivery controls.
---
PART 22 — RELIABLE INGESTION, WATERMARKS AND REPLAY
22.1 The ingestion state machine
Every batch or CDC micro-batch should have a durable state record:
DISCOVERED → EXTRACTED → LANDED → VALIDATED → LOADED → TRANSFORMED → COMMITTED
A failed run remains in a failed/intermediate state with an error reason. A retry resumes from a known durable boundary or safely replays the batch. Never advance a source watermark merely because extraction started.
A useful control table:
CREATE TABLE IF NOT EXISTS OPS.INGESTION_BATCH (
    PIPELINE_NAME       VARCHAR NOT NULL,
    BATCH_ID            VARCHAR NOT NULL,
    SOURCE_LOW_WATERMARK VARCHAR,
    SOURCE_HIGH_WATERMARK VARCHAR,
    EXPECTED_COUNT      NUMBER,
    LANDED_COUNT        NUMBER,
    LOADED_COUNT        NUMBER,
    STATUS              VARCHAR NOT NULL,
    STARTED_AT          TIMESTAMP_NTZ,
    COMMITTED_AT        TIMESTAMP_NTZ,
    ERROR_MESSAGE       VARCHAR,
    PRIMARY KEY (PIPELINE_NAME, BATCH_ID)
);
Snowflake standard-table primary-key constraints are generally informational rather than enforced. Enforce uniqueness through deterministic batch IDs, controlled writers, merge logic and explicit duplicate checks.
22.2 Watermark strategy
For a source with a monotonically increasing change sequence, use a half-open interval:
(last_committed_sequence, current_high_sequence]
Persist both the low and high watermark for audit and replay. If only timestamps are available, use an overlap window and deduplicate by a stable source key plus source version/sequence.
Avoid a naïve MAX(updated_at) watermark when timestamps can be late, repeated, null, backdated or have insufficient precision.
SQL pattern: overlap extraction
-- Parameters are supplied by the orchestrator from the control table.
SELECT *
FROM SOURCE_DB.PUBLIC.ORDERS
WHERE UPDATED_AT >= :last_committed_ts - INTERVAL '2' HOUR
  AND UPDATED_AT <  :captured_high_watermark;
The overlap creates duplicates by design; downstream deduplication must be deterministic. Advance the committed watermark only after landing, validation and target commit have succeeded.
22.3 Atomic target commit pattern
A robust batch flow is:
140.	Allocate a deterministic `batch_id`.
141.	Capture source high watermark.
142.	Extract the bounded interval.
143.	Write immutable GCS objects to a batch prefix.
144.	Validate schema, count and checksum; publish manifest.
145.	Load into a Snowflake batch-staging table with `batch_id`.
146.	Deduplicate and merge into target in a transaction.
147.	Mark the batch committed and advance the watermark in the same control-plane commit where feasible.
148.	Emit metrics and lineage.
Where the source and destination cannot participate in one distributed transaction, design for at-least-once delivery plus idempotent sink writes. Document the exact commit point.
22.4 Deduplication using event identity
CREATE OR REPLACE TEMP TABLE STG_ORDER_EVENTS_DEDUP AS
SELECT *
FROM RAW.ORDER_EVENTS
WHERE BATCH_ID = :batch_id
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY SOURCE_SYSTEM, EVENT_ID
    ORDER BY SOURCE_LOG_SEQUENCE DESC, INGESTED_AT DESC
) = 1;
Do not use arrival timestamp as the only tie-breaker if the source exposes a transaction/log sequence. Define what to do when two rows have the same event ID but conflicting payloads: quarantine and alert rather than arbitrarily selecting one.
22.5 Interview scenario: extraction succeeded, target merge failed
Answer: The landed batch remains immutable and its watermark is not committed. I rerun the load/merge for the same batch ID. The target merge is idempotent, and the batch control table prevents a second logical commit. I reconcile source count, landed count, rejected count and target insert/update/delete counts before marking success. I do not re-extract a moving source interval unless the original high watermark and source snapshot semantics are preserved.
---
PART 23 — PYSPARK ADVANCED OPERATING PATTERNS
23.1 Understand the execution plan
A Spark DataFrame transformation builds a logical plan. Catalyst optimizes it; the physical planner selects operators; execution is split into stages at shuffle boundaries. The Spark UI is the primary diagnostic tool for task skew, shuffle read/write, spill, executor loss and long-tail tasks.
Senior engineers should know:
•	Narrow transformations usually avoid a shuffle; wide transformations commonly require one.
•	`repartition()` redistributes data and causes a shuffle.
•	`coalesce()` can reduce partitions with less data movement, but may create uneven partitions.
•	Broadcast joins are appropriate only when the build-side relation is safely small for executor memory.
•	AQE can coalesce shuffle partitions, change join strategies and mitigate skew, subject to Spark version/configuration.
•	`cache()` is not automatically an optimization; cache only when reuse justifies memory pressure.
23.2 Production batch transformation
from pyspark.sql import functions as F
from pyspark.sql.window import Window
 
def transform_orders(raw):
    required = ["order_id", "customer_id", "amount", "source_sequence", "event_ts"]
    missing = sorted(set(required) - set(raw.columns))
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
 
    typed = (
        raw.select(
            F.col("order_id").cast("string"),
            F.col("customer_id").cast("string"),
            F.col("amount").cast("decimal(18,2)"),
            F.col("source_sequence").cast("long"),
            F.to_timestamp("event_ts").alias("event_ts"),
            F.col("event_id").cast("string"),
        )
        .withColumn("ingested_at", F.current_timestamp())
    )
 
    invalid = typed.filter(
        F.col("order_id").isNull()
        | F.col("customer_id").isNull()
        | F.col("amount").isNull()
        | (F.col("amount") < 0)
    )
 
    valid = typed.filter(
        F.col("order_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & F.col("amount").isNotNull()
        & (F.col("amount") >= 0)
    )
    w = Window.partitionBy("order_id").orderBy(
        F.col("source_sequence").desc(),
        F.col("event_ts").desc(),
        F.col("event_id").desc(),
    )
 
    latest = valid.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    return latest, invalid
For high-volume production use, avoid collecting records to the driver. Persist invalid rows to a quarantine sink with batch/run metadata. Validate the uniqueness and nullability assumptions behind the window partition key.
23.3 Skew diagnosis and mitigation
Symptoms:
•	Most tasks finish quickly but a few run for a long time.
•	A small number of shuffle partitions contain a disproportionate amount of data.
•	Executors spill or run out of memory on specific keys.
Mitigation sequence:
149.	Confirm skew in Spark UI and key-frequency statistics.
150.	Filter early and project only needed columns.
151.	Broadcast a genuinely small dimension.
152.	Enable/test AQE skew join handling.
153.	Salt pathological hot keys when semantics permit.
154.	Revisit partitioning and data model.
155.	Increase resources only after identifying the bottleneck.
Salting pattern for an aggregation
from pyspark.sql import functions as F
 
salt_buckets = 16
 
salted = df.withColumn(
    "_salt",
    (F.rand(seed=42) * salt_buckets).cast("int")
)
 
partial = (
    salted.groupBy("customer_id", "_salt")
    .agg(F.sum("amount").alias("partial_amount"))
)
 
final = (
    partial.groupBy("customer_id")
    .agg(F.sum("partial_amount").alias("total_amount"))
)
This two-stage pattern is valid for decomposable aggregations such as sum/count. It is not a universal fix for joins or non-decomposable calculations. For deterministic replay, derive salt from a stable hash rather than random values.
23.4 Structured Streaming with checkpointing
from pyspark.sql import functions as F
 
events = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVERS)
    .option("subscribe", "order_events")
    .option("startingOffsets", "latest")
    .load()
)
 
parsed = (
    events.select(F.col("value").cast("string").alias("json"))
    .select(F.from_json("json", event_schema).alias("e"))
    .select("e.*")
    .withColumn("event_ts", F.to_timestamp("event_ts"))
)
 
aggregated = (
    parsed.withWatermark("event_ts", "30 minutes")
    .groupBy(F.window("event_ts", "5 minutes"), "region")
    .agg(F.sum("amount").alias("revenue"))
)
 
query = (
    aggregated.writeStream
    .format("parquet")
    .option("path", "gs://company-lake/silver/order_metrics/")
    .option("checkpointLocation", "gs://company-lake/checkpoints/order_metrics_v1/")
    .outputMode("append")
    .start()
)
This is a teaching example. Confirm that the chosen sink and output mode support the required semantics. A checkpoint directory belongs to a specific query identity and must not be casually reused by a different logical query. For a transactional table sink, use a supported connector/table format and document its commit protocol. A watermark bounds state and defines late-data behavior; it does not mean every older event is automatically discarded from the raw archive.
23.5 Interview scenario: one Spark task runs 40 times longer
Answer: I compare task input bytes/records, shuffle read, spill, GC time and executor placement. If one key dominates, I confirm skew with key-frequency profiling. I then filter/project early, broadcast a small dimension if safe, test AQE skew handling, or salt a hot key with a mathematically valid two-stage aggregation. I do not blindly add executors because the same skewed partition may remain the bottleneck.
---
PART 24 — AIRFLOW / CLOUD COMPOSER PRODUCTION SOP
24.1 DAG design standards
•	DAG files must be import-safe: no API calls, database queries or expensive computation at module import time.
•	Use UTC for orchestration timestamps; convert to business timezone only at defined boundaries.
•	Use `data_interval_start` and `data_interval_end` rather than assuming execution time equals data time.
•	Set retries, retry delay, execution timeout, pools and concurrency deliberately.
•	Use deterministic task inputs and idempotent writes.
•	Keep secrets in Secret Manager or an approved secrets backend.
•	Use deferrable sensors where supported for long waits.
•	Use dynamic task mapping for runtime-discovered work, but cap concurrency.
•	Treat backfills as production workloads with explicit capacity and source-retention checks.
24.2 TaskFlow example
from datetime import datetime, timedelta
from airflow.decorators import dag, task
 
@dag(
    dag_id="orders_daily_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=2),
    },
    tags=["orders", "production"],
)
def orders_daily_pipeline():
 
    @task
    def discover_batches(data_interval_start=None, data_interval_end=None):
        # Return small metadata descriptors only, not full datasets.
        return [{
            "batch_id": data_interval_start.strftime("orders_%Y%m%d"),
            "start": data_interval_start.isoformat(),
            "end": data_interval_end.isoformat(),
        }]
 
    @task
    def process_batch(batch):
        # Call an idempotent external job/API using batch["batch_id"].
        # The external job owns durable output and its commit manifest.
        return {"batch_id": batch["batch_id"], "status": "submitted"}
 
    @task
    def publish_completion(results):
        if not results or any(x["status"] != "submitted" for x in results):
            raise RuntimeError("One or more batches were not submitted")
        return results
 
    batches = discover_batches()
    results = process_batch.expand(batch=batches)
    publish_completion(results)
 
orders_daily_pipeline()
Airflow versions and provider APIs differ. Validate decorator arguments and context injection against the installed version. The example submits jobs; production code must wait for durable completion and validate the commit manifest before declaring the DAG successful.
24.3 Retry, timeout and idempotency
A retry is safe only if repeating the task has the same logical outcome. Use:
•	Deterministic run/batch IDs.
•	Immutable output paths.
•	Conditional object creation.
•	Idempotent `MERGE` or replace-partition operations.
•	External job status lookup before resubmission.
•	A clear commit marker.
•	A separate cleanup policy for abandoned temporary outputs.
Never use Airflow XCom as a bulk data transport. Store data in GCS/Snowflake and pass references/metadata through XCom.
24.4 Interview question: Airflow versus streaming engine
Answer: Airflow schedules, coordinates and monitors finite tasks. Kafka/Pub/Sub transports events; Dataflow or Spark Structured Streaming continuously processes them. I use Airflow to provision/deploy streaming jobs, run validations, manage batch dependencies and coordinate backfills—not to poll and process every individual event.
---
PART 25 — SNOWFLAKE ADVANCED TOPICS TO COMPLETE THE CORE
25.1 Streams, Tasks and Dynamic Tables: choose by operating model
Feature	Primary abstraction	Use when
Streams + Tasks	Explicit change tracking and scheduled/triggered SQL	You need procedural control over CDC consumption and transaction boundaries
Dynamic Tables	Declarative target-lag transformation graph	You want Snowflake to manage refresh of supported SQL transformations
Snowpipe	File-triggered ingestion	Data arrives as files in supported cloud storage
Snowpipe Streaming	Client-driven row ingestion	You need lower-latency row ingestion without a file notification cycle
Do not combine these merely because they are available. Choose the simplest service that satisfies latency, semantics, recovery and cost requirements.
25.2 Dynamic Table example
CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.DAILY_CUSTOMER_REVENUE
    TARGET_LAG = '10 MINUTES'
    WAREHOUSE = TRANSFORM_WH
AS
SELECT
    CUSTOMER_ID,
    ORDER_DATE::DATE AS ORDER_DAY,
    SUM(ORDER_AMOUNT) AS REVENUE
FROM ANALYTICS.FCT_ORDERS
GROUP BY CUSTOMER_ID, ORDER_DATE::DATE;
Target lag is a freshness objective, not a guarantee that every refresh completes within exactly that duration. Inspect refresh history and supported query limitations for the Snowflake release in use.
25.3 Row access and masking policy examples
CREATE OR REPLACE ROW ACCESS POLICY SECURITY.REGION_POLICY
AS (ROW_REGION VARCHAR)
RETURNS BOOLEAN ->
    IS_ROLE_IN_SESSION('GLOBAL_ANALYST')
    OR EXISTS (
        SELECT 1
        FROM SECURITY.ROLE_REGION_ACCESS A
        WHERE A.ROLE_NAME = CURRENT_ROLE()
          AND A.REGION = ROW_REGION
    );
 
ALTER TABLE MARTS.SALES
    ADD ROW ACCESS POLICY SECURITY.REGION_POLICY ON (REGION);
CREATE OR REPLACE MASKING POLICY SECURITY.EMAIL_MASK
AS (VAL VARCHAR)
RETURNS VARCHAR ->
    CASE
      WHEN IS_ROLE_IN_SESSION('PII_FULL_ACCESS') THEN VAL
      WHEN VAL IS NULL THEN NULL
      ELSE REGEXP_REPLACE(VAL, '(^.).*(@.*$)', '\\1***\\2')
    END;
 
ALTER TABLE MARTS.DIM_CUSTOMER
    MODIFY COLUMN EMAIL
    SET MASKING POLICY SECURITY.EMAIL_MASK;
Validate policy syntax, role hierarchy and policy evaluation in a test schema. Avoid granting direct PII access broadly; use least privilege, secure views and audited exception roles.
25.4 Resource monitor and workload governance
CREATE OR REPLACE RESOURCE MONITOR ETL_MONTHLY_MONITOR
    WITH CREDIT_QUOTA = 500
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
      ON 75 PERCENT DO NOTIFY
      ON 90 PERCENT DO NOTIFY
      ON 100 PERCENT DO SUSPEND;
 
ALTER WAREHOUSE ETL_WH
    SET RESOURCE_MONITOR = ETL_MONTHLY_MONITOR;
A resource monitor is a guardrail, not a substitute for cost attribution. Tag warehouses, users, databases and workloads; monitor serverless features separately where applicable; alert before hard suspension affects business SLAs.
25.5 Advanced interview questions
Q: A large `MERGE` is slow. What is your sequence?  
A: Inspect Query Profile and query history; quantify source and target scan, pruning, join cardinality, duplicate source keys, spill and queue time. Reduce the source to one deterministic row per merge key, filter to changed keys/partitions, verify clustering only if access patterns justify it, scale up only for compute pressure, and isolate the workload. I compare runtime and credits before/after.
Q: When would you use Dynamic Tables instead of dbt?  
A: Dynamic Tables suit warehouse-native declarative refresh with target-lag requirements and supported SQL. dbt is stronger when I need a software-engineering project structure, Jinja/macros, tests, documentation, package ecosystem, CI selection and explicit deployment workflow. They can coexist, but ownership of each transformation must be unambiguous.
Q: How do you handle schema evolution from Parquet?  
A: Treat schema as a versioned contract. Additive nullable fields may be accepted through a controlled compatibility policy. Renames, type narrowing and semantic changes require migration/versioning. I capture source schema version, quarantine incompatible files, test the downstream Snowflake/dbt contract, and never silently coerce a breaking change.
---
PART 26 — DBT ADVANCED DELIVERY AND TESTING
26.1 Incremental model with bounded lookback and merge key
{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_id',
    on_schema_change='fail'
  )
}}
 
WITH source_rows AS (
    SELECT
        order_id,
        customer_id,
        order_amount,
        source_updated_at,
        source_sequence
    FROM {{ source('raw_erp', 'orders') }}
 
    {% if is_incremental() %}
      WHERE source_updated_at >= (
        SELECT DATEADD(
          day, -3,
          COALESCE(MAX(source_updated_at), '1900-01-01'::TIMESTAMP_NTZ)
        )
        FROM {{ this }}
      )
    {% endif %}
),
deduplicated AS (
    SELECT *
    FROM source_rows
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY order_id
      ORDER BY source_sequence DESC, source_updated_at DESC
    ) = 1
)
SELECT * FROM deduplicated
The lookback catches bounded late arrivals but is not a mathematical guarantee if lateness is unbounded. For unbounded corrections, use a CDC sequence, explicit reprocessing window, affected-key queue or periodic reconciliation. Also ensure the generated merge cannot let a stale source row overwrite a newer target row.
26.2 dbt deployment gates
Recommended pull-request gates:
156.	`dbt deps`
157.	`dbt parse`
158.	SQL lint/static checks
159.	Unit tests for deterministic transformation logic
160.	Slim CI build using a trusted production manifest (`state:modified+`)
161.	Data tests on changed models and critical downstream models
162.	Contract/schema compatibility checks
163.	Review compiled SQL and query cost for high-impact models
164.	Deploy to isolated staging
165.	Promote the same immutable commit to production
Do not treat --full-refresh as a routine fix. Estimate rebuild cost, lock/replace implications, downstream dependencies and rollback plan first.
26.3 Unit test versus data test
•	Unit test: validates model logic for controlled input fixtures and expected output.
•	Data test: validates properties of built data, such as uniqueness, relationships, accepted values and business invariants.
•	Source freshness: checks whether source data arrived within an expected interval.
•	Contract test: validates schema/interface compatibility between producer and consumer.
Use all four at appropriate boundaries; no single test category replaces the others.
26.4 Interview scenario: incremental model silently missed late updates
Answer: I compare source change sequence and target watermark, inspect the lookback window, and identify the missed keys. I repair with a bounded key-level replay or controlled backfill, not an unreviewed full refresh. Then I add a test fixture for late arrivals, widen or replace the watermark design, and add periodic source-to-target reconciliation to detect future misses.
---
PART 27 — DATA MODELING AND BUSINESS SEMANTICS
27.1 Define the grain before writing the SQL
Every fact table must state its grain in one sentence. Examples:
•	`FCT_ORDER`: one row per order.
•	`FCT_ORDER_LINE`: one row per order line.
•	`FCT_PAYMENT_EVENT`: one row per payment event.
•	`FCT_DAILY_ACCOUNT_BALANCE`: one row per account per business date.
Do not mix order-level and order-line-level measures in one fact without explicit allocation rules. Declare additive, semi-additive and non-additive measures.
27.2 Conformed dimensions and surrogate keys
Use a stable surrogate key for warehouse joins and preserve the source business key plus source system. A customer may exist in multiple source systems with conflicting identifiers; define mastering/matching rules rather than assuming customer_id is globally unique.
27.3 SCD2 invariants
For each business key:
•	At most one current row.
•	`valid_from < valid_to` for closed intervals.
•	Validity intervals do not overlap.
•	A new version is created only for tracked attribute changes.
•	Source event ordering is deterministic.
•	Delete/reactivation is modeled explicitly.
•	Facts resolve to the dimension version valid at the fact's business event time, where required.
SQL test: overlapping validity intervals
WITH ordered AS (
    SELECT
        customer_id,
        valid_from,
        valid_to,
        LEAD(valid_from) OVER (
            PARTITION BY customer_id
            ORDER BY valid_from, source_sequence
        ) AS next_valid_from
    FROM MARTS.DIM_CUSTOMER_HISTORY
)
SELECT *
FROM ordered
WHERE next_valid_from < valid_to;
SQL test: more than one current row
SELECT customer_id, COUNT(*) AS current_rows
FROM MARTS.DIM_CUSTOMER_HISTORY
WHERE is_current = TRUE
GROUP BY customer_id
HAVING COUNT(*) > 1;
27.4 Interview scenario: fact arrives before dimension
Answer: I use an inferred/unknown dimension member or a suspense mapping according to business rules, preserve the natural key on the fact, and maintain a repair process. When the dimension arrives, I update the fact's surrogate-key reference if the warehouse design requires it. I do not discard the fact or invent dimension attributes. The policy must distinguish genuinely unknown from late-arriving dimension data.
---
PART 28 — SECURITY, PRIVACY, GOVERNANCE AND DATA PRODUCTS
28.1 Data classification
Classify data at ingestion: public, internal, confidential, restricted/PII, financial or regulated. Attach ownership, retention, residency and permitted-use metadata. Classification should drive storage, access, masking, logging and deletion behavior.
28.2 Governance controls
•	Data owner approves business meaning and access.
•	Data steward maintains definitions, quality rules and reference values.
•	Platform team owns runtime, reliability and access mechanisms.
•	Producer owns source contract and change communication.
•	Consumer owns downstream use and interpretation.
•	Lineage links source object → ingestion batch → transformation version → mart → dashboard.
28.3 Privacy operations
Implement retention and deletion as end-to-end workflows. A delete request may need to propagate through raw storage, curated layers, Snowflake, extracts, caches and downstream shares, subject to legal retention rules. Record the request, scope, completion evidence and exceptions. Immutable backups and regulatory holds need explicit policy handling.
28.4 Data contract example
contract:
  name: orders.v2
  owner: commerce-platform
  schema_version: 2
  primary_key: [order_id]
  event_identity: event_id
  event_time: source_commit_timestamp
  compatibility: backward_compatible_additive
  fields:
    - name: order_id
      type: string
      nullable: false
    - name: customer_id
      type: string
      nullable: false
    - name: amount
      type: decimal(18,2)
      nullable: false
    - name: source_commit_timestamp
      type: timestamp
      nullable: false
A contract is effective only when enforced in CI and at runtime. A YAML file without compatibility checks or producer/consumer ownership is documentation, not governance.
---
PART 29 — SRE, SLOs, INCIDENT RESPONSE AND DISASTER RECOVERY
29.1 Define data SLOs
SLI	Example SLO	Measurement
Freshness	99% of daily orders available by 06:00 local business time	Source event time to mart availability
Completeness	At least 99.95% of expected source records accounted for	Source/landed/loaded/rejected counts
Validity	99.9% pass critical schema/business rules	Accepted / evaluated records
Availability	Critical mart query succeeds 99.9% of scheduled checks	Synthetic query result
Recovery	Critical pipeline restored within 2 hours	Incident timeline
Targets are examples. Agree them with data consumers and measure from business impact, not arbitrary infrastructure metrics.
29.2 Incident response runbook
166.	Declare severity and identify affected data products.
167.	Stop unsafe downstream publication if correctness is at risk.
168.	Establish the last known good committed batch/watermark.
169.	Preserve logs, query IDs, manifests and source offsets.
170.	Determine blast radius: source, ingestion, storage, transformation or consumption.
171.	Restore using replay/backfill from immutable raw data.
172.	Reconcile counts, amounts, keys and business aggregates.
173.	Run quality gates before republishing.
174.	Communicate impact, recovery ETA and known limitations.
175.	Complete a blameless post-incident review with prevention actions and owners.
29.3 Recovery objectives
•	RPO: maximum tolerable data loss measured in time or committed source position.
•	RTO: maximum tolerable time to restore service.
•	Replay horizon: how far back raw events and source logs remain available.
•	Recovery dependency: credentials, schemas, code artifacts, network, KMS keys and cloud quotas.
A recovery plan is incomplete unless it has been tested. Run periodic restore/replay exercises and record measured RPO/RTO.
29.4 Disaster recovery checklist
•	[ ] Raw GCS retention and object versioning/retention policy reviewed.
•	[ ] Snowflake replication/failover design documented where licensed/available.
•	[ ] Terraform state and deployment artifacts recoverable.
•	[ ] Secrets and key recovery paths tested.
•	[ ] Source CDC log retention exceeds worst-case recovery horizon.
•	[ ] dbt manifests, package lock and Git commit are preserved.
•	[ ] Airflow variables/connections/pools and DAG code can be recreated.
•	[ ] Cross-region/network dependencies documented.
•	[ ] Restore and reconciliation exercise completed.
---
PART 30 — ADVANCED SENIOR / LEAD DATA ENGINEER INTERVIEW BANK
Use a consistent answer structure: clarify requirement → state assumptions → explain design → discuss failure semantics → quantify trade-offs → describe monitoring and recovery. A senior answer should not jump straight to a tool name.
A. Architecture and systems design
1. Design a platform for 2 TB/day of CDC plus daily batch files.
Answer: I separate event transport, durable raw retention, compute, warehouse modeling and consumption. Database-log CDC enters Kafka or Pub/Sub with source sequence metadata; files land in GCS with immutable batch IDs and manifests. Dataflow or Spark validates and writes curated Parquet/Avro. Snowflake ingests through Snowpipe/COPY or Snowpipe Streaming depending on latency. dbt builds dimensions/facts/marts. Airflow/Composer orchestrates finite batch dependencies and deployment, while streaming engines run continuously. I define per-domain contracts, replay horizon, SLOs, workload isolation, IAM boundaries and cost budgets before selecting partition counts or warehouse sizes.
2. How do you guarantee no data loss across GCS and Snowflake?
Answer: I do not promise a distributed exactly-once transaction across independent systems. I use durable raw landing, immutable object names, a committed manifest, file-load history, deterministic batch IDs, idempotent target merges and a control table. A batch is committed only after reconciliation. Retries may redeliver data, but the sink's idempotency key prevents duplicate business effects.
3. How would you migrate a 100 TB warehouse with minimal downtime?
Answer: Inventory dependencies and critical reports; establish source/target schema contracts; bulk-load a consistent snapshot; capture ongoing changes with CDC; dual-run and reconcile row counts, aggregates and key-level samples; validate query semantics and performance; freeze or fence writes at cutover; apply final CDC; switch consumers; retain rollback capability. I define acceptance thresholds and rollback triggers before starting.
4. When would you not use streaming?
Answer: When the business SLA is hourly/daily, source changes are naturally batch-oriented, operational complexity outweighs latency benefit, or consumers need complete closed-period data. Micro-batch or batch is often simpler, cheaper and easier to reconcile.
B. Snowflake and SQL
5. A `MERGE` produces duplicate target rows. Why?
Answer: Common causes are duplicate source keys, pre-existing target duplicates, non-unique match conditions, concurrent writers or an incorrect grain. I profile source and target keys, enforce one deterministic source row per target key, serialize competing writers or partition ownership, and add uniqueness tests. Snowflake standard-table PK constraints do not enforce uniqueness.
SELECT order_id, COUNT(*)
FROM STG_ORDER_CHANGES
GROUP BY order_id
HAVING COUNT(*) > 1;
 
SELECT order_id, COUNT(*)
FROM MARTS.FCT_ORDER
GROUP BY order_id
HAVING COUNT(*) > 1;
6. A query scans 90% of micro-partitions. What do you do?
Answer: First determine whether 90% is expected for the requested result. Inspect filters, data distribution, partition metadata, join order/cardinality and Query Profile. Rewrite non-sargable date predicates, project fewer columns, filter before joins, and evaluate clustering only if repeated selective access patterns justify its cost. A low scan ratio is not itself the objective; efficient completion at acceptable credits is.
7. How do you prevent an old CDC update from overwriting a newer state?
Answer: Store a source ordering token (LSN/SCN/binlog position/sequence) with the target. Compare the incoming token with the target token; update only when incoming is newer. Timestamps alone are insufficient if they can tie or be backdated.
MERGE INTO CORE.CUSTOMER_CURRENT T
USING STG.CUSTOMER_LATEST S
ON T.CUSTOMER_ID = S.CUSTOMER_ID
WHEN MATCHED
 AND S.SOURCE_SEQUENCE > T.SOURCE_SEQUENCE
 AND S.OPERATION <> 'DELETE'
THEN UPDATE SET
  T.CITY = S.CITY,
  T.SOURCE_SEQUENCE = S.SOURCE_SEQUENCE,
  T.SOURCE_COMMIT_TS = S.SOURCE_COMMIT_TS
WHEN MATCHED
 AND S.SOURCE_SEQUENCE > T.SOURCE_SEQUENCE
 AND S.OPERATION = 'DELETE'
THEN DELETE
WHEN NOT MATCHED
 AND S.OPERATION <> 'DELETE'
THEN INSERT (CUSTOMER_ID, CITY, SOURCE_SEQUENCE, SOURCE_COMMIT_TS)
VALUES (S.CUSTOMER_ID, S.CITY, S.SOURCE_SEQUENCE, S.SOURCE_COMMIT_TS);
This assumes one deterministic latest event per customer in S and a comparable sequence domain. If deletes must remain auditable, write tombstones to a history table rather than physically deleting the only copy.
8. How do you investigate a 40% Snowflake credit increase?
Answer: Attribute usage by warehouse, query tag, user, task, pipe and serverless feature; compare workload volume and runtime to baseline; separate warehouse runtime from queueing and cloud-services/serverless usage; identify regressions, full refreshes, retries and concurrency; then optimize the dominant cost driver. Set resource monitors and workload budgets after attribution.
C. dbt and modeling
9. A dbt incremental model is correct in dev but misses production rows.
Answer: Compare target state, source watermark, timezone, lookback, unique key, late-arrival distribution, concurrent runs, grants and adapter configuration. Reproduce with a production-like fixture. Backfill affected keys/periods, then add late-arrival and idempotency tests. Never run a blind full refresh before estimating impact.
10. Snapshot or CDC for SCD2?
Answer: Snapshot when periodic observation of mutable state meets the requirement. Use CDC when every source transition, commit order, delete and late-arriving event matters. Snapshots cannot infer intermediate states that disappeared between runs.
11. How do you make dbt CI fast but safe?
Answer: Parse and lint every change, use state-based selection against a trusted manifest, build modified models and relevant descendants in an isolated schema, run unit/data/contract tests, and promote the same reviewed commit. Critical shared models may require broader regression tests.
D. PySpark and streaming
12. Executor OOM occurs only on one stage.
Answer: Inspect Spark UI for skew, wide rows, oversized partitions, broadcast size, cache pressure and shuffle spill. Reduce columns, tune partition sizing, use AQE, avoid collect(), reconsider broadcast threshold, and salt hot keys only when semantics permit. Increase memory only after finding the cause.
13. What does a watermark guarantee?
Answer: It tracks event-time progress and bounds state for supported stateful operations. It defines how long late data may update a window/state; it is not a global guarantee that all older events are retained or that no late event can ever arrive. Archive raw events and define a correction path.
14. Consumer lag grows continuously.
Answer: Determine whether input rate exceeds processing rate, whether partitions are imbalanced, whether downstream writes are slow, or whether consumers are repeatedly failing/rebalancing. Compare per-partition lag and throughput; scale consumers only up to partition parallelism and sink capacity; fix hot keys, backpressure and poison messages.
E. Airflow and production operations
15. A task times out after writing output but before marking success.
Answer: The task must be safe to retry. It checks the deterministic batch ID and committed manifest, validates existing output, and either treats the prior completed result as success or resumes from a safe stage. It never blindly appends a second copy. The watermark is advanced only after the durable target commit.
16. How do you backfill six months without destabilizing production?
Answer: Estimate source availability and compute cost; use a separate backfill queue/warehouse/pool; process bounded intervals; cap concurrency; make writes partition/key-scoped and idempotent; monitor downstream load; reconcile each interval; and preserve the production freshness SLA. Backfill should be restartable and cancellable.
17. A dashboard is wrong but all pipeline tasks are green.
Answer: Trace the metric definition and lineage from dashboard to semantic model, mart, dbt model, Snowflake raw load and source. Validate grain, timezone, currency, late-arriving facts, join fan-out, filters and cache/extract freshness. Pipeline success proves execution, not business correctness.
F. Leadership and design judgement
18. A team wants a new tool for every pipeline. What do you recommend?
Answer: Standardize a small paved road: approved ingestion patterns, storage layout, contracts, observability, CI/CD and security defaults. Allow exceptions when a measurable requirement cannot be met. Optimize for operability and team skill, not novelty.
19. How do you review a junior engineer's production SQL?
Answer: Review grain, join cardinality, null semantics, deterministic ordering, incremental correctness, late data, deletes, query profile, data contracts, tests, rollback and cost. I ask the author to explain failure modes and recovery, not only the happy path.
20. How do you communicate a data incident to executives?
Answer: State affected product/metrics, time window, business impact, confidence level, workaround, next update time and recovery ETA. Keep technical hypotheses separate from confirmed facts. After resolution, publish root cause, corrective actions, owners and due dates.
---
PART 31 — END-TO-END CAPSTONE ACCEPTANCE CRITERIA
The capstone is complete only when it demonstrates all of the following:
176.	**Ingestion:** API batch and database CDC both land in GCS with immutable paths, manifests and schema versions.
177.	**Replay:** A selected date/batch can be replayed without duplicate business effects.
178.	**CDC correctness:** Inserts, updates, deletes, duplicate events, tied timestamps and out-of-order events are handled.
179.	**SCD2:** Validity intervals are deterministic, non-overlapping and have at most one current row.
180.	**Warehouse:** Snowflake RAW/landing/curated/mart responsibilities are documented; warehouses are isolated by workload.
181.	**dbt:** Sources, refs, incremental models, snapshots where appropriate, tests, docs and CI are implemented.
182.	**PySpark:** At least one distributed transformation includes schema validation, quarantine, skew-aware joins and output compaction.
183.	**Streaming:** Checkpointing, event-time semantics, late-data policy, DLQ and replay are demonstrated.
184.	**Orchestration:** Airflow handles dependencies, retries, backfills, timeouts, pools and idempotent execution.
185.	**Quality:** Count, amount, key, freshness and referential reconciliations are automated.
186.	**Security:** Workload identities, least privilege, PII masking and auditability are tested.
187.	**Operations:** Dashboards, alerts, runbooks, SLOs and a post-incident template exist.
188.	**Recovery:** A failure is injected at each major boundary and recovered from the last committed checkpoint.
189.	**Cost:** Baseline and optimized cost/performance are recorded.
190.	**Documentation:** Architecture decisions, data contracts, lineage, ownership and support procedures are maintained.
Final sign-off checklist
•	[ ] Every pipeline has an owner, SLA/SLO, runbook and escalation path.
•	[ ] Every dataset has a declared grain, key, schema and business definition.
•	[ ] Every ingestion path has a stable event/batch identity.
•	[ ] Every retry is idempotent or explicitly compensating.
•	[ ] Every watermark has a documented commit point and replay strategy.
•	[ ] Every CDC entity has delete and out-of-order semantics.
•	[ ] Every critical model has tests and reconciliation.
•	[ ] Every sensitive dataset has classification and access policy.
•	[ ] Every deployment is versioned, reviewed and reversible.
•	[ ] Recovery has been tested, not merely documented.
Final engineering principle
A senior or lead data engineer is accountable for the full lifecycle: design, correctness, deployment, cost, security, operability and recovery. Tool familiarity is necessary but insufficient. The strongest evidence of seniority is the ability to explain what happens when assumptions fail—and to show, with code and measurable controls, how the platform recovers without corrupting business data.
 
PART 32 — INTERVIEW-GRADE SCENARIO PLAYBOOK
This section is deliberately scenario-driven. In interviews, explain the business impact, failure mode, correctness invariant, implementation, observability and trade-off—not just the tool.
32.1 Late, duplicated and out-of-order CDC events
Interview scenario: An upstream database emits duplicate events, updates can arrive out of order, and deletes must be reflected without losing history.
Senior-level approach: Use a stable source key plus source commit/version ordering. Deduplicate by event identity, then apply only the newest version. Keep tombstones for deletes. Do not use ingestion time as source ordering unless the source guarantees it.
Implementation pattern
-- Snowflake pattern: one winning event per business key
MERGE INTO CORE.CUSTOMER t
USING (
  SELECT *
  FROM RAW.CUSTOMER_CDC
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CUSTOMER_ID
    ORDER BY SOURCE_LSN DESC, INGESTED_AT DESC
  ) = 1
) s
ON t.CUSTOMER_ID = s.CUSTOMER_ID
WHEN MATCHED AND s.SOURCE_LSN > t.SOURCE_LSN
  AND s.OP = 'D' THEN DELETE
WHEN MATCHED AND s.SOURCE_LSN > t.SOURCE_LSN
  AND s.OP <> 'D' THEN UPDATE SET
    NAME = s.NAME, EMAIL = s.EMAIL, SOURCE_LSN = s.SOURCE_LSN
WHEN NOT MATCHED AND s.OP <> 'D' THEN INSERT
  (CUSTOMER_ID, NAME, EMAIL, SOURCE_LSN)
  VALUES (s.CUSTOMER_ID, s.NAME, s.EMAIL, s.SOURCE_LSN);
Production checks: Test duplicate event IDs, same-key ties, late lower LSN, delete followed by stale update, and replay of the same batch. Track source LSN lag and rejected events.
Likely follow-up: How would you handle a source that has no monotonic LSN? State the weaker guarantee, use a source-issued version if possible, and quarantine ambiguous conflicts.
32.2 Idempotent file ingestion with manifest and checksum
Interview scenario: A daily delivery can be resent, partially uploaded, or contain a corrupt file. The pipeline must not double count.
Senior-level approach: Treat the manifest as the batch contract. Validate file size/checksum and schema, record a durable file identity, stage first, then commit target changes and ingestion ledger atomically where possible.
Implementation pattern
-- Ledger prevents processing the same immutable object twice
CREATE TABLE IF NOT EXISTS OPS.FILE_LEDGER (
  OBJECT_URI STRING, GENERATION_ID STRING, CHECKSUM STRING,
  STATUS STRING, ROW_COUNT NUMBER, PROCESSED_AT TIMESTAMP_NTZ,
  PRIMARY KEY (OBJECT_URI, GENERATION_ID)
);

-- Python orchestration sketch
for item in manifest["files"]:
    assert item["size_bytes"] > 0
    actual = gcs_sha256(item["uri"])
    if actual != item["sha256"]:
        quarantine(item, reason="checksum_mismatch")
        continue
    if ledger.exists(item["uri"], item["generation"]):
        continue
    stage_file(item["uri"])
    validate_stage(item["batch_id"])
    merge_batch(item["batch_id"])
    ledger.mark_committed(item, row_count=count_batch(item["batch_id"]))
Production checks: Test retry after stage, retry after merge, object overwrite with a new generation, empty manifest, corrupt checksum and partial batch. Make the commit marker the final step.
Likely follow-up: Why not mark the file processed before MERGE? A crash would permanently skip data. Explain exactly-once effects as idempotent processing plus durable progress.
32.3 Incremental model with a lookback window
Interview scenario: An incremental fact model receives records up to 72 hours late. A simple created_at greater-than-max watermark misses corrections.
Senior-level approach: Use a bounded lookback for normal lateness, plus periodic reconciliation for lateness beyond the bound. MERGE on the true grain; do not append blindly.
Implementation pattern
-- dbt/Snowflake incremental model pattern
{{ config(materialized='incremental', unique_key='order_id') }}

SELECT
  order_id, customer_id, order_status, amount,
  source_updated_at
FROM {{ source('app', 'orders') }}
{% if is_incremental() %}
WHERE source_updated_at >= (
  SELECT DATEADD(hour, -72, COALESCE(MAX(source_updated_at), '1900-01-01'))
  FROM {{ this }}
)
{% endif %}
Production checks: Measure arrival-lag distribution and late-row rate. Run a daily wider reconciliation; test updates, deletes, null timestamps, timezone conversion and first-run behavior.
Likely follow-up: How choose the lookback? Derive it from observed lateness percentiles and business SLA, then document the residual risk.
32.4 Skewed PySpark join causing executor OOM
Interview scenario: A 20 TB fact table joins to a customer dimension, but one customer key owns 35% of rows and tasks run far longer than others.
Senior-level approach: Confirm skew in Spark UI: task duration, shuffle read, spill and partition size. Filter/project early, broadcast only if the dimension safely fits, otherwise use AQE skew handling or targeted salting.
Implementation pattern
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

fact = (spark.read.parquet(fact_path)
        .select("customer_id", "event_date", "amount")
        .filter(F.col("event_date") >= F.lit("2026-01-01")))

dim = (spark.read.parquet(dim_path)
       .select("customer_id", "segment"))

# Only broadcast when measured dimension size is safely below executor memory.
result = fact.join(broadcast(dim), "customer_id", "left")

# Enable adaptive execution and skew join handling in the session/job config:
# spark.sql.adaptive.enabled=true
# spark.sql.adaptive.skewJoin.enabled=true
Production checks: Compare input vs shuffle bytes, spill, max/median task duration, executor GC and broadcast size. Validate row counts and unmatched keys before/after optimization.
Likely follow-up: Explain salting: add a deterministic salt to the hot side and replicate matching dimension rows across salt buckets; quantify extra dimension expansion.
32.5 Event-time streaming with watermark and deduplication
Interview scenario: A mobile event may arrive 40 minutes late, and producers retry. Compute 10-minute event-time counts without unbounded state.
Senior-level approach: Use event time, a watermark based on the lateness contract, and a stable event ID. Watermarks bound state; they do not guarantee all arbitrarily late events are retained.
Implementation pattern
from pyspark.sql import functions as F

events = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", brokers)
    .option("subscribe", "app-events")
    .load())

parsed = (events.selectExpr("CAST(value AS STRING) AS json")
    .select(F.from_json("json", schema).alias("e"))
    .select("e.*")
    .withColumn("event_ts", F.to_timestamp("event_time")))

counts = (parsed
    .withWatermark("event_ts", "45 minutes")
    .dropDuplicates(["event_id", "event_ts"])
    .groupBy(F.window("event_ts", "10 minutes"), "event_type")
    .count())

query = (counts.writeStream
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .format("delta")
    .start(output_path))
Production checks: Monitor input rate, processing rate, batch duration, state-store size, watermark delay, late dropped rows and checkpoint health. Test restart from checkpoint and duplicate events.
Likely follow-up: What if lateness grows to 3 hours? Change the contract and watermark with state/cost impact understood, or route late corrections to a backfill path.
32.6 Airflow backfill without overwhelming the warehouse
Interview scenario: A 400-day backfill is required while daily production continues. The warehouse has a strict concurrency and cost budget.
Senior-level approach: Separate historical backfill from the live DAG or use a controlled parameterized backfill. Limit active runs/tasks, use date partitions, idempotent writes and a pause/resume mechanism.
Implementation pattern
with DAG(
    dag_id="daily_orders",
    schedule="@daily",
    catchup=False,
    max_active_runs=2,
    max_active_tasks=8,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=5)},
) as dag:
    @task(pool="warehouse_pool")
    def build_partition(ds=None):
        # Use logical date, not wall-clock now(), to make retries deterministic.
        run_sql_for_date(ds)

    build_partition()
Production checks: Use Airflow pools, task concurrency, warehouse resource monitors, partition-level audit status and a canary date. Verify retries do not duplicate output.
Likely follow-up: Why is `now()` dangerous in a backfill? It makes the same logical run produce different source boundaries on retry.
32.7 Data quality failure: block, quarantine or warn?
Interview scenario: A source sends 12% null customer IDs. The dashboard SLA is at risk, but dropping records would hide revenue.
Senior-level approach: Classify rules by severity. Hard invariants block publication; recoverable bad rows are quarantined with reason codes; statistical anomalies alert and may trigger a hold depending on contract.
Implementation pattern
-- Separate accepted and rejected rows
CREATE OR REPLACE TABLE STG.ORDERS_VALID AS
SELECT *, CASE
  WHEN order_id IS NULL THEN 'missing_order_id'
  WHEN amount < 0 THEN 'negative_amount'
  WHEN order_ts IS NULL THEN 'missing_order_ts'
  ELSE NULL END AS reject_reason
FROM RAW.ORDERS;

INSERT INTO OPS.ORDERS_QUARANTINE
SELECT * FROM STG.ORDERS_VALID WHERE reject_reason IS NOT NULL;

-- Publish only after hard-gate checks pass
SELECT COUNT_IF(reject_reason IS NOT NULL) AS rejected,
       COUNT(*) AS total
FROM STG.ORDERS_VALID;
Production checks: Define owner, severity, threshold, quarantine retention, alert route and release criteria. Reconcile raw = accepted + quarantined; never silently discard.
Likely follow-up: Would you block the entire pipeline? Only if the failed rule threatens correctness of the published product; otherwise isolate and communicate partial availability.
32.8 Snowflake slow query diagnosis
Interview scenario: A query becomes 10x slower after data volume doubles. The team asks to resize the warehouse immediately.
Senior-level approach: Read Query Profile first: partitions scanned vs total, bytes spilled, join explosion, remote/local disk spill, queue time and pruning. Fix SQL/data layout before buying compute.
Implementation pattern
-- Inspect recent query executions
SELECT query_id, warehouse_name, total_elapsed_time/1000 AS elapsed_seconds,
       bytes_scanned, partitions_scanned, partitions_total,
       bytes_spilled_to_local_storage, bytes_spilled_to_remote_storage,
       queued_overload_time
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD(day, -1, CURRENT_TIMESTAMP())
  AND execution_status = 'SUCCESS'
ORDER BY total_elapsed_time DESC
LIMIT 50;
Production checks: Compare representative query IDs, query profile operator timings, scan selectivity, warehouse load and concurrency. Establish a before/after benchmark.
Likely follow-up: Scale up for memory/compute-bound single queries; scale out for concurrency isolation. Explain why neither fixes a many-to-many join explosion.
32.9 SCD Type 2 with same-day changes
Interview scenario: A customer changes address twice on the same day. Preserve history and guarantee one current row.
Senior-level approach: Use source event ordering at timestamp/LSN precision, expire the current version, insert the new version, and define whether intervals are half-open [valid_from, valid_to).
Implementation pattern
-- Validate SCD2 invariant: exactly one current row per business key
SELECT customer_id, COUNT_IF(is_current) AS current_rows
FROM DIM.CUSTOMER
GROUP BY customer_id
HAVING COUNT_IF(is_current) <> 1;

-- Overlap detection for half-open validity intervals
SELECT a.customer_id, a.customer_sk, b.customer_sk
FROM DIM.CUSTOMER a
JOIN DIM.CUSTOMER b
  ON a.customer_id = b.customer_id
 AND a.customer_sk < b.customer_sk
 AND a.valid_from < COALESCE(b.valid_to, '9999-12-31'::TIMESTAMP_NTZ)
 AND b.valid_from < COALESCE(a.valid_to, '9999-12-31'::TIMESTAMP_NTZ);
Production checks: Test same timestamp tie-breaking, late-arriving dimension change, unchanged attributes, reactivation, delete and interval overlap. Use a deterministic surrogate key.
Likely follow-up: How handle a late historical change? Split/restate affected validity intervals and rebuild impacted facts or use a correction strategy agreed with consumers.
32.10 Schema evolution and breaking contract change
Interview scenario: A producer renames a field and changes decimal amount to string. Downstream jobs must not silently corrupt data.
Senior-level approach: Use versioned data contracts and compatibility checks. Additive nullable fields are usually backward-compatible; rename/type narrowing/removal are breaking. Stage v2 side-by-side and migrate consumers.
Implementation pattern
# Example contract gate (Python)
BREAKING = {"removed_required", "type_narrowed", "renamed_without_alias"}

def validate_change(old: dict, new: dict):
    changes = compare_schemas(old, new)
    breaking = [c for c in changes if c.kind in BREAKING]
    if breaking:
        raise ValueError(f"Contract change requires version bump: {breaking}")
    return changes

# At ingestion: preserve raw payload, parse by declared schema version,
# quarantine unknown/incompatible versions instead of coercing silently.
Production checks: Test old producer/new consumer and new producer/old consumer combinations. Track schema version, unknown fields, parse failures and deprecation dates.
Likely follow-up: What is the safest migration? Expand, dual-write/read, migrate consumers, verify parity, then contract/remove the old field.
PART 33 — CODING ROUND PATTERNS YOU SHOULD BE ABLE TO WRITE FROM MEMORY
Python: stream a large file without loading it all
import json
from pathlib import Path

def read_jsonl(path):
    with Path(path).open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            if not line.strip():
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON at line {line_no}") from exc
Python: bounded retries with exponential backoff and jitter
import random, time

def retry(call, attempts=5, base=0.5, cap=20):
    for n in range(attempts):
        try:
            return call()
        except TransientError:
            if n == attempts - 1:
                raise
            delay = min(cap, base * (2 ** n))
            time.sleep(delay * random.uniform(0.7, 1.3))
SQL: top N per group, deterministic tie-break
SELECT *
FROM (
  SELECT e.*,
         ROW_NUMBER() OVER (
           PARTITION BY department_id
           ORDER BY salary DESC, employee_id
         ) AS rn
  FROM employee e
) x
WHERE rn <= 3;
SQL: gaps and islands for consecutive dates
WITH x AS (
  SELECT user_id, activity_date,
         DATEADD(day, -ROW_NUMBER() OVER
           (PARTITION BY user_id ORDER BY activity_date), activity_date) AS grp
  FROM (SELECT DISTINCT user_id, activity_date FROM activity)
)
SELECT user_id, MIN(activity_date) AS streak_start,
       MAX(activity_date) AS streak_end, COUNT(*) AS days
FROM x
GROUP BY user_id, grp;
PySpark: deduplicate by latest source version
from pyspark.sql import functions as F, Window
w = Window.partitionBy("id").orderBy(
    F.col("source_version").desc(), F.col("ingested_at").desc()
)
latest = (df.withColumn("rn", F.row_number().over(w))
            .filter(F.col("rn") == 1)
            .drop("rn"))
PySpark: aggregate with explicit null handling
summary = (df.groupBy("business_date", "region")
    .agg(F.count("*").alias("row_count"),
         F.countDistinct("customer_id").alias("customers"),
         F.sum(F.coalesce("net_amount", F.lit(0))).alias("net_amount")))
Airflow: task groups and retry policy
default_args = {
  "owner": "data-platform",
  "retries": 3,
  "retry_delay": timedelta(minutes=4),
  "retry_exponential_backoff": True,
  "max_retry_delay": timedelta(minutes=30),
}
dbt: test uniqueness and accepted values
models:
  - name: fct_orders
    columns:
      - name: order_id
        data_tests: [not_null, unique]
      - name: status
        data_tests:
          - accepted_values:
              arguments:
                values: ['placed', 'shipped', 'cancelled']
PART 34 — SYSTEM DESIGN ANSWER FRAMEWORK
For any ‘design a data platform/pipeline’ interview, answer in this order:
191.	Clarify users, business metric, freshness SLA, data volume, growth, retention, regions and compliance.
192.	Define source semantics: batch/API/CDC/events, keys, ordering, deletes, schema evolution and delivery guarantees.
193.	Draw the path: source → immutable landing → validation/quarantine → transform → serving layer → consumers.
194.	State correctness: grain, idempotency key, deduplication rule, watermark, late-data policy, replay and reconciliation.
195.	State scale choices: partitioning, file sizing, parallelism, shuffle strategy, warehouse sizing and concurrency.
196.	State operations: orchestration, retries, backfill, lineage, metrics, alerting, SLO, ownership and runbook.
197.	State security: least privilege, secrets, encryption, PII classification, masking, audit and retention.
198.	State trade-offs and failure modes. Give one design alternative and explain why you rejected it.
PART 35 — FINAL INTERVIEW READINESS SCORECARD
☐ Can explain a complete batch + CDC + streaming architecture and its failure recovery.
☐ Can write SQL for deduplication, SCD1/SCD2, sessionization, gaps/islands, top-N and incremental loads.
☐ Can write Python for pagination, retries, API ingestion, schema validation, file manifests and unit tests.
☐ Can explain Spark shuffles, partition sizing, skew, broadcast joins, AQE, checkpointing and stateful streaming.
☐ Can diagnose Snowflake query profiles, pruning, spills, warehouse queues, micro-partitions and cost.
☐ Can design dbt incremental models, snapshots, tests, macros, source freshness and CI deferral.
☐ Can design Airflow DAGs with logical dates, idempotency, pools, backfills, retries and SLAs.
☐ Can explain data contracts, privacy, IAM, lineage, observability, SLOs, DR and incident command.
☐ Can quantify trade-offs using data volume, latency, cost, failure probability and recovery time.
☐ Can narrate one end-to-end project with architecture, scale, personal contribution, incidents and measurable outcomes.
Important interview discipline: never claim a universal exactly-once guarantee. Explain the boundary (source, transport, sink), the deduplication key, transaction/commit behavior, replay semantics and what happens after a crash. Be explicit about assumptions and product/version-specific syntax.
