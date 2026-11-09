# Senior Data Engineer Master Handbook: Final Edition

*Snowflake + dbt + GCS/GCP + Python + PySpark + Airflow + Kafka/Pub/Sub + CDC. Finishes the handbook, adds missing chapters, production code, and an advanced interview bank. Prepared November 2026.*

## Contents and how to use this handbook

This is the consolidated, corrected edition. Every code sample applies the corrections found during review (ordering by source position rather than arrival time, a single `9999-12-31` open-ended sentinel, collapsed stream semantics, idempotent writes, `gcs://` for Snowflake stages and `gs://` for Spark/gsutil). Each technical chapter follows the same shape where useful: concept, when to use it, implementation, challenges, interview angle.

| Part | Chapters |
| --- | --- |
| I. Foundations | 1 Architecture, 2 Batch/Streaming/CDC, 3 GCS data lake, 4 Snowflake core, 5 Snowflake performance and administration, 6 Snowpipe, Streams, Tasks |
| II. Transformation engineering | 7 dbt, 8 SCD and dimensional modeling |
| III. Processing and orchestration | 9 Python, 10 PySpark, 11 Airflow |
| IV. Reliability and delivery | 12 Data quality, 13 Security and observability, 14 CI/CD and Terraform, 15 Troubleshooting runbooks |
| V. Interview preparation | 16 Conceptual, scenario and coding questions |
| VI. Platform extensions | 17 Pub/Sub and Dataflow, 18 Snowflake advanced features, 19 DR and cost, 20 Capstone |
| VII. Decision guides | 21 When to use what, 22 Advanced concepts, use cases and challenges |

### Seven invariants that every design must satisfy

1. **Immutable raw record.** Original source events are kept so any layer can be rebuilt.
2. **Source ordering.** State is decided by source commit position (timestamp plus sequence), never arrival time.
3. **Idempotent writes.** Running any step twice for the same input produces the same result.
4. **Watermarks advance last.** Only after a durable, verified commit.
5. **Explicit deletes.** A delete is an event that wins by position; absence is not a delete.
6. **Reconciliation.** Counts and checksums are compared per batch, not just at the end.
7. **Rebuildability.** After a defect, a layer can be recomputed from Bronze without touching the source.

---

# Chapter 1. Enterprise Data Engineering Architecture

## 1.1 Reference architecture

```text
SOURCES: Oracle / SQL Server / PostgreSQL / MySQL / SaaS APIs / files
   |                                   |
 BATCH (Airflow + Python/API)       CDC (Debezium / Datastream)
   |                                   |
   |                              Kafka or Pub/Sub
   |                                   |
   |                      Dataflow / Spark Structured Streaming
   +----------------+------------------+
                    v
   GCS BRONZE   raw JSON/Avro/Parquet, immutable, manifests
                    v   Dataproc / PySpark (cleanse, validate, quarantine)
   GCS SILVER   typed, partitioned Parquet
                    v   Storage integration + stage, COPY INTO / Snowpipe
   SNOWFLAKE    RAW -> LANDING -> PREPARE -> NORMALIZED -> SCHEMATIZED -> MARKETPLACE -> SEMANTIC
                    v   dbt Core orchestrated by Airflow / Cloud Composer
   BI           Tableau / Power BI (read-only governed role)
```

Airflow orchestrates scheduled work. Streaming jobs run continuously with their own checkpoints; Airflow deploys, monitors and coordinates them but does not restart them per event. The same design works on AWS by swapping GCS for S3, Datastream for DMS and Dataproc for EMR/Glue; Snowflake and dbt layers stay identical.

## 1.2 Layers are contracts, not copies

| Medallion | Snowflake layer | Responsibility | Typical materialization |
| --- | --- | --- | --- |
| Bronze | RAW | Preserve source payload plus ingestion metadata | Table, append-only |
| Bronze/Silver | LANDING | Consistent structure, type-safe casts, file/batch lineage | Incremental table or view |
| Silver | PREPARE | Trim, standardise, validate, delivery-dedup, quarantine | Incremental table |
| Silver | NORMALIZED | Conformed business entities and relationships | Table |
| Gold | SCHEMATIZED | Facts, dimensions, SCD, surrogate keys, defined grain | Incremental / table |
| Gold | MARKETPLACE | Business-ready data products and aggregates | Table / Dynamic Table |
| Gold | SEMANTIC | Governed KPIs and consumption views | Views |
| n/a | AUDIT | Run log, watermarks, rejected records, reconciliation | Tables |

Some layers can be views or merged when volume is small. The value is a clear contract per layer, not more physical copies. Extra layers that no one owns only add cost and latency.

| Support area | Purpose |
| --- | --- |
| Quarantine (`rejected/`) | Invalid records with the rule that failed |
| Archive | Cold raw history under lifecycle rules |
| Manifests | Which files make up a completed batch |
| Checkpoints | Streaming recovery state |

**Interview: why keep Bronze if Silver exists?** Bronze lets you replay after a logic defect without re-extracting from the source, supports audits and regulatory queries, absorbs schema evolution, and makes recovery after downstream failure a recompute instead of an incident with the source team.

## 1.3 ELT versus ETL, warehouse versus lakehouse

| Choice | Use when | Avoid when |
| --- | --- | --- |
| ELT in Snowflake with dbt | SQL-expressible logic, analyst-owned models, fast iteration | Heavy non-SQL processing (images, custom parsers) |
| ETL in Spark before load | Large-scale cleansing, parsing, joins before warehouse, cost-sensitive heavy transforms | Simple relational transforms (adds an operational layer) |
| Warehouse-centric (Snowflake) | BI, governed analytics, concurrency, simplicity | Open-format multi-engine access is a hard requirement |
| Lakehouse / Iceberg | Several engines (Spark, Trino, Snowflake) must share tables | Small team that needs one engine |

## 1.4 Common challenges at the architecture level

| Challenge | Mitigation |
| --- | --- |
| Layer sprawl and duplicated logic | One owner per layer, contracts, lineage via dbt |
| Cost of copying data between layers | Views and incremental models where latency allows |
| Cross-cloud (S3 plus GCS) egress and security | Keep Snowflake in the cloud where most data lands; evaluate egress before hybrid |
| Streaming and batch giving different numbers | One event schema, one ledger, daily reconciliation between paths |
| Unclear ownership of KPI definitions | Semantic layer with named owners |

---

# Chapter 2. Batch, Streaming and CDC Processing

## 2.1 Processing models

| Model | Latency | Examples | Tools | Choose when |
| --- | --- | --- | --- | --- |
| Batch | Minutes to hours | Daily ERP load, month-end finance | Airflow, PySpark, dbt | Correctness and cost matter more than freshness |
| Micro-batch | Seconds to minutes | Orders every 5 minutes | Snowpipe, Spark Structured Streaming, tasks | Near-real-time dashboards |
| Streaming | Sub-second to seconds | Fraud alerts, inventory | Kafka/Pub/Sub with Flink, Dataflow, Spark | Action must happen before a batch could run |
| CDC | Seconds to minutes | Operational DB replicated to analytics | Debezium, Datastream | You need inserts, updates and deletes with ordering |

A useful rule: do not build streaming unless a business decision actually depends on data being fresher than minutes. Micro-batch covers most BI needs at a fraction of the operational burden.

## 2.2 Streaming vocabulary

| Term | Meaning |
| --- | --- |
| Producer / consumer | Writes / reads events |
| Topic and partition | Logical stream and its unit of parallelism and ordering |
| Offset | Position within a partition |
| Consumer group | Set of consumers that divide partitions |
| Checkpoint | Persisted progress and state for recovery |
| Watermark | Estimate of event-time completeness, used to bound late data |
| Window | Grouping of events by event time |
| Dead-letter queue (DLQ) | Where unprocessable events go for review |

## 2.3 Event time, arrival time and processing time

| Moment | Example |
| --- | --- |
| Source commit (event) time | 10:00, customer changes city |
| Arrival time | 10:04, event reaches Kafka |
| Processing time | 10:06, Spark handles it |

Historical dimensions must be built from event time and source position. Arrival time is only for measuring latency and for incremental watermarks, never for deciding which change is newer.

**Interview: a 10:00 event arrives at 14:00.** For current-state processing, compare the source position with what the target already holds and apply only if newer. For historical processing (SCD2), insert it at its correct position and recompute validity intervals for that key. For windowed aggregates, apply it if within allowed lateness; beyond that, route to a late-data recovery path or reprocess from the ledger.

## 2.4 Change data capture methods

| Method | How | Pros | Cons |
| --- | --- | --- | --- |
| Log-based (Debezium, Datastream, GoldenGate) | Read the database transaction log | Captures deletes, ordering, low source load | Needs log access, connector operations |
| Query/timestamp based | `WHERE updated_at > watermark` | Simple, no log access | Misses hard deletes, depends on reliable timestamps, same-timestamp ties |
| Trigger based | DB triggers write to a change table | Works without log access | Adds load and write latency to the source |
| Transactional outbox | App writes events to an outbox table in the same transaction | Business-level events, no dual-write | Requires application changes |
| Snapshot diff | Compare full extracts | Works for any source | Expensive, no intermediate changes |

## 2.5 CDC event contract

```json
{
  "event_id": "evt-10001",
  "source_system": "ORACLE",
  "table_name": "CUSTOMER",
  "business_key": "101",
  "operation": "UPDATE",
  "before": {"city": "Kolkata"},
  "after": {"city": "Delhi"},
  "source_commit_ts": "2026-11-09T10:30:00Z",
  "source_sequence": "000001250",
  "transaction_id": "TXN-9001",
  "schema_version": 1,
  "ingestion_ts": "2026-11-09T10:31:00Z"
}
```

Ordering key: `(source_commit_ts, source_sequence, event_id)`. Timestamps can tie because of precision or because many rows commit in one transaction. The sequence (Oracle SCN, PostgreSQL LSN, SQL Server LSN, or the connector offset) is the authority; compare it numerically or as a fixed-width string, never as an unpadded string, and never across unrelated sources.

## 2.6 Delivery guarantees and what they mean for design

| Guarantee | Meaning | Consequence |
| --- | --- | --- |
| At-most-once | May lose events | Unacceptable for business data |
| At-least-once | May duplicate events | Default for Kafka, Pub/Sub: deduplicate on `event_id` |
| Exactly-once (effect) | At-least-once delivery plus idempotent write | Achieved with deterministic keys, `MERGE`, load history and transactional commits |

## 2.7 Handling late, duplicated and out-of-order events

| Problem | Standard answer |
| --- | --- |
| Duplicate delivery | De-duplicate by `(source_system, event_id)` only |
| Distinct changes for the same key | Keep all; order by source position |
| Out-of-order arrival | Ledger plus ordered recomputation or guarded merge |
| Late arrival beyond watermark | Never drop; persist to ledger and reprocess affected keys |
| Delete before older update | Tombstone wins by position (chapter 16, S1) |
| Poison message | DLQ with error metadata, alert, replay after fix |

---

# Chapter 3. Google Cloud Storage Data Lake Engineering

## 3.1 Storage classes and when to use them

| Class | Use | Watch out for |
| --- | --- | --- |
| Standard | Hot Bronze/Silver read by pipelines | Highest storage price |
| Nearline | Data read about monthly | 30-day minimum duration, retrieval fee |
| Coldline | Data read about quarterly | 90-day minimum, retrieval fee |
| Archive | Compliance retention, rarely read | 365-day minimum, highest retrieval cost |

Choose by access pattern, retrieval cost, minimum storage duration and recovery needs. Lifecycle transitions are not backups.

## 3.2 Bucket layout and partitioning

```text
gs://enterprise-data-lake/
  bronze/<source>/<table>/batch_id=<id>/part-00001.json
  silver/<domain>/<table>/ingest_date=2026-11-09/part-00001.parquet
  cdc/<table>/epoch=<n>/...
  rejected/<source>/batch_id=<id>/
  manifests/<source>/<batch_id>.json
  checkpoints/<job>/
  archive/
```

Partition by ingestion date for operational replay and keep source event time as a column. Never partition by a high-cardinality key such as `customer_id`. Target files of 128 MB to 1 GB.

## 3.3 Lifecycle configuration

```json
{
  "rule": [
    {"action": {"type": "SetStorageClass", "storageClass": "NEARLINE"},
     "condition": {"age": 90, "matchesPrefix": ["bronze/"]}},
    {"action": {"type": "SetStorageClass", "storageClass": "ARCHIVE"},
     "condition": {"age": 365, "matchesPrefix": ["archive/"]}}
  ]
}
```

```bash
gcloud storage buckets update gs://enterprise-data-lake --lifecycle-file=lifecycle.json
```

Validate against your retention policy first. Moving Bronze to Nearline is only sensible if replays are rare, because replay reads incur retrieval fees.

## 3.4 Manifests and the commit protocol

Object stores have no atomic multi-file commit and no atomic directory rename. Make a batch visible only through a manifest written last:

```json
{"batch_id": "orders_20261109_001", "source": "ERP", "record_count": 500000,
 "files": ["bronze/orders/batch_id=.../part-00001.json"], "status": "COMPLETED"}
```

Benefits: file completeness, duplicate batch prevention, reprocessing, count reconciliation and audit. Consumers ignore any prefix that has no manifest. Implementation with `if_generation_match=0` is in chapter 9.4.

## 3.5 Protection

| Control | Purpose |
| --- | --- |
| Uniform bucket-level access, public access prevention | No object ACL surprises |
| Per-prefix IAM conditions | Ingestion writes Bronze only; Snowflake reads Silver only |
| Versioning and retention policies | Recovery from overwrite or delete, regulatory holds |
| CMEK, audit logs | Key control and evidence |
| Separate buckets or projects per environment | Blast-radius control |

## 3.6 Challenges and answers

| Challenge | Answer |
| --- | --- |
| Spark job fails after writing 5 of 10 files | Write to a staging prefix, publish via manifest; consumers read only committed batches |
| Small-file problem | Tune output partitions, compact on a schedule to a new prefix, never force a single partition |
| Listing is slow and costly | Fewer larger files, date partitions, explicit file lists from manifests |
| Duplicate batch objects on retry | Deterministic object names plus create-only writes |
| Cross-region egress cost | Co-locate buckets, Snowflake account and compute in the same region |

---

# Chapter 4. Snowflake Core: Architecture and SQL Engineering

## 4.1 Architecture

| Layer | Responsibility |
| --- | --- |
| Cloud services | Authentication, metadata, query optimization, transactions, access control |
| Compute | Virtual warehouses |
| Storage | Compressed columnar micro-partitions with per-column metadata |

Storage and compute are separate, so ETL, BI and data-science workloads use different warehouses on the same data without copies or contention.

| Warehouse size | Relative credits/hour |
| --- | --- |
| X-Small | 1 |
| Small | 2 |
| Medium | 4 |
| Large | 8 |
| X-Large | 16 |
| 2X-Large | 32 |

Doubling size doubles the credit rate but does not always halve runtime. Optimise SQL before scaling.

## 4.2 Table types

| Type | Time Travel | Fail-safe | Use |
| --- | --- | --- | --- |
| Permanent | Up to 90 days (Enterprise), default 1 | 7 days | Production data |
| Transient | 0 or 1 day | None | Rebuildable staging and intermediate |
| Temporary | 0 or 1 day, session only | None | Session scratch |
| External table | n/a | n/a | Query files in place in GCS (slower, no DML) |
| Dynamic table | Per its settings | Per type | Declarative, auto-refreshed transformation |
| Iceberg table | Per catalog | n/a | Open format shared with other engines |

## 4.3 GCS integration: storage integration, stage, file format, COPY

```sql
CREATE STORAGE INTEGRATION GCS_INTEGRATION
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = GCS
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://enterprise-data-lake/silver/');

DESC STORAGE INTEGRATION GCS_INTEGRATION;   -- note STORAGE_GCP_SERVICE_ACCOUNT, grant it read on the bucket prefix

CREATE FILE FORMAT FF_PARQUET TYPE = PARQUET;
CREATE FILE FORMAT FF_NDJSON  TYPE = JSON STRIP_OUTER_ARRAY = FALSE;

CREATE OR REPLACE STAGE GCS_ORDERS_STAGE
  URL = 'gcs://enterprise-data-lake/silver/orders/'
  STORAGE_INTEGRATION = GCS_INTEGRATION
  FILE_FORMAT = FF_PARQUET;

LIST @GCS_ORDERS_STAGE;

COPY INTO RAW.RAW_ORDERS (order_id, customer_id, order_ts, order_amount, status, source_file, ingested_at)
FROM (
  SELECT $1:order_id::NUMBER, $1:customer_id::NUMBER, $1:order_ts::TIMESTAMP_NTZ,
         $1:order_amount::NUMBER(18,2), $1:status::VARCHAR,
         METADATA$FILENAME, CURRENT_TIMESTAMP()
  FROM @GCS_ORDERS_STAGE)
FILE_FORMAT = (FORMAT_NAME = FF_PARQUET)
ON_ERROR = 'ABORT_STATEMENT';
```

Snowflake uses `gcs://` in stage and integration URLs, while Spark and `gcloud` use `gs://`. Never embed service-account keys: the integration generates the identity you authorise. Validate the Parquet field names and types before deploying.

## 4.4 Window functions (with the portable rolling window)

```sql
-- latest record per key
SELECT * FROM LANDING.LND_CUSTOMER
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY source_commit_ts DESC, source_sequence DESC, event_id DESC) = 1;

-- previous value and delta
SELECT customer_id, transaction_date, amount,
       LAG(amount) OVER (PARTITION BY customer_id ORDER BY transaction_date) AS prev_amount,
       amount - LAG(amount) OVER (PARTITION BY customer_id ORDER BY transaction_date) AS delta
FROM CUSTOMER_TRANSACTIONS;

-- running total
SELECT customer_id, transaction_date, amount,
       SUM(amount) OVER (PARTITION BY customer_id ORDER BY transaction_date, txn_id
                         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
FROM CUSTOMER_TRANSACTIONS;

-- rolling 30 days over a calendar spine (handles missing days)
WITH spine AS (
  SELECT DATEADD('day', SEQ4(), '2026-01-01'::DATE) AS d FROM TABLE(GENERATOR(ROWCOUNT => 365))),
daily AS (SELECT order_date, SUM(order_amount) AS rev FROM SCHEMATIZED.FACT_ORDERS GROUP BY 1)
SELECT s.d, COALESCE(a.rev, 0) AS rev,
       SUM(COALESCE(a.rev, 0)) OVER (ORDER BY s.d ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS rolling_30
FROM spine s LEFT JOIN daily a ON a.order_date = s.d;
```

| Function family | Use |
| --- | --- |
| `ROW_NUMBER`, `RANK`, `DENSE_RANK` | Dedup, top-N, ties handling |
| `LAG`, `LEAD` | Change detection, timelines |
| `FIRST_VALUE`, `LAST_VALUE` | Boundary values (mind the frame clause) |
| `SUM/COUNT/AVG OVER` | Running and moving aggregates |
| `NTILE`, `PERCENT_RANK` | Bucketing, percentiles |

## 4.5 Production MERGE (current-state, SCD1)

```sql
CREATE OR REPLACE TEMP TABLE STG_CUSTOMER_LATEST AS
SELECT * FROM STG_CUSTOMER_CDC
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id
        ORDER BY source_commit_ts DESC, source_sequence DESC, event_id DESC) = 1;

MERGE INTO CUSTOMER_CURRENT t
USING STG_CUSTOMER_LATEST s ON t.customer_id = s.customer_id
WHEN MATCHED AND s.operation = 'DELETE'
     AND (s.source_commit_ts > t.source_commit_ts
          OR (s.source_commit_ts = t.source_commit_ts AND s.source_sequence > t.source_sequence))
  THEN DELETE
WHEN MATCHED AND s.operation <> 'DELETE'
     AND (s.source_commit_ts > t.source_commit_ts
          OR (s.source_commit_ts = t.source_commit_ts AND s.source_sequence > t.source_sequence))
  THEN UPDATE SET t.city = s.city, t.source_commit_ts = s.source_commit_ts, t.source_sequence = s.source_sequence
WHEN NOT MATCHED AND s.operation <> 'DELETE'
  THEN INSERT (customer_id, city, source_commit_ts, source_sequence)
       VALUES (s.customer_id, s.city, s.source_commit_ts, s.source_sequence);
```

A MERGE source must contain at most one row per target key. The version guard prevents a stale event from overwriting a newer state. Deletes for keys that are not in the target need a tombstone table (chapter 16, S1). Treat concurrent writers as serialised.

## 4.6 Semi-structured data

```sql
SELECT r.payload:order_id::NUMBER AS order_id, li.value:sku::STRING AS sku
FROM RAW.RAW_ORDERS_JSON r, LATERAL FLATTEN(INPUT => r.payload:line_items, OUTER => TRUE) li;
```

Store the raw payload in a `VARIANT` column in RAW and extract typed columns downstream with `TRY_` casts. Do not leave production marts as VARIANT.

## 4.7 Time Travel, Fail-safe and zero-copy cloning

```sql
SELECT * FROM CUSTOMER_CURRENT AT (OFFSET => -3600);
UNDROP TABLE CUSTOMER_CURRENT;
CREATE TABLE CUSTOMER_RECOVERY CLONE CUSTOMER_CURRENT AT (TIMESTAMP => '2026-11-09 10:00:00'::TIMESTAMP_NTZ);
CREATE TABLE DEV_CUSTOMER CLONE PROD_CUSTOMER;
```

Time Travel is user-accessible history within retention. Fail-safe is a Snowflake-only 7-day window after Time Travel for permanent tables, not a backup you can query. A clone shares micro-partitions at creation and diverges as either side changes, so it is cheap to create but not free to keep.

## 4.8 Challenges and when to use what

| Situation | Choice |
| --- | --- |
| Rebuildable intermediate data | Transient table |
| Need to test a migration on production-sized data | Zero-copy clone |
| Query files in GCS rarely | External table; otherwise load into native tables |
| Share with partner without copying | Secure share (chapter 18.7) |
| Large VARIANT used for BI | Extract typed columns in a downstream model |

---

# Chapter 5. Snowflake Performance Optimization and Administration

## 5.1 Diagnose before tuning

| Query Profile signal | Meaning | First action |
| --- | --- | --- |
| Partitions scanned close to total | Poor pruning | Range predicates on raw columns, better load order, clustering |
| Large bytes scanned for few rows returned | Missing filter or wide scans | Select needed columns, push filters earlier |
| Local spill | Memory pressure on an operation | Reduce data per operation, then consider a larger warehouse |
| Remote spill | Severe memory pressure | Fix join/aggregate first; size up if justified |
| High queued overload time | Concurrency, not slow SQL | Multi-cluster or separate warehouses |
| Exploding join (rows out much greater than in) | Missing or non-unique join keys | Fix cardinality, de-duplicate inputs |
| Long compilation time | Very complex or deeply nested views | Simplify, materialise intermediate results |

```sql
SELECT query_id, warehouse_name, total_elapsed_time, execution_time, queued_overload_time,
       bytes_scanned, partitions_scanned, partitions_total,
       bytes_spilled_to_local_storage, bytes_spilled_to_remote_storage
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD('day', -1, CURRENT_TIMESTAMP())
ORDER BY total_elapsed_time DESC LIMIT 50;
```

`ACCOUNT_USAGE` has ingestion latency (up to a few hours for some views). For live troubleshooting use `INFORMATION_SCHEMA.QUERY_HISTORY()` or Snowsight. Pruning ratio is `1 - partitions_scanned / partitions_total`, a diagnostic and not a target.

## 5.2 Pruning-friendly SQL

```sql
-- bad: function on the filter column defeats pruning
WHERE TO_CHAR(order_date, 'YYYY-MM') = '2026-01'
-- good
WHERE order_date >= '2026-01-01' AND order_date < '2026-02-01'
```

Also avoid implicit casts on filter columns, `SELECT *` in transformations, and `ORDER BY` without `LIMIT` on large results.

## 5.3 Scale up versus scale out

| Problem | Fix |
| --- | --- |
| One heavy query, spilling or slow | Fix SQL; if still needed, increase warehouse size |
| Many queries waiting | Multi-cluster warehouse (`MAX_CLUSTER_COUNT`), or isolate workloads |
| ETL slows dashboards | Separate ETL and BI warehouses |
| Idle credits | Lower `AUTO_SUSPEND` (60 s for ETL, longer only if cache reuse matters) |

```sql
CREATE OR REPLACE WAREHOUSE BI_WH WITH
  WAREHOUSE_SIZE = 'MEDIUM' MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 4
  SCALING_POLICY = 'STANDARD' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;
```

`STANDARD` starts clusters quickly to avoid queuing; `ECONOMY` waits longer to keep clusters fully loaded and saves credits when some queueing is acceptable.

## 5.4 Clustering, Search Optimization, Materialized Views

| Feature | Best for | Cost and caveats |
| --- | --- | --- |
| Clustering key | Large tables filtered by range on a few columns, poor natural order | Automatic reclustering credits; high-churn tables get expensive |
| Search Optimization Service | Selective point lookups, equality/IN/substring, VARIANT paths on big tables | Storage plus maintenance credits; no value for broad scans |
| Materialized view | Repeated supported aggregates on a single table | Restricted SQL; maintenance credits |
| Dynamic table | Multi-step derived data with a freshness target | Refresh credits; check it refreshes incrementally |
| Result cache | Identical repeated queries | Free, but only if data and query text unchanged |

```sql
ALTER TABLE SALES_FACT CLUSTER BY (order_date, region_id);
SELECT SYSTEM$CLUSTERING_INFORMATION('SALES_FACT', '(order_date, region_id)');
ALTER TABLE CUSTOMER_SUPPORT ADD SEARCH OPTIMIZATION ON EQUALITY(email);
```

Put the lowest-cardinality, most-filtered column first in a clustering key, and measure before and after. Clustering is not mandatory for every large table.

## 5.5 Caching

| Cache | Behavior |
| --- | --- |
| Result cache | Reuses results for 24 hours when text, role context and data are unchanged and no non-deterministic function is used |
| Warehouse local cache | Keeps scanned data on SSD; lost when the warehouse suspends |
| Metadata cache | Answers `COUNT(*)`, `MIN/MAX` on clustered columns without scanning |

## 5.6 Governance of cost and capacity

```sql
CREATE RESOURCE MONITOR RM_ETL WITH CREDIT_QUOTA = 500 FREQUENCY = MONTHLY START_TIMESTAMP = IMMEDIATELY
  TRIGGERS ON 80 PERCENT DO NOTIFY ON 100 PERCENT DO SUSPEND;
ALTER WAREHOUSE ETL_WH SET RESOURCE_MONITOR = RM_ETL;
ALTER WAREHOUSE ETL_WH SET STATEMENT_TIMEOUT_IN_SECONDS = 7200;
```

Design rule: one warehouse per workload class (ingest, transform, BI, data science, ad hoc), with a resource monitor and a statement timeout for each. Tag queries (`QUERY_TAG`) for cost attribution.

## 5.7 When to use what: Snowflake performance

| Symptom | Likely tool |
| --- | --- |
| Dashboard filters by date range on a 5 TB table | Clustering on date |
| Support lookup by email in 2 billion rows | Search optimization |
| Same KPI query run by 200 users | Marts and result cache, multi-cluster BI warehouse |
| MERGE slow on big target | Dedup source, pruning predicate on target, cluster on merge key |
| Month-end spike | Temporary larger or multi-cluster warehouse with auto-suspend |

---

# Chapter 6. Snowpipe, Streams and Tasks

## 6.1 Snowpipe (file-based continuous ingestion from GCS)

```text
GCS bucket -> OBJECT_FINALIZE notification -> Pub/Sub topic -> subscription
   -> Snowflake notification integration -> Snowpipe -> COPY INTO RAW table
```

```sql
CREATE NOTIFICATION INTEGRATION GCS_NOTIFICATION_INTEGRATION
  TYPE = QUEUE NOTIFICATION_PROVIDER = GCP_PUBSUB ENABLED = TRUE
  GCP_PUBSUB_SUBSCRIPTION_NAME = 'projects/my-project/subscriptions/snowpipe-sub';

CREATE OR REPLACE PIPE RAW.ORDERS_PIPE AUTO_INGEST = TRUE INTEGRATION = 'GCS_NOTIFICATION_INTEGRATION'
AS COPY INTO RAW.RAW_ORDERS FROM @GCS_ORDERS_STAGE FILE_FORMAT = (FORMAT_NAME = FF_PARQUET)
   MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
```

Creating the pipe alone does not complete the flow: the Pub/Sub topic, bucket notification, subscription and IAM for the Snowflake-generated service account must exist (chapter 17.6).

| Option | Latency | Source | Use when |
| --- | --- | --- | --- |
| Scheduled `COPY INTO` | Minutes to hours | Files | Batch loads orchestrated by Airflow |
| Snowpipe | About a minute | Files in GCS | Continuous file drops, micro-batch |
| Snowpipe Streaming / Kafka connector | Seconds | Rows from a client or Kafka | Low latency without landing files |

File size guidance: aim for roughly 100 to 250 MB compressed per file; many tiny files raise Snowpipe overhead.

## 6.2 Streams (change tracking on a table)

```sql
CREATE OR REPLACE STREAM CUSTOMER_STREAM ON TABLE RAW.CUSTOMER;          -- standard
CREATE OR REPLACE STREAM CUSTOMER_APPEND ON TABLE RAW.CUSTOMER_CDC_LEDGER APPEND_ONLY = TRUE;
```

| Fact | Consequence |
| --- | --- |
| Metadata columns: `METADATA$ACTION`, `METADATA$ISUPDATE`, `METADATA$ROW_ID` | Read these to interpret changes |
| A standard stream shows an UPDATE as a DELETE row plus an INSERT row with `ISUPDATE = TRUE` | A MERGE must collapse them per key; otherwise duplicates |
| Standard streams show the net change since the offset | Multiple edits to one row between reads appear as one change |
| Append-only streams show inserts only | Ideal over an insert-only CDC ledger |
| The offset advances only when a DML statement that reads the stream commits | A `SELECT` does not consume; `CREATE TABLE AS SELECT` and `INSERT/MERGE ... FROM stream` do |
| A stream goes stale if not consumed within the table's retention window | A stale stream must be recreated, losing unconsumed changes |

Streams track table changes, not business-event history. For full event history keep an immutable ledger table.

## 6.3 Tasks

```sql
CREATE OR REPLACE TASK LOAD_CUSTOMER_CURRENT
  WAREHOUSE = ETL_WH
  SCHEDULE = '2 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('CUSTOMER_APPEND')
  ALLOW_OVERLAPPING_EXECUTION = FALSE
  SUSPEND_TASK_AFTER_NUM_FAILURES = 3
AS CALL AUDIT.APPLY_CUSTOMER_CHANGES();

ALTER TASK LOAD_CUSTOMER_CURRENT RESUME;
```

The corrected, complete merge that the task should execute is in chapter 18.1. Task graphs chain tasks with `AFTER`; only the root has a schedule. Serverless tasks (omit `WAREHOUSE`, set an initial size) avoid managing a warehouse but bill separately. Monitor with `INFORMATION_SCHEMA.TASK_HISTORY()`.

## 6.4 When to use what: change processing in Snowflake

| Need | Choice | Reason |
| --- | --- | --- |
| Files from GCS, continuous | Snowpipe | Managed, serverless |
| Apply ordered CDC to a current-state table | Append-only stream on ledger + task + guarded MERGE | Source-position ordering and replay safety |
| Keep an aggregate fresh | Dynamic table | Declarative lag target |
| Complex SCD2 and tests, team uses Git | dbt incremental | Version control, tests, CI |
| Cross-system dependencies and retries | Airflow | Snowflake tasks only see Snowflake |

## 6.5 Challenges

| Challenge | Handling |
| --- | --- |
| Stale stream | Alert on time since last consumption; keep task running; extend retention if needed |
| Task overlap and double processing | `ALLOW_OVERLAPPING_EXECUTION = FALSE`, idempotent merge |
| Pipe silently stops | `SYSTEM$PIPE_STATUS`, Pub/Sub backlog metric, IAM changes |
| Duplicate loads after re-landing files | Deterministic file names, `COPY` load history (64 days), `FORCE = FALSE` |
| Poison file | Load with `ON_ERROR = ABORT_STATEMENT` for critical feeds, quarantine the file, replay after fix |

# Chapter 6-7. dbt Engineering and Incremental Processing (Completion)

## 7.1 Source freshness and tests on sources

```yaml
version: 2

sources:
  - name: raw_erp
    database: RAW_DB
    schema: ERP
    loaded_at_field: INGESTION_TS
    freshness:
      warn_after: {count: 2, period: hour}
      error_after: {count: 6, period: hour}
    tables:
      - name: customer_cdc
        columns:
          - name: event_id
            data_tests: [not_null]
          - name: operation
            data_tests:
              - accepted_values:
                  arguments:
                    values: ['INSERT', 'UPDATE', 'DELETE']
```

Run `dbt source freshness` before `dbt build` in production so that stale sources fail fast instead of producing stale marts. Note: on dbt versions before 1.10, `accepted_values` takes `values:` directly without the `arguments:` wrapper.

## 7.2 Staging model: deterministic, replay-safe

Staging models should only rename, cast, trim and de-duplicate delivery duplicates. They must not drop distinct events for the same key.

```sql
-- models/staging/stg_customer_cdc_ledger.sql
{{ config(materialized='view') }}

WITH src AS (
    SELECT * FROM {{ source('raw_erp', 'customer_cdc') }}
),

delivery_dedup AS (
    SELECT *
    FROM src
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY source_system, event_id
        ORDER BY ingestion_ts DESC
    ) = 1
)

SELECT
    source_system,
    event_id,
    TRY_TO_NUMBER(customer_id)                  AS customer_id,
    UPPER(operation)                            AS operation,
    NULLIF(TRIM(customer_name), '')             AS customer_name,
    UPPER(NULLIF(TRIM(city), ''))               AS city,
    source_commit_ts,
    source_sequence,
    ingestion_ts,
    MD5(CONCAT_WS('||',
        COALESCE(customer_name, '~null~'),
        COALESCE(city, '~null~')
    ))                                          AS record_hash
FROM delivery_dedup
WHERE customer_id IS NOT NULL
```

Rows with a null key are not silently dropped in a mature platform. Route them to a rejected-records model (see chapter 12.2).

## 7.3 Custom SCD2 from a CDC ledger (late-arrival safe)

Use this instead of `dbt snapshot` when you hold the full event history and must reconstruct timelines. Strategy: find the keys touched by new events, rebuild each touched key's full timeline from the ledger, and replace those keys with `delete+insert`.

```sql
-- models/marts/core/dim_customer_scd2.sql
{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='customer_id',
        on_schema_change='append_new_columns'
    )
}}

WITH touched_keys AS (
    SELECT DISTINCT customer_id
    FROM {{ ref('stg_customer_cdc_ledger') }}
    {% if is_incremental() %}
    WHERE ingestion_ts > (
        SELECT COALESCE(MAX(last_loaded_at), '1900-01-01'::TIMESTAMP_NTZ)
        FROM {{ this }}
    )
    {% endif %}
),

ledger AS (
    SELECT l.*
    FROM {{ ref('stg_customer_cdc_ledger') }} l
    JOIN touched_keys k USING (customer_id)
),

ordered AS (
    SELECT
        *,
        LAG(record_hash) OVER (
            PARTITION BY customer_id
            ORDER BY source_commit_ts, source_sequence, event_id
        ) AS prev_hash,
        LAG(operation) OVER (
            PARTITION BY customer_id
            ORDER BY source_commit_ts, source_sequence, event_id
        ) AS prev_operation
    FROM ledger
),

versions AS (
    -- keep only events that change state (drop no-op updates)
    SELECT *
    FROM ordered
    WHERE prev_hash IS NULL
       OR prev_hash <> record_hash
       OR operation = 'DELETE'
       OR prev_operation = 'DELETE'
),

timeline AS (
    SELECT
        customer_id,
        customer_name,
        city,
        operation = 'DELETE'            AS is_deleted,
        record_hash,
        source_commit_ts                AS valid_from,
        COALESCE(
            LEAD(source_commit_ts) OVER (
                PARTITION BY customer_id
                ORDER BY source_commit_ts, source_sequence, event_id
            ),
            '9999-12-31'::TIMESTAMP_NTZ
        )                               AS valid_to,
        ingestion_ts                    AS last_loaded_at
    FROM versions
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'valid_from']) }} AS customer_sk,
    customer_id,
    customer_name,
    city,
    is_deleted,
    record_hash,
    valid_from,
    valid_to,
    (valid_to = '9999-12-31'::TIMESTAMP_NTZ) AS is_current,
    last_loaded_at
FROM timeline
```

Why this design is correct for interviews:

- A late event only changes the keys it touches, and the whole timeline for those keys is recomputed, so overlaps and gaps cannot occur.
- `delete+insert` on `customer_id` is idempotent: re-running the same events yields the same rows.
- Deletes are explicit rows (`is_deleted = TRUE`), so an older update cannot resurrect a deleted customer, because ordering is by source position, not arrival.
- The watermark uses `ingestion_ts` (arrival), while the ordering uses source position. Never mix the two roles.

One caveat: the `last_loaded_at` stored per version is that event's arrival time, so the watermark is the maximum over the table. If a backfill loads events whose `ingestion_ts` is older than the current maximum, use a full-refresh of the affected keys or a separate replay model. Document this.

### Tests for the SCD2 model

```yaml
models:
  - name: dim_customer_scd2
    columns:
      - name: customer_sk
        data_tests: [unique, not_null]
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          arguments:
            combination_of_columns: [customer_id, valid_from]
```

```sql
-- tests/assert_scd2_one_current_row.sql
SELECT customer_id
FROM {{ ref('dim_customer_scd2') }}
WHERE is_current
GROUP BY customer_id
HAVING COUNT(*) <> 1
```

```sql
-- tests/assert_scd2_no_overlaps.sql
SELECT a.customer_id, a.valid_from, b.valid_from AS overlapping_from
FROM {{ ref('dim_customer_scd2') }} a
JOIN {{ ref('dim_customer_scd2') }} b
  ON a.customer_id = b.customer_id
 AND a.valid_from < b.valid_from
 AND b.valid_from < a.valid_to
```

## 7.4 Microbatch incremental strategy (dbt 1.9+)

Microbatch processes event-time slices independently, which makes backfills and retries cheap and idempotent per batch.

```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='microbatch',
        event_time='order_ts',
        batch_size='day',
        lookback=3,
        begin='2025-01-01'
    )
}}

SELECT order_id, customer_id, order_ts, order_amount, status
FROM {{ ref('stg_orders') }}
```

Upstream models that feed a microbatch model should also declare `event_time` so dbt can filter their inputs per batch. Backfill a window with:

```bash
dbt run --select fct_orders --event-time-start '2026-10-01' --event-time-end '2026-11-01'
```

Use microbatch for large, event-time-partitioned facts. Do not use it for dimensions where the key, not time, determines the replaced rows.

## 7.5 Making Snowflake MERGE incremental models cheaper

A plain `merge` incremental model scans the whole target to find matches. `incremental_predicates` lets Snowflake prune the target:

```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='order_id',
        incremental_predicates=[
            "DBT_INTERNAL_DEST.order_date >= DATEADD('day', -7, CURRENT_DATE())"
        ],
        merge_exclude_columns=['created_at'],
        cluster_by=['order_date']
    )
}}
```

The predicate is only safe if updates cannot arrive for orders older than seven days. Pair it with a periodic full refresh or reconciliation job that catches older changes.

## 7.6 Contracts, versions and schema change policy

```yaml
models:
  - name: dim_customer
    latest_version: 2
    config:
      contract: {enforced: true}
    columns:
      - name: customer_id
        data_type: number
        constraints: [{type: not_null}]
      - name: city
        data_type: varchar
    versions:
      - v: 1
        deprecation_date: 2027-03-31
      - v: 2
        columns:
          - include: all
            exclude: [legacy_flag]
```

| `on_schema_change` | Behavior | Use when |
| --- | --- | --- |
| `ignore` (default) | New source columns are ignored | Strict, contract-governed outputs |
| `append_new_columns` | Adds new columns, does not drop | Raw-adjacent models |
| `sync_all_columns` | Adds and removes columns | Dev only; dangerous in production |
| `fail` | Run errors | Contracts you must never change silently |

## 7.7 Hooks, environment-aware schemas and operations

```sql
-- macros/generate_schema_name.sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if target.name == 'prod' and custom_schema_name is not none -%}
        {{ custom_schema_name | trim | upper }}
    {%- elif custom_schema_name is not none -%}
        {{ target.schema | upper }}_{{ custom_schema_name | trim | upper }}
    {%- else -%}
        {{ target.schema | upper }}
    {%- endif -%}
{%- endmacro %}
```

```yaml
# dbt_project.yml (excerpt)
models:
  my_project:
    marts:
      +schema: MARKETPLACE
      +post-hook:
        - "GRANT SELECT ON {{ this }} TO ROLE ROLE_BI"
      +copy_grants: true
    staging:
      +materialized: view
```

Developers build into `DBT_<USER>_MARKETPLACE`; production builds into `MARKETPLACE`. Without this macro, every developer overwrites shared schemas.

```sql
-- macros/drop_old_dev_schemas.sql
{% macro drop_dev_schema(schema_name) %}
    {% do run_query('DROP SCHEMA IF EXISTS ' ~ target.database ~ '.' ~ schema_name ~ ' CASCADE') %}
    {{ log('Dropped ' ~ schema_name, info=True) }}
{% endmacro %}
```

```bash
dbt run-operation drop_dev_schema --args '{schema_name: DBT_ALICE_MARKETPLACE}'
```

## 7.8 Slim CI with defer

```bash
# 1. Production job publishes manifest.json to GCS after every successful run
gcloud storage cp target/manifest.json gs://company-dbt-artifacts/prod/manifest.json

# 2. CI downloads it and builds only what changed, deferring unchanged parents to prod
mkdir -p prod_state
gcloud storage cp gs://company-dbt-artifacts/prod/manifest.json prod_state/

dbt build \
  --select state:modified+ \
  --defer --state prod_state \
  --target ci \
  --vars '{ci_run_id: pr_1234}'
```

With `--defer`, unchanged upstream models resolve to production relations, so a PR touching one mart does not rebuild the world. The CI target must write to an isolated schema (for example `CI_PR_1234`) that a cleanup job drops.

---

# Chapter 8. SCD, Dimensional Modeling and Fact Correction (Completion)

## 8.1 Point-in-time join from fact to SCD2 dimension

```sql
SELECT
    f.order_id,
    f.order_ts,
    f.order_amount,
    d.customer_sk,
    d.city AS city_at_order_time
FROM {{ ref('stg_orders') }} f
LEFT JOIN {{ ref('dim_customer_scd2') }} d
  ON  f.customer_id = d.customer_id
  AND f.order_ts >= d.valid_from
  AND f.order_ts <  d.valid_to
  AND NOT d.is_deleted
```

The half-open interval `[valid_from, valid_to)` guarantees each order matches exactly one version. Using `BETWEEN` with inclusive upper bound double-counts orders that fall exactly on a change timestamp.

## 8.2 Late-arriving dimension (early-arriving fact) with inferred members

```sql
-- Step 1: create inferred members idempotently
MERGE INTO SCHEMATIZED.DIM_CUSTOMER d
USING (
    SELECT DISTINCT customer_id FROM PREPARE.PREP_ORDERS
) s
  ON d.customer_id = s.customer_id AND d.is_current
WHEN NOT MATCHED THEN INSERT (
    customer_id, customer_name, is_current, is_inferred,
    effective_start_ts, effective_end_ts
) VALUES (
    s.customer_id, 'UNKNOWN', TRUE, TRUE,
    '1900-01-01'::TIMESTAMP_NTZ, '9999-12-31'::TIMESTAMP_NTZ
);

-- Step 2: when the real record arrives, overwrite the inferred row in place (Type 1 for the inferred row only)
UPDATE SCHEMATIZED.DIM_CUSTOMER d
SET customer_name = s.customer_name,
    city = s.city,
    is_inferred = FALSE
FROM PREPARE.PREP_CUSTOMER s
WHERE d.customer_id = s.customer_id
  AND d.is_inferred;
```

The surrogate key does not change, so existing facts remain valid. Facts do not need restatement, which is the entire point of inferred members.

## 8.3 Atomic timeline rebuild for affected keys (SQL script)

```sql
BEGIN TRANSACTION;

CREATE OR REPLACE TEMP TABLE affected AS
SELECT DISTINCT customer_id
FROM RAW.CUSTOMER_CDC_LEDGER
WHERE batch_id = $BATCH_ID;

DELETE FROM SCHEMATIZED.DIM_CUSTOMER_SCD2
WHERE customer_id IN (SELECT customer_id FROM affected);

INSERT INTO SCHEMATIZED.DIM_CUSTOMER_SCD2
    (customer_id, city, effective_start_ts, effective_end_ts, is_current)
WITH ev AS (
    SELECT l.customer_id, l.city, l.source_commit_ts, l.source_sequence, l.event_id
    FROM RAW.CUSTOMER_CDC_LEDGER l
    JOIN affected a USING (customer_id)
    WHERE l.operation IN ('INSERT', 'UPDATE')
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY l.customer_id, l.source_commit_ts, l.source_sequence
        ORDER BY l.event_id DESC) = 1
)
SELECT
    customer_id,
    city,
    source_commit_ts,
    COALESCE(LEAD(source_commit_ts) OVER (
        PARTITION BY customer_id ORDER BY source_commit_ts, source_sequence),
        '9999-12-31'::TIMESTAMP_NTZ),
    LEAD(source_commit_ts) OVER (
        PARTITION BY customer_id ORDER BY source_commit_ts, source_sequence) IS NULL
FROM ev;

COMMIT;
```

Because delete and insert are in one transaction, readers never see a half-built timeline, and a failure rolls back cleanly. Snowflake does not allow concurrent transactions to write the same micro-partitions without lock waiting, so serialize runs per table (Airflow `max_active_runs=1` plus a run lock table).

## 8.4 SCD Type 6 (hybrid) table

```sql
CREATE OR REPLACE TABLE DIM_CUSTOMER_T6 (
    customer_sk        NUMBER AUTOINCREMENT,
    customer_id        NUMBER,
    city_historical    VARCHAR,   -- Type 2: value at that time
    city_current       VARCHAR,   -- Type 1: overwritten on ALL rows of the key
    city_previous      VARCHAR,   -- Type 3: prior value
    valid_from         TIMESTAMP_NTZ,
    valid_to           TIMESTAMP_NTZ,
    is_current         BOOLEAN
);

-- after inserting the new version, refresh Type 1 column across the history
UPDATE DIM_CUSTOMER_T6 t
SET city_current = c.city_historical
FROM DIM_CUSTOMER_T6 c
WHERE t.customer_id = c.customer_id AND c.is_current;
```

BI users can group by `city_historical` (as it was) or `city_current` (as it is now) without writing a self-join. Cost: the Type 1 refresh rewrites every historical row of a key on each change.

## 8.5 Fact correction: reversal versus update

| Approach | How | Use when |
| --- | --- | --- |
| Update in place | Overwrite the fact row | Non-financial facts, no audit requirement |
| Reversal + restate (append-only) | Insert a negative row, then a corrected row | Financial, auditable, regulated facts |

```sql
-- Order 9001 amount corrected from 100.00 to 80.00
INSERT INTO FACT_ORDER_LEDGER (order_id, amount, entry_type, effective_ts, batch_id)
SELECT order_id, -amount, 'REVERSAL', CURRENT_TIMESTAMP(), $BATCH_ID
FROM FACT_ORDER_LEDGER
WHERE order_id = 9001 AND entry_type = 'ORIGINAL'
UNION ALL
SELECT 9001, 80.00, 'RESTATEMENT', CURRENT_TIMESTAMP(), $BATCH_ID;

-- Revenue is always SUM(amount); history stays reproducible as of any date
SELECT SUM(amount) FROM FACT_ORDER_LEDGER WHERE effective_ts <= '2026-10-31';
```

## 8.6 Accumulating snapshot (one row per loan lifecycle)

```sql
MERGE INTO FACT_LOAN_LIFECYCLE t
USING (
    SELECT loan_id,
           MIN(IFF(stage = 'APPLIED',  event_ts, NULL)) AS applied_ts,
           MIN(IFF(stage = 'APPROVED', event_ts, NULL)) AS approved_ts,
           MIN(IFF(stage = 'FUNDED',   event_ts, NULL)) AS funded_ts
    FROM STG_LOAN_EVENTS
    GROUP BY loan_id
) s ON t.loan_id = s.loan_id
WHEN MATCHED THEN UPDATE SET
    t.applied_ts  = COALESCE(t.applied_ts,  s.applied_ts),
    t.approved_ts = COALESCE(t.approved_ts, s.approved_ts),
    t.funded_ts   = COALESCE(t.funded_ts,   s.funded_ts),
    t.days_to_fund = DATEDIFF('day', COALESCE(t.applied_ts, s.applied_ts), COALESCE(t.funded_ts, s.funded_ts))
WHEN NOT MATCHED THEN INSERT (loan_id, applied_ts, approved_ts, funded_ts)
    VALUES (s.loan_id, s.applied_ts, s.approved_ts, s.funded_ts);
```

Accumulating snapshots are the one fact type that is deliberately updated in place, because milestone columns fill in over time.

---

# Chapter 9. Python Production Engineering

## 9.1 Project layout and typed configuration

```text
ingestion/
  pyproject.toml
  src/ingest/
    config.py        # typed settings from env, no secrets in code
    secrets.py       # Secret Manager access
    http.py          # session, retry, pagination, rate limit
    storage.py       # GCS writer with manifests
    watermark.py     # state in Snowflake
    snowflake_io.py  # loading and audit
    validate.py      # schema validation and quarantine
    run.py           # orchestrates one batch
  tests/
```

```python
# config.py
from dataclasses import dataclass
import os

@dataclass(frozen=True)
class Settings:
    project_id: str = os.environ.get('GCP_PROJECT', '')
    bucket: str = os.environ.get('RAW_BUCKET', '')
    source_name: str = os.environ.get('SOURCE_NAME', 'erp_orders')
    api_url: str = os.environ.get('SOURCE_API_URL', '')
    page_size: int = int(os.environ.get('PAGE_SIZE', '1000'))
    sf_account: str = os.environ.get('SF_ACCOUNT', '')
    sf_user: str = os.environ.get('SF_USER', '')
    sf_warehouse: str = os.environ.get('SF_WAREHOUSE', 'ETL_WH')
```

## 9.2 Secrets from Secret Manager (never from code or DAG files)

```python
# secrets.py
from functools import lru_cache
from google.cloud import secretmanager

@lru_cache(maxsize=32)
def get_secret(project_id: str, name: str, version: str = 'latest') -> str:
    client = secretmanager.SecretManagerServiceClient()
    path = f'projects/{project_id}/secrets/{name}/versions/{version}'
    return client.access_secret_version(request={'name': path}).payload.data.decode('utf-8')
```

Run the job under a service account with `roles/secretmanager.secretAccessor` on specific secrets only. Never log the secret value.

## 9.3 HTTP extraction: retries, backoff, pagination, rate limiting

```python
# http.py
import logging, time
from typing import Iterator
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)

def build_session(token: str) -> requests.Session:
    retry = Retry(
        total=5, backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(['GET']),
        respect_retry_after_header=True,
    )
    s = requests.Session()
    s.mount('https://', HTTPAdapter(max_retries=retry))
    s.headers.update({'Authorization': f'Bearer {token}'})
    return s

def paginate(session: requests.Session, url: str, since: str, until: str,
             page_size: int = 1000, max_rps: float = 5.0) -> Iterator[list[dict]]:
    cursor, min_gap = None, 1.0 / max_rps
    while True:
        started = time.monotonic()
        params = {'updated_after': since, 'updated_before': until, 'limit': page_size}
        if cursor:
            params['cursor'] = cursor
        r = session.get(url, params=params, timeout=(5, 60))
        r.raise_for_status()
        body = r.json()
        yield body['data']
        cursor = body.get('next_cursor')
        if not cursor:
            return
        time.sleep(max(0.0, min_gap - (time.monotonic() - started)))
```

A generator keeps memory flat: one page at a time, never the whole dataset. Always pin an `until` bound so the extract window is closed and reproducible.

## 9.4 Atomic, duplicate-safe writes to GCS with a manifest

GCS object writes are atomic per object, but a batch of files is not. Consumers must only trust a batch when its manifest exists.

```python
# storage.py
import json, hashlib
from google.api_core.exceptions import PreconditionFailed
from google.cloud import storage

def batch_id(source: str, start: str, end: str) -> str:
    return hashlib.sha256(f'{source}|{start}|{end}'.encode()).hexdigest()[:20]

def write_batch(bucket_name: str, source: str, bid: str, pages: list[list[dict]]) -> dict:
    bucket = storage.Client().bucket(bucket_name)
    files, total = [], 0
    for i, page in enumerate(pages):
        name = f'bronze/{source}/batch_id={bid}/part-{i:05d}.json'
        body = '\n'.join(json.dumps(r, default=str) for r in page)   # NDJSON
        try:
            # if_generation_match=0 means: create only if it does not exist yet
            bucket.blob(name).upload_from_string(
                body, content_type='application/x-ndjson', if_generation_match=0)
        except PreconditionFailed:
            pass  # same batch re-run: object already written, content is deterministic
        files.append(name)
        total += len(page)
    manifest = {'batch_id': bid, 'source': source, 'record_count': total,
                'files': files, 'status': 'COMPLETED'}
    try:
        bucket.blob(f'manifests/{source}/{bid}.json').upload_from_string(
            json.dumps(manifest), content_type='application/json', if_generation_match=0)
    except PreconditionFailed:
        pass
    return manifest
```

The manifest is written last. A crash before that leaves orphan part files that no consumer will read, and a rerun overwrites nothing and completes the manifest.

## 9.5 Watermarks and run state in Snowflake

```sql
CREATE TABLE IF NOT EXISTS AUDIT.ETL_WATERMARK (
    source_name     VARCHAR,
    high_watermark  TIMESTAMP_NTZ,
    last_batch_id   VARCHAR,
    updated_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (source_name)
);

CREATE TABLE IF NOT EXISTS AUDIT.ETL_RUN_LOG (
    batch_id VARCHAR, source_name VARCHAR, window_start TIMESTAMP_NTZ,
    window_end TIMESTAMP_NTZ, status VARCHAR, record_count NUMBER,
    started_at TIMESTAMP_NTZ, finished_at TIMESTAMP_NTZ, error_message VARCHAR
);
```

```python
# run.py (core control flow)
def run_batch(cfg, sf, source_session):
    start, end = sf.get_watermark(cfg.source_name), sf.now_minus(minutes=5)  # 5-min safety lag
    bid = batch_id(cfg.source_name, str(start), str(end))
    if sf.batch_completed(bid):
        log.info('batch %s already completed, skipping', bid); return
    sf.log_start(bid, cfg.source_name, start, end)
    try:
        pages = list(paginate(source_session, cfg.api_url, str(start), str(end), cfg.page_size))
        manifest = write_batch(cfg.bucket, cfg.source_name, bid, pages)
        sf.copy_into_raw(manifest)                 # COPY is idempotent per file via load history
        sf.advance_watermark(cfg.source_name, end, bid)   # LAST step only
        sf.log_finish(bid, 'SUCCESS', manifest['record_count'])
    except Exception as exc:
        sf.log_finish(bid, 'FAILED', 0, str(exc)); raise
```

The rule that interviewers look for: advance the watermark only after durable success, and make every earlier step safe to repeat.

## 9.6 Loading into Snowflake safely

```python
# snowflake_io.py (key-pair auth, no password)
import snowflake.connector
from cryptography.hazmat.primitives import serialization

def connect(account, user, private_key_pem: bytes, warehouse, role='ROLE_INGESTION'):
    key = serialization.load_pem_private_key(private_key_pem, password=None)
    der = key.private_bytes(
        serialization.Encoding.DER, serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption())
    return snowflake.connector.connect(
        account=account, user=user, private_key=der,
        warehouse=warehouse, role=role, database='ENTERPRISE_DW', schema='RAW')

def copy_into_raw(conn, bid: str, prefix: str):
    sql = f"""
    COPY INTO RAW.RAW_ORDERS_JSON (payload, source_file, batch_id, ingested_at)
    FROM (SELECT $1, METADATA$FILENAME, '{bid}', CURRENT_TIMESTAMP() FROM @GCS_ORDERS_STAGE/{prefix}/)
    FILE_FORMAT = (FORMAT_NAME = FF_NDJSON)
    ON_ERROR = 'ABORT_STATEMENT'
    """
    conn.cursor().execute(sql)
```

`COPY INTO` skips files it already loaded (64-day load metadata), which gives file-level idempotency. Use `ON_ERROR = 'ABORT_STATEMENT'` for critical feeds so bad files fail loudly; use `CONTINUE` only with a rejected-record review step. Bind parameters instead of f-strings for any value that comes from outside your control.

## 9.7 Validation with quarantine

```python
# validate.py
from pydantic import BaseModel, ValidationError, field_validator
from decimal import Decimal
from datetime import datetime

class Order(BaseModel):
    order_id: int
    customer_id: int
    order_ts: datetime
    amount: Decimal
    status: str

    @field_validator('amount')
    @classmethod
    def non_negative(cls, v):
        if v < 0:
            raise ValueError('amount must be >= 0')
        return v

def split_valid_invalid(rows: list[dict]):
    good, bad = [], []
    for r in rows:
        try:
            good.append(Order(**r).model_dump(mode='json'))
        except ValidationError as e:
            bad.append({'record': r, 'errors': e.errors(include_url=False)})
    return good, bad
```

Write `bad` to `gs://.../rejected/<source>/<batch_id>/` and count it in the run log. A pipeline that silently drops invalid rows is a data-loss incident waiting to be discovered.

## 9.8 Testing

```python
# tests/test_batch_id.py
from ingest.storage import batch_id

def test_batch_id_is_deterministic():
    assert batch_id('erp', '2026-11-09', '2026-11-10') == batch_id('erp', '2026-11-09', '2026-11-10')

def test_batch_id_changes_with_window():
    assert batch_id('erp', 'a', 'b') != batch_id('erp', 'a', 'c')
```

```python
# tests/test_validate.py
from ingest.validate import split_valid_invalid

def test_negative_amount_is_quarantined():
    good, bad = split_valid_invalid([
        {'order_id': 1, 'customer_id': 5, 'order_ts': '2026-11-09T10:00:00', 'amount': '10.00', 'status': 'NEW'},
        {'order_id': 2, 'customer_id': 5, 'order_ts': '2026-11-09T10:00:00', 'amount': '-1', 'status': 'NEW'},
    ])
    assert len(good) == 1 and len(bad) == 1
```

Test the control flow (`run_batch`) with fakes for Snowflake and GCS: simulate a crash after `write_batch`, rerun, and assert that no duplicate manifest or double watermark advance occurs.

---

# Chapter 10. PySpark Distributed Computing (Completion)

## 10.1 Session and Dataproc submission

```python
from pyspark.sql import SparkSession

def get_spark(app: str) -> SparkSession:
    return (SparkSession.builder.appName(app)
        .config('spark.sql.adaptive.enabled', 'true')
        .config('spark.sql.adaptive.coalescePartitions.enabled', 'true')
        .config('spark.sql.adaptive.skewJoin.enabled', 'true')
        .config('spark.sql.sources.partitionOverwriteMode', 'dynamic')
        .config('spark.sql.parquet.compression.codec', 'snappy')
        .getOrCreate())
```

```bash
gcloud dataproc batches submit pyspark gs://company-code/jobs/bronze_to_silver.py \
  --region=asia-south1 --version=2.2 \
  --service-account=sa-spark@PROJECT.iam.gserviceaccount.com \
  -- --batch_id=abc123 --run_date=2026-11-09
```

Dataproc Serverless removes cluster sizing; for steady heavy workloads, ephemeral Dataproc clusters created and deleted by Airflow are usually cheaper.

## 10.2 Bronze to Silver: explicit schema, quarantine, ledger preserved

```python
import argparse
from pyspark.sql import functions as F, Window
from pyspark.sql.types import *

SCHEMA = StructType([
    StructField('event_id', StringType(), False),
    StructField('customer_id', LongType(), True),
    StructField('operation', StringType(), True),
    StructField('city', StringType(), True),
    StructField('source_commit_ts', TimestampType(), True),
    StructField('source_sequence', LongType(), True),
    StructField('_corrupt', StringType(), True),
])

def main(batch_id, run_date):
    spark = get_spark('bronze_to_silver_customer')
    raw = (spark.read.schema(SCHEMA)
           .option('mode', 'PERMISSIVE')
           .option('columnNameOfCorruptRecord', '_corrupt')
           .json(f'gs://enterprise-data-lake/bronze/customers/batch_id={batch_id}/'))
    raw = raw.cache()   # needed: reused for valid and invalid branches

    is_valid = (F.col('_corrupt').isNull() & F.col('event_id').isNotNull()
                & F.col('customer_id').isNotNull()
                & F.col('operation').isin('INSERT', 'UPDATE', 'DELETE'))
    bad = raw.filter(~is_valid).withColumn('batch_id', F.lit(batch_id))
    good = raw.filter(is_valid).drop('_corrupt')

    # delivery-duplicate removal ONLY: one row per event_id, all distinct events survive
    good = good.dropDuplicates(['event_id'])
    good = (good.withColumn('ingest_date', F.lit(run_date))
                .withColumn('batch_id', F.lit(batch_id)))

    bad.write.mode('overwrite').json(f'gs://enterprise-data-lake/rejected/customers/batch_id={batch_id}/')
    (good.repartition(8)
         .write.mode('overwrite').partitionBy('ingest_date')
         .parquet('gs://enterprise-data-lake/silver/customer_cdc/'))
    raw.unpersist()

if __name__ == '__main__':
    p = argparse.ArgumentParser(); p.add_argument('--batch_id'); p.add_argument('--run_date')
    a = p.parse_args(); main(a.batch_id, a.run_date)
```

With `partitionOverwriteMode=dynamic`, only the `ingest_date` partitions present in the DataFrame are replaced, so a rerun of one day is idempotent and does not wipe other days.

## 10.3 Skew: diagnose first, then fix

Symptoms in the Spark UI: one task in a stage running far longer than the median, large shuffle read for that task, or executor OOM on a single task.

```python
# 1. Find the hot keys
fact.groupBy('customer_id').count().orderBy(F.desc('count')).show(10)

# 2. Prefer AQE skew-join and a broadcast of the small side where possible
result = fact.join(F.broadcast(small_dim), 'customer_id', 'left')

# 3. If both sides are large and skew remains, salt the hot keys
SALT = 16
hot_keys = [r['customer_id'] for r in fact.groupBy('customer_id').count()
            .filter('count > 5000000').collect()]

fact_s = fact.withColumn('salt',
    F.when(F.col('customer_id').isin(hot_keys), (F.rand() * SALT).cast('int')).otherwise(F.lit(0)))

salts = spark.range(SALT).withColumnRenamed('id', 'salt')
dim_s = (dim.join(salts, F.col('customer_id').isin(hot_keys) | (F.col('salt') == 0), 'inner')
         .filter(F.col('customer_id').isin(hot_keys) | (F.col('salt') == 0)))

result = fact_s.join(dim_s, ['customer_id', 'salt'], 'left').drop('salt')
```

Salting replicates the hot keys' dimension rows SALT times. Only salt the proven hot keys, never the whole table.

## 10.4 Window analytics: sessionization

```python
w = Window.partitionBy('user_id').orderBy('event_ts')
ev = (events
  .withColumn('prev_ts', F.lag('event_ts').over(w))
  .withColumn('gap_min', (F.col('event_ts').cast('long') - F.col('prev_ts').cast('long')) / 60)
  .withColumn('new_session', F.when(F.col('gap_min').isNull() | (F.col('gap_min') > 30), 1).otherwise(0))
  .withColumn('session_no', F.sum('new_session').over(w.rowsBetween(Window.unboundedPreceding, 0)))
  .withColumn('session_id', F.concat_ws('-', 'user_id', 'session_no')))
```

## 10.5 Structured Streaming: Kafka to GCS with idempotent micro-batches

```python
from pyspark.sql.functions import from_json, col

raw = (spark.readStream.format('kafka')
       .option('kafka.bootstrap.servers', 'broker:9092')
       .option('subscribe', 'customer_cdc')
       .option('startingOffsets', 'earliest')
       .option('maxOffsetsPerTrigger', 200000)
       .option('failOnDataLoss', 'true')
       .load())

parsed = (raw.select(from_json(col('value').cast('string'), SCHEMA).alias('e'),
                     col('partition').alias('kafka_partition'),
                     col('offset').alias('kafka_offset'),
                     col('timestamp').alias('kafka_ts'))
          .select('e.*', 'kafka_partition', 'kafka_offset', 'kafka_ts'))

deduped = (parsed
    .withWatermark('source_commit_ts', '2 hours')
    .dropDuplicates(['event_id', 'source_commit_ts']))   # include event-time col so state can expire

def write_batch(df, epoch_id):
    # epoch_id is stable across retries of the same micro-batch: overwrite the same path = idempotent
    (df.write.mode('overwrite')
       .parquet(f'gs://enterprise-data-lake/cdc/customers/epoch={epoch_id}/'))

q = (deduped.writeStream.foreachBatch(write_batch)
     .option('checkpointLocation', 'gs://enterprise-data-lake/checkpoints/customer_cdc/')
     .trigger(processingTime='30 seconds')
     .start())
q.awaitTermination()
```

Key points to explain in an interview:

- The checkpoint stores offsets and state. Deleting it replays from `startingOffsets`; changing the query logic in incompatible ways can make it unrecoverable.
- The watermark bounds the dedup state. Without it `dropDuplicates` state grows forever.
- Exactly-once from Kafka to a file sink depends on idempotent output keyed by `epoch_id`, not on Spark alone. Downstream Snowflake loads must also be idempotent (file-level load history).
- Poison messages: use `PERMISSIVE` parsing and send null-parse rows to a rejected path instead of failing the stream.

## 10.6 Small-file compaction job

```python
df = spark.read.parquet('gs://enterprise-data-lake/silver/orders/order_date=2026-11-08/')
# compute the output file count from input size
# size-based: aim for ~256 MB per file using input byte size from listing
import math
from google.cloud import storage
blobs = storage.Client().list_blobs('enterprise-data-lake', prefix='silver/orders/order_date=2026-11-08/')
total_bytes = sum(b.size for b in blobs)
n = max(1, math.ceil(total_bytes / (256 * 1024 * 1024)))
(df.coalesce(n).write.mode('overwrite')
   .parquet('gs://enterprise-data-lake/silver/orders_compacted/order_date=2026-11-08/'))
```

Write compacted output to a new prefix, validate row counts, then swap the consumer path or table location. Never overwrite the directory you are reading from in the same job.

## 10.7 Tuning cheat sheet

| Symptom | Likely cause | Action |
| --- | --- | --- |
| One straggler task | Key skew | AQE skew join, broadcast, salting of hot keys |
| Executor lost / OOM | Large partitions, wide rows, collect | More partitions, bigger executor memory overhead, avoid `collect()` |
| Thousands of tiny tasks | Too many shuffle partitions or tiny files | Enable AQE coalescing, compact inputs |
| Slow Python stage | Row-at-a-time Python UDF | Native functions, or `pandas_udf` (vectorized) |
| Repeated recomputation | Reused DataFrame not cached | `cache()` only when reused; `unpersist()` after |
| Slow GCS listing | Too many partitions or files | Fewer, larger files; partition by date not by high-cardinality keys |

```python
# Vectorized UDF instead of a row UDF
import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf('string')
def mask_email(s: pd.Series) -> pd.Series:
    return s.str.replace(r'^[^@]+', '***', regex=True)
```

## 10.8 Testing Spark transformations

```python
# tests/test_dedup.py
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder.master('local[2]').config('spark.sql.shuffle.partitions', '2').getOrCreate()

def test_distinct_events_for_same_key_are_kept(spark):
    df = spark.createDataFrame(
        [('e1', 1, 'UPDATE'), ('e1', 1, 'UPDATE'), ('e2', 1, 'UPDATE')],
        ['event_id', 'customer_id', 'operation'])
    out = df.dropDuplicates(['event_id'])
    assert out.count() == 2
```

---

# Chapter 11. Airflow / Cloud Composer Orchestration (Completion)

## 11.1 Principles

| Principle | Meaning |
| --- | --- |
| Idempotent tasks | Re-running a task for the same data interval yields the same result |
| Orchestrate, do not compute | Airflow triggers Snowflake, Dataproc and dbt; it does not process data itself |
| Small XComs | Pass IDs, paths and counts only, never dataframes |
| Logical date vs run time | Use `data_interval_start` / `data_interval_end`, never `datetime.now()` inside tasks |
| One writer per table | `max_active_runs=1` or a pool for tables that serialise writes |

Import paths: Airflow 3 exposes the authoring API in `airflow.sdk`; on Airflow 2.x use `airflow.decorators`. The structure below is identical.

## 11.2 Production DAG: GCS to Snowflake to dbt

```python
from datetime import datetime, timedelta
from airflow.decorators import dag, task, task_group
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowFailException

def alert_on_failure(context):
    ti = context['task_instance']
    # send to Slack/PagerDuty via a connection; include dag, task, run id, log url
    print(f'FAILED {ti.dag_id}.{ti.task_id} run={context["run_id"]} log={ti.log_url}')

@dag(
    dag_id='orders_gcs_to_snowflake',
    start_date=datetime(2026, 1, 1),
    schedule='0 2 * * *',
    catchup=False,
    max_active_runs=1,
    default_args={
        'owner': 'data-eng', 'retries': 3,
        'retry_delay': timedelta(minutes=5), 'retry_exponential_backoff': True,
        'on_failure_callback': alert_on_failure,
        'execution_timeout': timedelta(hours=2),
    },
    sla_miss_callback=None,
    tags=['orders', 'snowflake', 'gcs'],
)
def orders_pipeline():

    wait_manifest = GCSObjectsWithPrefixExistenceSensor(
        task_id='wait_for_manifest',
        bucket='enterprise-data-lake',
        prefix='manifests/orders/{{ ds_nodash }}',
        mode='reschedule', poke_interval=300, timeout=6 * 3600,
        soft_fail=False,
    )

    load_raw = SnowflakeSqlApiOperator(
        task_id='copy_into_raw',
        snowflake_conn_id='snowflake_etl',
        sql="""
            COPY INTO RAW.RAW_ORDERS_JSON (payload, source_file, batch_id, ingested_at)
            FROM (SELECT $1, METADATA$FILENAME, '{{ ds_nodash }}', CURRENT_TIMESTAMP()
                  FROM @GCS_ORDERS_STAGE/batch_date={{ ds }}/)
            FILE_FORMAT = (FORMAT_NAME = FF_NDJSON) ON_ERROR = 'ABORT_STATEMENT'
        """,
    )

    @task
    def reconcile_counts(ds=None):
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        hook = SnowflakeHook(snowflake_conn_id='snowflake_etl')
        raw = hook.get_first("SELECT COUNT(*) FROM RAW.RAW_ORDERS_JSON WHERE batch_id = %s", parameters=(ds.replace('-', ''),))[0]
        expected = hook.get_first("SELECT record_count FROM AUDIT.BATCH_MANIFEST WHERE batch_id = %s", parameters=(ds.replace('-', ''),))[0]
        if raw != expected:
            raise AirflowFailException(f'Count mismatch raw={raw} expected={expected}')
        return {'batch': ds, 'rows': raw}

    dbt_build = BashOperator(
        task_id='dbt_build',
        bash_command='cd /opt/dbt && dbt source freshness --target prod && '
                     'dbt build --select tag:orders --target prod --vars "{run_date: {{ ds }}}"',
        retries=1,
    )

    publish = SQLExecuteQueryOperator(
        task_id='mark_batch_published', conn_id='snowflake_etl',
        sql="UPDATE AUDIT.ETL_RUN_LOG SET status='PUBLISHED' WHERE batch_id = '{{ ds_nodash }}'",
    )

    wait_manifest >> load_raw >> reconcile_counts() >> dbt_build >> publish

orders_pipeline()
```

Notes to defend in an interview:

- `mode='reschedule'` frees the worker slot between pokes. On Composer prefer deferrable operators where available.
- Tasks use templated `{{ ds }}` so a backfill for a past date processes that date, not today.
- `AirflowFailException` fails immediately with no retries, which is right for deterministic data errors; retries are for transient errors.

## 11.3 Dynamic task mapping (one task per table or file)

```python
@dag(schedule='@daily', start_date=datetime(2026, 1, 1), catchup=False)
def multi_table_load():
    @task
    def list_tables():
        return ['customers', 'orders', 'payments', 'products']

    @task(max_active_tis_per_dag=2)   # throttle concurrent Snowflake loads
    def load_table(table: str, ds=None):
        # run COPY INTO for one table / one date
        return f'{table}:{ds}'

    load_table.expand(table=list_tables())

multi_table_load()
```

## 11.4 Data-aware scheduling (datasets / assets)

```python
from airflow.datasets import Dataset   # Airflow 3 renames this concept to Asset

raw_orders = Dataset('snowflake://ENTERPRISE_DW/RAW/RAW_ORDERS')

# producer
load_raw = SQLExecuteQueryOperator(task_id='load', sql='...', conn_id='snowflake_etl', outlets=[raw_orders])

# consumer DAG runs when the producer updates the dataset
@dag(schedule=[raw_orders], start_date=datetime(2026, 1, 1), catchup=False)
def build_marts():
    BashOperator(task_id='dbt', bash_command='dbt build --select marts')
```

## 11.5 Backfills and catchup

```bash
airflow dags backfill orders_gcs_to_snowflake \
  --start-date 2026-10-01 --end-date 2026-10-07 --reset-dagruns
```

Backfills are safe only if every task is idempotent per `data_interval`. Check before you run one: does a rerun for 2026-10-03 overwrite or MERGE that day, or does it append duplicates?

## 11.6 Orchestrating streaming

Do not schedule a continuous Spark stream as a recurring batch task that restarts it daily. Airflow should deploy the job (Dataproc Serverless batch or Dataflow template), monitor health (consumer lag, last checkpoint time), and alert. A sensor on a lag metric, or a `PythonOperator` calling the Cloud Monitoring API, is a reasonable health check task running every few minutes in a small DAG.

---

# Chapter 12. Data Quality, Reconciliation and Contracts (Completion)

## 12.1 Where checks run

| Stage | Check | Tool |
| --- | --- | --- |
| Ingest | File arrived, manifest valid, row count equals manifest | Python / Airflow |
| RAW | Load history clean, no rejected files | Snowflake `COPY_HISTORY` |
| PREPARE | Types, nulls, duplicates, accepted values | dbt tests |
| SCHEMATIZED | Referential integrity, SCD2 integrity | dbt tests |
| MARKETPLACE | KPI reconciliation to source of truth | dbt + reconciliation SQL |
| Continuous | Freshness, volume anomalies | dbt freshness, scheduled SQL |

## 12.2 Rejected-record store

```sql
CREATE TABLE IF NOT EXISTS AUDIT.REJECTED_RECORDS (
    batch_id VARCHAR, source_name VARCHAR, rule_name VARCHAR,
    business_key VARCHAR, record VARIANT, rejected_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO AUDIT.REJECTED_RECORDS (batch_id, source_name, rule_name, business_key, record)
SELECT batch_id, 'ORACLE.CUSTOMER', 'NULL_CUSTOMER_ID', NULL, OBJECT_CONSTRUCT(*)
FROM LANDING.LND_CUSTOMER
WHERE customer_id IS NULL AND batch_id = $BATCH_ID;
```

Do not delete from the target without recording why. Every rejected row must be traceable to a rule and a batch.

## 12.3 Reconciliation by batch with tolerance

```sql
CREATE OR REPLACE PROCEDURE AUDIT.RECONCILE_ORDERS(P_BATCH_ID VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    src_cnt NUMBER; tgt_cnt NUMBER; src_amt NUMBER(18,2); tgt_amt NUMBER(18,2);
BEGIN
    SELECT COUNT(*), COALESCE(SUM(order_amount), 0) INTO :src_cnt, :src_amt
      FROM PREPARE.PREP_ORDERS WHERE batch_id = :P_BATCH_ID;
    SELECT COUNT(*), COALESCE(SUM(order_amount), 0) INTO :tgt_cnt, :tgt_amt
      FROM SCHEMATIZED.FACT_ORDERS WHERE load_batch_id = :P_BATCH_ID;

    INSERT INTO AUDIT.RECONCILIATION_LOG
      (batch_id, check_name, source_value, target_value, status, checked_at)
    VALUES (:P_BATCH_ID, 'ORDERS_COUNT', :src_cnt, :tgt_cnt,
            IFF(:src_cnt = :tgt_cnt, 'PASS', 'FAIL'), CURRENT_TIMESTAMP()),
           (:P_BATCH_ID, 'ORDERS_AMOUNT', :src_amt, :tgt_amt,
            IFF(ABS(:src_amt - :tgt_amt) < 0.01, 'PASS', 'FAIL'), CURRENT_TIMESTAMP());

    RETURN IFF(:src_cnt = :tgt_cnt AND ABS(:src_amt - :tgt_amt) < 0.01, 'PASS', 'FAIL');
END;
$$;
```

Compare like with like. The source side must apply the same filters (excluded statuses, cancelled orders, rejected rows) as the target, and the difference must equal the rejected count when rejections are legitimate.

## 12.4 Volume anomaly detection

```sql
WITH daily AS (
    SELECT DATE(ingested_at) AS d, COUNT(*) AS n
    FROM RAW.RAW_ORDERS WHERE ingested_at >= DATEADD('day', -35, CURRENT_DATE())
    GROUP BY 1
),
stats AS (
    SELECT AVG(n) AS mu, STDDEV(n) AS sigma FROM daily WHERE d < CURRENT_DATE()
)
SELECT d, n, (n - mu) / NULLIF(sigma, 0) AS z_score
FROM daily, stats
WHERE d = CURRENT_DATE() AND ABS((n - mu) / NULLIF(sigma, 0)) > 3;
```

Seasonal data (weekends, month-end) needs a same-weekday baseline, not a flat average.

## 12.5 Contract enforcement in the pipeline

```python
import yaml
from pydantic import create_model

TYPES = {'integer': int, 'string': str, 'timestamp': str}

def model_from_contract(path: str):
    c = yaml.safe_load(open(path))
    fields = {col['name']: (TYPES[col['type']] if not col['nullable'] else (TYPES[col['type']] | None), ...)
              for col in c['columns']}
    return create_model(c['dataset'].title(), **fields)
```

A breaking change (dropped column, changed type, new required field) must fail the producer's CI against the contract, not be discovered by the consumer at 3 a.m. Additive nullable columns are non-breaking.

---

# Chapter 13. Security, Governance and Observability (Completion)

## 13.1 Role hierarchy with future grants

```sql
USE ROLE SECURITYADMIN;
CREATE ROLE IF NOT EXISTS ROLE_INGESTION;
CREATE ROLE IF NOT EXISTS ROLE_TRANSFORM;
CREATE ROLE IF NOT EXISTS ROLE_BI_READ;
CREATE ROLE IF NOT EXISTS ROLE_PII_READER;

GRANT ROLE ROLE_BI_READ   TO ROLE ROLE_TRANSFORM;   -- transform can read what BI reads
GRANT ROLE ROLE_TRANSFORM TO ROLE SYSADMIN;         -- keep custom roles under SYSADMIN

GRANT USAGE ON DATABASE ENTERPRISE_DW TO ROLE ROLE_BI_READ;
GRANT USAGE ON SCHEMA ENTERPRISE_DW.MARKETPLACE TO ROLE ROLE_BI_READ;
GRANT SELECT ON ALL TABLES IN SCHEMA ENTERPRISE_DW.MARKETPLACE TO ROLE ROLE_BI_READ;
GRANT SELECT ON FUTURE TABLES IN SCHEMA ENTERPRISE_DW.MARKETPLACE TO ROLE ROLE_BI_READ;
GRANT SELECT ON ALL VIEWS IN SCHEMA ENTERPRISE_DW.SEMANTIC TO ROLE ROLE_BI_READ;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA ENTERPRISE_DW.SEMANTIC TO ROLE ROLE_BI_READ;
GRANT USAGE ON WAREHOUSE BI_WH TO ROLE ROLE_BI_READ;
```

## 13.2 Masking and row access policies

```sql
CREATE OR REPLACE MASKING POLICY GOV.MASK_EMAIL AS (val STRING) RETURNS STRING ->
  CASE
    WHEN IS_ROLE_IN_SESSION('ROLE_PII_READER') THEN val
    ELSE REGEXP_REPLACE(val, '.+@', '*****@')
  END;

ALTER TABLE SCHEMATIZED.DIM_CUSTOMER MODIFY COLUMN email
  SET MASKING POLICY GOV.MASK_EMAIL;

-- tag-based: classify once, protect everywhere
CREATE TAG IF NOT EXISTS GOV.PII_TYPE ALLOWED_VALUES 'EMAIL', 'PHONE';
ALTER TAG GOV.PII_TYPE SET MASKING POLICY GOV.MASK_EMAIL;
ALTER TABLE SCHEMATIZED.DIM_CUSTOMER MODIFY COLUMN email SET TAG GOV.PII_TYPE = 'EMAIL';

-- row access: regional managers see only their region
CREATE OR REPLACE ROW ACCESS POLICY GOV.RAP_REGION AS (region_id NUMBER) RETURNS BOOLEAN ->
  IS_ROLE_IN_SESSION('ROLE_ALL_REGIONS')
  OR EXISTS (SELECT 1 FROM GOV.USER_REGION_MAP m
             WHERE m.user_name = CURRENT_USER() AND m.region_id = region_id);

ALTER TABLE MARKETPLACE.MART_SALES ADD ROW ACCESS POLICY GOV.RAP_REGION ON (region_id);
```

Use `IS_ROLE_IN_SESSION` rather than `CURRENT_ROLE()` so role hierarchies behave correctly. Never let the BI service account own the object (policies do not bind an owner the same way you expect), and test policies with `EXECUTE AS` or a test role.

## 13.3 Authentication and network

```sql
CREATE USER svc_dbt DEFAULT_ROLE = ROLE_TRANSFORM TYPE = SERVICE
  RSA_PUBLIC_KEY = 'MIIBIjANBgkq...';

CREATE NETWORK POLICY ETL_ONLY ALLOWED_IP_LIST = ('203.0.113.0/24');
ALTER USER svc_dbt SET NETWORK_POLICY = ETL_ONLY;
```

Service accounts use key-pair or OAuth and never passwords. Humans use SSO with MFA. Rotate keys and store the private key in Secret Manager.

## 13.4 GCS access: least privilege

```bash
# bucket-level uniform access, no public access
gcloud storage buckets update gs://enterprise-data-lake \
  --uniform-bucket-level-access --public-access-prevention

# the ingestion job writes bronze only
gcloud storage buckets add-iam-policy-binding gs://enterprise-data-lake \
  --member='serviceAccount:sa-ingest@PROJECT.iam.gserviceaccount.com' \
  --role='roles/storage.objectCreator' \
  --condition='expression=resource.name.startsWith("projects/_/buckets/enterprise-data-lake/objects/bronze/"),title=bronze-only'

# Snowflake's generated service account reads silver only
gcloud storage buckets add-iam-policy-binding gs://enterprise-data-lake \
  --member='serviceAccount:SNOWFLAKE_GENERATED_SA@PROJECT.iam.gserviceaccount.com' \
  --role='roles/storage.objectViewer' \
  --condition='expression=resource.name.startsWith("projects/_/buckets/enterprise-data-lake/objects/silver/"),title=silver-read'
```

Add retention policies and object versioning on regulated prefixes, CMEK where policy requires it, and Cloud Audit Data Access logs on the bucket.

## 13.5 Observability and cost guardrails

```sql
CREATE RESOURCE MONITOR RM_ETL WITH CREDIT_QUOTA = 500
  FREQUENCY = MONTHLY START_TIMESTAMP = IMMEDIATELY
  TRIGGERS ON 80 PERCENT DO NOTIFY
           ON 100 PERCENT DO SUSPEND;
ALTER WAREHOUSE ETL_WH SET RESOURCE_MONITOR = RM_ETL;

-- who spent what this month
SELECT warehouse_name, SUM(credits_used) AS credits
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATE_TRUNC('month', CURRENT_DATE())
GROUP BY 1 ORDER BY credits DESC;

-- who accessed a sensitive column
SELECT query_start_time, user_name, direct_objects_accessed
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
WHERE query_start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  AND ARRAY_CONTAINS('SCHEMATIZED.DIM_CUSTOMER.EMAIL'::VARIANT,
      TRANSFORM(base_objects_accessed, o -> o:objectName || '.' || col.value:columnName))
;
```

The last query is illustrative: `ACCESS_HISTORY` stores column lists as nested arrays, so the usual approach is `LATERAL FLATTEN` over `base_objects_accessed` and `columns`. Test it on your account before relying on it.

---

# Chapter 14. CI/CD and Terraform (Completion)

## 14.1 GitHub Actions with keyless GCP authentication

```yaml
name: data-platform-ci
on:
  pull_request:
    branches: [main]

permissions:
  id-token: write      # Workload Identity Federation, no JSON keys
  contents: read

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: {python-version: '3.11', cache: pip}
      - run: pip install -r requirements-ci.txt
      - run: ruff check . && pytest tests/ -q
      - run: sqlfluff lint models/ --dialect snowflake

      - uses: google-github-actions/auth@v2
        with:
          workload_identity_provider: ${{ secrets.WIF_PROVIDER }}
          service_account: ${{ secrets.CI_SERVICE_ACCOUNT }}

      - name: Fetch production manifest
        run: |
          mkdir -p prod_state
          gcloud storage cp gs://company-dbt-artifacts/prod/manifest.json prod_state/

      - name: dbt slim CI
        env:
          SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_CI_USER }}
          SNOWFLAKE_PRIVATE_KEY: ${{ secrets.SNOWFLAKE_CI_KEY }}
          DBT_SCHEMA: CI_PR_${{ github.event.number }}
        run: |
          dbt deps
          dbt build --select state:modified+ --defer --state prod_state --target ci

      - name: Drop CI schema
        if: always()
        run: dbt run-operation drop_dev_schema --args "{schema_name: CI_PR_${{ github.event.number }}}" --target ci
```

Deploy to production on merge to `main` from a separate workflow bound to a protected GitHub Environment with required reviewers, then upload the new `manifest.json` to GCS as the last step.

## 14.2 Terraform: GCS, Pub/Sub, IAM

```hcl
terraform {
  required_version = ">= 1.6"
  backend "gcs" {
    bucket = "company-tfstate"
    prefix = "data-platform/prod"   # use a separate prefix per environment
  }
  required_providers {
    google    = { source = "hashicorp/google", version = "~> 6.0" }
    snowflake = { source = "snowflakedb/snowflake", version = "~> 1.0" }
  }
}

variable "env" { type = string }

resource "google_storage_bucket" "lake" {
  name                        = "enterprise-data-lake-${var.env}"
  location                    = "ASIA-SOUTH1"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  versioning { enabled = true }

  lifecycle_rule {
    condition { age = 90, matches_prefix = ["bronze/"] }
    action    { type = "SetStorageClass", storage_class = "NEARLINE" }
  }
}

resource "google_service_account" "ingest" {
  account_id   = "sa-ingest-${var.env}"
  display_name = "Ingestion jobs"
}

resource "google_storage_bucket_iam_member" "ingest_write" {
  bucket = google_storage_bucket.lake.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.ingest.email}"
}

resource "google_pubsub_topic" "gcs_events" {
  name = "gcs-object-events-${var.env}"
}

resource "google_storage_notification" "lake_events" {
  bucket         = google_storage_bucket.lake.name
  payload_format = "JSON_API_V1"
  topic          = google_pubsub_topic.gcs_events.id
  event_types    = ["OBJECT_FINALIZE"]
  object_name_prefix = "silver/"
}
```

The Snowflake provider's resource names and arguments changed significantly between major versions. Pin the provider version and check its documentation for `snowflake_warehouse`, `snowflake_database` and grant resources before copying definitions. Keep secrets out of state where possible and encrypt the state bucket.

## 14.3 Promotion and release checklist

1. Pull request opens: lint, unit tests, `dbt parse`, slim CI build in an isolated schema.
2. Terraform `plan` posted to the PR as a comment; reviewers read it.
3. Merge to `main` deploys to the test environment automatically.
4. Manual approval gates production.
5. Post-deploy: `dbt source freshness`, row-count smoke tests, dashboard spot-check.
6. Rollback: Git revert plus re-deploy; for data, Snowflake Time Travel clone of the affected table taken before the release.

---

# Chapter 17. Pub/Sub, Dataflow and Datastream (New)

## 17.1 Pub/Sub topic, subscription and dead-letter setup

```bash
gcloud pubsub topics create customer-cdc
gcloud pubsub topics create customer-cdc-dlq

gcloud pubsub subscriptions create customer-cdc-sub \
  --topic=customer-cdc \
  --ack-deadline=60 \
  --enable-message-ordering \
  --message-retention-duration=7d \
  --dead-letter-topic=customer-cdc-dlq \
  --max-delivery-attempts=5 \
  --min-retry-delay=10s --max-retry-delay=600s
```

Pub/Sub delivery is at-least-once. Duplicates are normal, so every consumer deduplicates on `event_id`. Ordering is only guaranteed per ordering key (use the business key), and only when the publisher sets the key and the subscription enables ordering.

## 17.2 Publisher with ordering key and attributes

```python
import json
from google.cloud import pubsub_v1

publisher = pubsub_v1.PublisherClient(
    publisher_options=pubsub_v1.types.PublisherOptions(enable_message_ordering=True))
topic = publisher.topic_path('my-project', 'customer-cdc')

def publish(event: dict):
    future = publisher.publish(
        topic,
        json.dumps(event).encode('utf-8'),
        ordering_key=str(event['customer_id']),
        event_id=event['event_id'],
        schema_version='1',
    )
    return future.result(timeout=30)   # raises on failure; handle ordering-key resume on error
```

If a publish with an ordering key fails, Pub/Sub pauses that key until you call `publisher.resume_publish(topic, ordering_key)`. Forgetting this is a classic production stall.

## 17.3 Subscriber with safe acknowledgement

```python
from google.cloud import pubsub_v1
import json

subscriber = pubsub_v1.SubscriberClient()
sub = subscriber.subscription_path('my-project', 'customer-cdc-sub')

def callback(msg):
    try:
        event = json.loads(msg.data)
        write_idempotently(event)     # upsert keyed by event_id
        msg.ack()
    except Exception:
        msg.nack()                    # redelivered; after max attempts goes to the DLQ

flow = pubsub_v1.types.FlowControl(max_messages=500)
future = subscriber.subscribe(sub, callback=callback, flow_control=flow)
future.result()
```

Ack only after the durable write. Monitor `subscription/oldest_unacked_message_age` and DLQ depth in Cloud Monitoring.

## 17.4 Dataflow (Apache Beam) streaming: Pub/Sub to GCS with late data and a dead-letter output

```python
import json
import apache_beam as beam
from apache_beam import window
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode
from apache_beam.utils.timestamp import Timestamp

class Parse(beam.DoFn):
    OK, BAD = 'ok', 'bad'
    def process(self, raw):
        try:
            e = json.loads(raw.decode('utf-8'))
            assert e['event_id'] and e['customer_id']
            ts = Timestamp.from_rfc3339(e['source_commit_ts'] + 'Z')
            yield window.TimestampedValue(e, ts)       # event time, not arrival time
        except Exception as exc:
            yield beam.pvalue.TaggedOutput(self.BAD, {'raw': raw.decode('utf-8', 'replace'), 'error': str(exc)})

opts = PipelineOptions(streaming=True)
opts.view_as(StandardOptions).streaming = True

with beam.Pipeline(options=opts) as p:
    parsed = (p
        | beam.io.ReadFromPubSub(subscription='projects/my-project/subscriptions/customer-cdc-sub', with_attributes=False)
        | beam.ParDo(Parse()).with_outputs(Parse.BAD, main=Parse.OK))

    (parsed[Parse.OK]
        | beam.WindowInto(
            window.FixedWindows(60),
            trigger=AfterWatermark(late=AfterProcessingTime(300)),
            allowed_lateness=3600,
            accumulation_mode=AccumulationMode.DISCARDING)
        | beam.Map(json.dumps)
        | fileio.WriteToFiles(path='gs://enterprise-data-lake/cdc/customers/', shards=2,
                              file_naming=fileio.destination_prefix_naming(suffix='.json')))

    (parsed[Parse.BAD]
        | beam.Map(json.dumps)
        | fileio.WriteToFiles(path='gs://enterprise-data-lake/rejected/customer_cdc/'))
```

Events later than one hour (`allowed_lateness`) are dropped by the window. In regulated pipelines, do not accept that: write everything to the immutable ledger first and treat windows as an optimisation. Ordering of CDC state is decided later in SQL using source sequence, not by the stream window.

## 17.5 Datastream versus Debezium

| Aspect | Datastream | Debezium on Kafka |
| --- | --- | --- |
| Operation | Managed by Google | You run Kafka Connect and brokers |
| Sources | Oracle, MySQL, PostgreSQL, SQL Server (verify current list) | Broad connector catalogue |
| Destinations | GCS, BigQuery | Any Kafka consumer |
| Replay | From retained log / backfill | Kafka retention plus connector offsets |
| Choose when | GCP-centric, low ops budget | Multiple consumers, custom routing, schema registry control |

## 17.6 Snowflake notification integration for GCS Snowpipe

```sql
CREATE NOTIFICATION INTEGRATION GCS_NOTIFICATION_INTEGRATION
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = GCP_PUBSUB
  ENABLED = TRUE
  GCP_PUBSUB_SUBSCRIPTION_NAME = 'projects/my-project/subscriptions/snowpipe-sub';

DESC NOTIFICATION INTEGRATION GCS_NOTIFICATION_INTEGRATION;
-- copy GCP_PUBSUB_SERVICE_ACCOUNT from the output, then grant it:
--   roles/pubsub.subscriber on the subscription
--   roles/monitoring.viewer on the project

CREATE OR REPLACE PIPE RAW.CUSTOMER_PIPE
  AUTO_INGEST = TRUE
  INTEGRATION = 'GCS_NOTIFICATION_INTEGRATION'
AS
COPY INTO RAW.RAW_CUSTOMERS FROM @GCS_CUSTOMER_STAGE
FILE_FORMAT = (FORMAT_NAME = FF_PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

SELECT SYSTEM$PIPE_STATUS('RAW.CUSTOMER_PIPE');
```

The bucket notification (chapter 14.2) publishes `OBJECT_FINALIZE` events to a topic, and `snowpipe-sub` is a subscription on that topic.

---

# Chapter 18. Snowflake Advanced Features (New)

## 18.1 Corrected Streams and Tasks CDC pattern

```sql
CREATE OR REPLACE STREAM CUSTOMER_STREAM ON TABLE RAW.CUSTOMER_CDC_LEDGER APPEND_ONLY = TRUE;

CREATE OR REPLACE TASK LOAD_CUSTOMER_CURRENT
  WAREHOUSE = ETL_WH
  SCHEDULE = '2 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('CUSTOMER_STREAM')
AS
MERGE INTO SCHEMATIZED.DIM_CUSTOMER_CURRENT t
USING (
    SELECT customer_id, customer_name, city, operation,
           source_commit_ts, source_sequence
    FROM CUSTOMER_STREAM
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY customer_id
        ORDER BY source_commit_ts DESC, source_sequence DESC, event_id DESC) = 1
) s
ON t.customer_id = s.customer_id
WHEN MATCHED AND s.operation = 'DELETE'
     AND (s.source_commit_ts > t.source_commit_ts
          OR (s.source_commit_ts = t.source_commit_ts AND s.source_sequence > t.source_sequence))
  THEN DELETE
WHEN MATCHED AND s.operation IN ('INSERT', 'UPDATE')
     AND (s.source_commit_ts > t.source_commit_ts
          OR (s.source_commit_ts = t.source_commit_ts AND s.source_sequence > t.source_sequence))
  THEN UPDATE SET t.customer_name = s.customer_name, t.city = s.city,
                  t.source_commit_ts = s.source_commit_ts, t.source_sequence = s.source_sequence
WHEN NOT MATCHED AND s.operation IN ('INSERT', 'UPDATE')
  THEN INSERT (customer_id, customer_name, city, source_commit_ts, source_sequence)
       VALUES (s.customer_id, s.customer_name, s.city, s.source_commit_ts, s.source_sequence);

ALTER TASK LOAD_CUSTOMER_CURRENT RESUME;
```

Why this works: the stream is `APPEND_ONLY` over an insert-only CDC ledger, so there are no update-pair rows to confuse the MERGE. Ordering uses the source position, not arrival time, and the tuple comparison guards against stale events. The stream offset advances when the MERGE commits, so a failed task replays the same changes.

Two residual gaps to name in an interview: a DELETE for a key not yet present is ignored (the tombstone must also go to a ledger-driven history model), and if the target must also keep deleted keys suppressed against older arrivals, store the tombstone in a separate table and check it in the MERGE.

## 18.2 Stream staleness

```sql
SHOW STREAMS;   -- check STALE and STALE_AFTER columns
```

A stream goes stale when its offset falls behind the source table's retention window. A stale stream cannot be read and must be recreated, which loses the unconsumed changes. Alert on `STALE_AFTER` approaching, and keep consumers running at least as often as retention allows.

## 18.3 Stored procedure with transaction and error handling

```sql
CREATE OR REPLACE PROCEDURE AUDIT.LOAD_WITH_AUDIT(P_BATCH_ID VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    BEGIN TRANSACTION;
    DELETE FROM SCHEMATIZED.FACT_ORDERS WHERE load_batch_id = :P_BATCH_ID;   -- idempotent replace
    INSERT INTO SCHEMATIZED.FACT_ORDERS (order_id, customer_sk, order_date, order_amount, order_status, load_batch_id)
    SELECT o.order_id, d.customer_sk, o.order_date::DATE, o.order_amount, o.status, :P_BATCH_ID
    FROM PREPARE.PREP_ORDERS o
    LEFT JOIN SCHEMATIZED.DIM_CUSTOMER d
      ON o.customer_id = d.customer_id AND d.is_current
    WHERE o.batch_id = :P_BATCH_ID;
    COMMIT;
    RETURN 'OK';
EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        INSERT INTO AUDIT.ETL_RUN_LOG (batch_id, status, error_message, finished_at)
        VALUES (:P_BATCH_ID, 'FAILED', :SQLERRM, CURRENT_TIMESTAMP());
        RAISE;
END;
$$;
```

## 18.4 Snowpark Python procedure

```python
# create the procedure from Python, executed by Snowflake compute
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, sum as sum_, row_number
from snowflake.snowpark import Window

def top_customers(session: snowpark.Session, month: str) -> str:
    df = (session.table('SCHEMATIZED.FACT_ORDERS')
          .filter(col('ORDER_DATE') >= month)
          .group_by('CUSTOMER_SK').agg(sum_('ORDER_AMOUNT').alias('REV')))
    w = Window.order_by(col('REV').desc())
    top = df.with_column('RK', row_number().over(w)).filter(col('RK') <= 100)
    top.write.mode('overwrite').save_as_table('MARKETPLACE.TOP_CUSTOMERS')
    return 'done'

session.sproc.register(
    top_customers, name='MARKETPLACE.SP_TOP_CUSTOMERS', is_permanent=True,
    stage_location='@SNOWPARK_STAGE', packages=['snowflake-snowpark-python'], replace=True)
```

Use Snowpark when logic is awkward in SQL (complex Python libraries, ML feature code). Keep set-based transformations in SQL or dbt. Snowpark pushes execution down to the warehouse; do not call `to_pandas()` on large tables.

## 18.5 Dynamic Tables versus dbt incremental

```sql
CREATE OR REPLACE DYNAMIC TABLE MARKETPLACE.DT_CUSTOMER_REVENUE
  TARGET_LAG = '10 minutes'
  WAREHOUSE = ETL_WH
  REFRESH_MODE = INCREMENTAL
AS
SELECT customer_sk, SUM(order_amount) AS revenue, COUNT(*) AS orders
FROM SCHEMATIZED.FACT_ORDERS GROUP BY customer_sk;

SHOW DYNAMIC TABLES;
SELECT * FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
ORDER BY refresh_start_time DESC LIMIT 20;
```

| Factor | Dynamic Table | dbt incremental |
| --- | --- | --- |
| Freshness | Declarative `TARGET_LAG` | Whatever your schedule is |
| Late data / deletes | Handled by the engine if the query supports incremental refresh | You design the logic |
| Custom merge logic, SCD2 | Not suitable | Full control |
| Testing and CI | Limited | First-class |
| Fallback | May fall back to full refresh for unsupported constructs (check `REFRESH_MODE`) | Explicit |

## 18.6 Schema evolution and schema inference

```sql
-- infer a table from Parquet files
CREATE OR REPLACE TABLE RAW.RAW_ORDERS_V2
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(INFER_SCHEMA(LOCATION => '@GCS_ORDERS_STAGE', FILE_FORMAT => 'FF_PARQUET')))
  ENABLE_SCHEMA_EVOLUTION = TRUE;

COPY INTO RAW.RAW_ORDERS_V2 FROM @GCS_ORDERS_STAGE
  FILE_FORMAT = (FORMAT_NAME = FF_PARQUET)
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
```

Schema evolution adds columns automatically on load. It does not change types or remove columns. Allow it only in RAW; downstream layers use explicit contracts and `append_new_columns` or `fail`.

## 18.7 Secure data sharing

```sql
CREATE SHARE SALES_SHARE;
GRANT USAGE ON DATABASE ENTERPRISE_DW TO SHARE SALES_SHARE;
GRANT USAGE ON SCHEMA ENTERPRISE_DW.SEMANTIC TO SHARE SALES_SHARE;
GRANT SELECT ON VIEW ENTERPRISE_DW.SEMANTIC.VW_MONTHLY_SALES_SECURE TO SHARE SALES_SHARE;
ALTER SHARE SALES_SHARE ADD ACCOUNTS = partner_org.partner_account;
```

Share only secure views. The consumer pays for its own compute, and there is no data copy. Cross-region or cross-cloud sharing requires replication to the target region first.

## 18.8 Iceberg tables with a GCS external volume

```sql
CREATE EXTERNAL VOLUME GCS_ICEBERG_VOL
  STORAGE_LOCATIONS = ((
    NAME = 'gcs-asia-south1'
    STORAGE_PROVIDER = 'GCS'
    STORAGE_BASE_URL = 'gcs://enterprise-data-lake/iceberg/'));

CREATE ICEBERG TABLE MARKETPLACE.ORDERS_ICE (order_id NUMBER, order_ts TIMESTAMP_NTZ, amount NUMBER(18,2))
  CATALOG = 'SNOWFLAKE' EXTERNAL_VOLUME = 'GCS_ICEBERG_VOL' BASE_LOCATION = 'orders/';
```

Choose Iceberg when Spark, Trino or other engines must read the same tables that Snowflake writes. Accept the trade-off of fewer Snowflake-native optimisations and your own responsibility for storage lifecycle.

---

# Chapter 19. Disaster Recovery and Cost Governance (New)

## 19.1 RPO and RTO by layer

| Layer | Protection | Typical RPO | Recovery action |
| --- | --- | --- | --- |
| GCS Bronze | Dual-region bucket, versioning, retention lock | Near zero | Switch consumers to surviving region |
| Kafka / Pub/Sub | Replication factor 3, 7-day retention | Retention window | Replay from offset |
| Snowflake tables | Time Travel (up to 90 days on Enterprise), Fail-safe | Minutes | `UNDROP`, `AT`/`BEFORE` clone |
| Snowflake account | Replication and failover groups (Business Critical) | Replication schedule | Promote secondary |
| dbt/Airflow code | Git | Last commit | Redeploy |

```sql
-- Replicate and fail over key objects to a secondary account
CREATE FAILOVER GROUP FG_ENTERPRISE
  OBJECT_TYPES = DATABASES, ROLES, WAREHOUSES, INTEGRATIONS
  ALLOWED_DATABASES = ENTERPRISE_DW
  ALLOWED_ACCOUNTS = myorg.dr_account
  REPLICATION_SCHEDULE = '10 MINUTE';

-- On the secondary account during a DR event
ALTER FAILOVER GROUP FG_ENTERPRISE PRIMARY;
```

Because Bronze is immutable and Snowflake layers are rebuildable from it, the strongest DR statement is: after restoring Bronze and code, we can recompute every downstream layer deterministically. Rehearse it. An untested DR plan is a hypothesis.

## 19.2 Recovering from a bad release

```sql
-- before releasing, snapshot critical tables
CREATE TABLE SCHEMATIZED.FACT_ORDERS_PRE_RELEASE CLONE SCHEMATIZED.FACT_ORDERS;

-- after a bad MERGE: see what changed, then restore
SELECT COUNT(*) FROM SCHEMATIZED.FACT_ORDERS BEFORE (STATEMENT => '01b2c3d4-0000-1111-2222-333344445555');

CREATE OR REPLACE TABLE SCHEMATIZED.FACT_ORDERS CLONE SCHEMATIZED.FACT_ORDERS
  BEFORE (STATEMENT => '01b2c3d4-0000-1111-2222-333344445555');
```

Replacing a table with a clone drops grants, policies and streams attached to the old object unless you use `COPY GRANTS` and re-attach policies. Include those steps in the runbook.

## 19.3 Cost investigation queries

```sql
-- top cost drivers by query pattern, last 7 days
SELECT query_parameterized_hash, ANY_VALUE(query_text) AS sample_sql,
       COUNT(*) AS runs,
       SUM(total_elapsed_time) / 3600000 AS hours,
       SUM(bytes_scanned) / POWER(1024, 4) AS tb_scanned
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY 1 ORDER BY hours DESC LIMIT 20;

-- idle warehouses (running but doing little)
SELECT warehouse_name, SUM(credits_used_compute) AS credits,
       SUM(credits_used_compute) - SUM(credits_attributed_compute_queries) AS idle_credits
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY 1 ORDER BY idle_credits DESC;
```

Common causes of a 40% jump: a new BI dashboard hitting a large warehouse with auto-suspend set too high, an incremental model that silently became a full refresh, an auto-clustering service on a high-churn table, a MERGE that stopped pruning, or a runaway retry loop. Set `QUERY_TAG` in dbt and Airflow so each credit is attributable to a pipeline.

```sql
ALTER SESSION SET QUERY_TAG = '{"pipeline":"orders_gcs_to_snowflake","run":"2026-11-09"}';
```

---

# Chapter 20. Capstone Runbook (New)

Build one flow end to end, with an acceptance test at every step. The goal is evidence you can show an interviewer.

| Step | Deliverable | Acceptance test |
| --- | --- | --- |
| 1 | Postgres source with `customers` and `orders` plus a script generating inserts, updates, deletes, duplicates, late events | Script can produce a known final state |
| 2 | Debezium or Datastream to Pub/Sub or Kafka to GCS Bronze with manifests | Replaying the same day produces identical Bronze content |
| 3 | PySpark Bronze to Silver with quarantine | `valid + rejected = Bronze count` |
| 4 | Snowflake storage integration, stage, Snowpipe | Drop a file in GCS: row appears in RAW within minutes |
| 5 | dbt staging, prepare, SCD2 dimension, fact | All SCD2 integrity tests pass, including the late-event case |
| 6 | Airflow DAG with sensor, load, dbt, reconcile | Re-running any day produces zero duplicates |
| 7 | Masking and row access policies | A BI role sees masked email and only its region |
| 8 | CI on pull requests with slim build and isolated schema | A model change builds only modified nodes |
| 9 | Failure drills | Kill the task mid-run, send a late event, send a delete before an old update, break the schema: each recovers without manual data fixes |
| 10 | Cost and DR notes | A one-page runbook with RPO/RTO and a credit report |

---

# Chapter 16. Advanced Interview Bank (Chapter 15 runbooks are in Appendix A)

## 16.0 How to answer senior-level questions

Use the same five steps for any design or debugging question. Interviewers score the structure as much as the content.

1. **Clarify**: volume, latency, correctness needs (can we lose or duplicate?), source behavior (are deletes and late events possible?), team size.
2. **State the invariant**: what must always be true (no duplicate current rows, totals reconcile, nothing lost).
3. **Design**: the simplest architecture that holds the invariant.
4. **Failure modes**: crash mid-run, duplicate delivery, late data, schema change, backfill.
5. **Trade-offs**: what you gave up, what you would monitor, and when you would redesign.

---

## 16.1 Snowflake: conceptual questions

**Q1. How does micro-partition pruning work, and why can a filtered query still scan everything?**

Snowflake stores min/max (and other) metadata per column per micro-partition and skips partitions whose range cannot match the predicate. Pruning fails when the filter column's values are spread across all partitions (poor natural clustering), when you wrap the column in a function (`TO_CHAR(order_date, ...)`), when you compare with mismatched types, or when a filter is applied only after a join. Diagnose with Query Profile (`Partitions scanned` versus `Partitions total`) and `SYSTEM$CLUSTERING_INFORMATION`. Fixes: rewrite predicates as ranges on the raw column, load data in filter-column order, or add a clustering key for large, frequently filtered tables. Clustering has an ongoing automatic reclustering cost, so it needs a measurable benefit.

**Q2. A query is fast the second time. Name every possible reason and how you would prove which one.**

Result cache (same text, same role context, unchanged data, no non-deterministic functions, within 24 hours), warehouse local disk cache (data files kept on the warehouse SSDs, lost when the warehouse suspends), and compilation/metadata reuse. Prove it with Query Profile: result-cache hits show a result-reuse node and near-zero execution time; warehouse-cache benefit shows as a high `Percentage scanned from cache`. To benchmark honestly, disable it with `ALTER SESSION SET USE_CACHED_RESULT = FALSE` and suspend/resume the warehouse.

**Q3. Queries are queued on a BI warehouse. Walk me through your decision.**

Check `QUEUED_OVERLOAD_TIME` versus `EXECUTION_TIME`. High queue time with healthy execution means concurrency, so add clusters (multi-cluster, `STANDARD` policy for responsiveness) or split workloads onto separate warehouses. High execution time with spill means a single large query: resize or fix the SQL. If both are high, fix the worst queries first, because they hold slots longer than they should.

```sql
SELECT warehouse_name,
       AVG(queued_overload_time) / 1000 AS avg_queue_s,
       AVG(execution_time) / 1000 AS avg_exec_s,
       SUM(IFF(bytes_spilled_to_remote_storage > 0, 1, 0)) AS remote_spills
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
GROUP BY 1 ORDER BY avg_queue_s DESC;
```

**Q4. Why is a large MERGE slow, and how do you speed it up?**

MERGE joins the source to the target and rewrites every micro-partition that contains an updated or deleted row. Slowness comes from (1) the join scanning the whole target because the join key does not prune, (2) updates scattered across many partitions so many are rewritten, (3) a source with duplicate keys, and (4) heavy spilling. Remedies: deduplicate the source first, add a pruning predicate on the target (for example a date range in the `ON` clause or dbt `incremental_predicates`), cluster the target on the merge key or date, update only changed rows (compare a hash in the `WHEN MATCHED AND` clause), and batch smaller changes more often rather than one huge merge.

```sql
MERGE INTO tgt t USING src s
  ON t.id = s.id AND t.event_date >= DATEADD('day', -7, CURRENT_DATE())
WHEN MATCHED AND t.record_hash <> s.record_hash THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...;
```

**Q5. Why does MERGE fail with 'Duplicate row detected during DML action', or silently misbehave?**

More than one source row matches a single target row. Snowflake's behavior depends on the `ERROR_ON_NONDETERMINISTIC_MERGE` parameter: when true (default) the statement fails; when false it picks an arbitrary match, which is the dangerous case because the result is nondeterministic. Always guarantee one source row per key with a deterministic `QUALIFY ROW_NUMBER()` ordering.

**Q6. Streams, Tasks, Dynamic Tables, Snowpipe and Snowpipe Streaming: when each?**

| Need | Use |
| --- | --- |
| Continuous load of files landing in GCS | Snowpipe |
| Row-level ingestion from an application with sub-minute latency | Snowpipe Streaming or the Kafka connector |
| Change capture on a Snowflake table to drive a custom merge | Stream + Task |
| Keep a derived table fresh declaratively within a lag target | Dynamic Table |
| Scheduled SQL or dependency chains | Task graph (or Airflow if cross-system) |

**Q7. Time Travel, Fail-safe and cloning: differences and cost.**

Time Travel is user-accessible history within the retention period (1 day default, up to 90 on Enterprise for permanent tables) and supports `AT`/`BEFORE` queries, `UNDROP` and clones. Fail-safe is a further 7-day Snowflake-only recovery window for permanent tables, not user-accessible. Both consume storage for changed micro-partitions. Transient and temporary tables have no Fail-safe, and transient tables have at most 1 day of Time Travel, so use transient tables for rebuildable staging to cut storage cost. Zero-copy clones share micro-partitions initially and diverge as either side changes.

**Q8. Search Optimization, clustering and materialized views: how do you choose?**

Clustering improves range scans and pruning over a table. Search Optimization accelerates highly selective point lookups (equality, IN, substring, VARIANT paths) on large tables. A materialized view precomputes a supported aggregate or projection over one table. Pick by query pattern: BI date-range scans, clustering; lookup by email or id, search optimization; repeated identical aggregate, MV or a Dynamic Table. Each has a maintenance cost billed as serverless credits.

**Q9. How does Snowflake handle concurrent writers?**

DML uses READ COMMITTED isolation at statement level, and concurrent updates to the same table queue on a table-level lock held while a statement runs (up to `LOCK_TIMEOUT`). Many small concurrent MERGEs against one table serialize, so design for one writer per table or coalesce writes.

**Q10. Flatten semi-structured JSON safely.**

```sql
SELECT
    r.payload:order_id::NUMBER             AS order_id,
    r.payload:customer:id::NUMBER          AS customer_id,
    li.value:sku::STRING                   AS sku,
    TRY_TO_NUMBER(li.value:qty::STRING)    AS qty
FROM RAW.RAW_ORDERS_JSON r,
     LATERAL FLATTEN(INPUT => r.payload:line_items, OUTER => TRUE) li;
```

`OUTER => TRUE` keeps orders with no line items. Use `TRY_` casts so a bad value does not fail the whole statement, and route failures to the rejected store.

---

## 16.2 dbt: conceptual questions

**Q11. What goes wrong with arrival-time watermarks in incremental models?**

A filter like `WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})` misses rows that were committed with an older `loaded_at` after the previous run read the table (concurrent loads, clock skew, backfills), and any row whose watermark column is null. Mitigations: a lookback window with a deterministic merge (safe because the merge is idempotent), watermarks based on a monotonic load ID or batch ID that the loader assigns after commit, periodic reconciliation against RAW, and a scheduled partial full-refresh for the trailing window.

**Q12. A merge-based incremental model has `unique_key` containing NULLs. What happens?**

NULLs never match in the merge `ON` condition, so each run inserts new rows instead of updating them and you accumulate duplicates. Guarantee non-null keys (test with `not_null`), or build a surrogate key with `COALESCE`.

**Q13. How do you test an incremental model's incremental branch?**

Use dbt unit tests with the `is_incremental` override and a given `this` relation.

```yaml
unit_tests:
  - name: incremental_keeps_newer_row
    model: fct_orders
    overrides:
      macros:
        is_incremental: true
    given:
      - input: ref('stg_orders')
        rows:
          - {order_id: 1, updated_at: '2026-11-09 10:00:00', amount: 50}
      - input: this
        rows:
          - {order_id: 1, updated_at: '2026-11-09 11:00:00', amount: 40}
    expect:
      rows:
        - {order_id: 1, updated_at: '2026-11-09 11:00:00', amount: 40}
```

The expectation encodes the guard against stale events. Unit tests prove logic on tiny data; data tests prove properties of real data. You need both.

**Q14. When do you full-refresh, and how do you do it safely in production?**

When logic or schema semantics change so old rows are wrong, or after a defect. Safe procedure: build the new version in a shadow table (`dbt run --select model --full-refresh --target prod` into a different schema or alias), run reconciliation against the live table, then swap atomically with `ALTER TABLE ... SWAP WITH`. Never full-refresh a huge model during business hours on a shared warehouse without checking cost and downstream locks.

**Q15. How does dbt build its DAG, and what breaks it?**

It parses `ref()` and `source()` calls into dependency edges. It breaks on circular refs, on references hidden in macros that build SQL strings dynamically (dbt cannot see them unless declared), and on referencing tables directly by name, which makes lineage and ordering incorrect.

---

## 16.3 CDC and streaming: conceptual questions

**Q16. Is exactly-once processing real?**

Exactly-once is an effect, built from at-least-once delivery plus idempotent or transactional writes. Kafka idempotent producers and transactions give exactly-once within Kafka; Spark checkpoints give exactly-once state updates for supported sinks. Across systems (Kafka to GCS to Snowflake), you get effectively-once by making every write replay-safe: deterministic file names or `epoch_id`, `COPY INTO` load history, `MERGE` keyed on event ID, and watermarks that advance only after commit.

**Q17. Why does Kafka partitioning matter for CDC?**

Ordering is guaranteed only inside a partition. Key messages by business key so every change for a customer lands on one partition and is consumed in order. Increasing partitions later changes the key-to-partition mapping and can break ordering for in-flight keys, so plan partition counts up front. Consumer parallelism is capped by partition count. Use log compaction for topics that represent current state rather than history.

**Q18. What is the dual-write problem and how does CDC solve it?**

Writing to a database and publishing an event separately can leave them inconsistent if one fails. Fixes: read the database transaction log (Debezium, Datastream), or use the transactional outbox pattern (write the event to an outbox table in the same transaction and let CDC publish it).

**Q19. Explain watermarks and late data in Spark Structured Streaming.**

A watermark is `max(event_time seen) - threshold`. State for windows older than the watermark is dropped and later events for them are discarded. Trade-off: larger threshold means more state and more latency to finalize results but fewer lost events. Because discarded events are lost, keep an unwindowed raw ledger of everything and treat windowed outputs as derived.

**Q20. Schema evolution in a streaming platform: what do you enforce?**

A schema registry with a compatibility mode (BACKWARD lets new consumers read old data, FORWARD the reverse, FULL both). Adding an optional field with a default is compatible. Renaming, removing a required field or changing a type is a breaking change and needs a new topic version with a migration plan. Every event carries `schema_version`, and consumers route unknown versions to a dead-letter queue rather than failing.

**Q21. How do you backfill without disturbing a live stream?**

Run the backfill as a separate batch job reading historical Bronze, writing to a staging location, and merge into targets using the same ordering rules and idempotent keys as the live path. Pause or fence the live consumer for affected keys only if ordering between the two streams of changes cannot be resolved by source position. Reconcile counts and checksums before cutover.

---

## 16.4 PySpark, Airflow, GCS: conceptual questions

**Q22. repartition versus coalesce, cache versus checkpoint, broadcast versus sort-merge.**

`repartition(n, col)` does a full shuffle and can increase or rebalance partitions. `coalesce(n)` only merges existing partitions without a shuffle, so it only reduces and can leave partitions uneven. `cache()` keeps data for reuse while keeping lineage (lost executors recompute), whereas `checkpoint()` writes to reliable storage and truncates lineage, which helps with very long iterative plans. Broadcast join ships the small table to every executor and avoids shuffling the large one (default threshold about 10 MB, controlled by `spark.sql.autoBroadcastJoinThreshold`); sort-merge join shuffles and sorts both sides and scales to large-large joins.

**Q23. Why are Parquet and partitioning on GCS designed the way they are?**

Parquet is columnar with per-column statistics, so Spark and Snowflake push down projections and predicates and skip row groups. Partition by low-cardinality query-relevant columns (date, region), never by an ID. Aim for files of roughly 128 MB to 1 GB. Too many tiny files create listing and task-scheduling overhead on object storage, and too few huge files limit parallelism.

**Q24. Why is rename not safe on GCS, and what does that mean for Spark output?**

Object stores have no atomic directory rename: a rename is copy plus delete per object. Spark's default commit protocol relies on renames, so a failure can leave a partially visible output. Use a committer designed for object stores or write to a unique staging prefix and publish with a manifest, which is why this handbook consumes only manifest-complete batches.

**Q25. Explain the Airflow scheduler concepts that cause most incidents.**

A DAG run for `data_interval_start` to `data_interval_end` is triggered after that interval ends, so a daily run for 2026-11-08 starts on 2026-11-09 (the source of the common 'it runs a day late' confusion). `catchup=True` creates runs for every missed interval, which can flood the system after downtime. Sensors in `poke` mode hold a worker slot; `reschedule` mode or deferrable operators do not. Pools limit concurrency against shared resources such as Snowflake. XCom lives in the metadata database and is for small values only.

**Q26. How do you make an Airflow task safe to retry?**

The task must produce the same result when run twice for the same data interval. Patterns: write to a path derived from the batch ID and overwrite; `DELETE WHERE batch_id = X` then `INSERT` inside one transaction; `MERGE` on a business key; side effects against external APIs use an idempotency key. Update the watermark in the last task only.

**Q27. Star schema versus one big table (OBT) for BI on Snowflake.**

Star schemas keep dimensions reusable, make SCD2 and governance clean, and reduce storage duplication. OBTs are simpler for self-service and can be faster for a single dashboard. Because Snowflake handles joins well, model a star in SCHEMATIZED and publish denormalized marts in MARKETPLACE for specific consumers, rebuilt from the star so they can never diverge.

---

## 16.5 Scenario-based questions with code

**S1. A DELETE for customer 101 arrives before an older UPDATE. How do you prevent resurrection?**

State the invariant: the winner is decided by source position (commit timestamp, then sequence), never by arrival order, and a delete is an event, not an absence. Current state is derived from the immutable ledger:

```sql
CREATE OR REPLACE TABLE SCHEMATIZED.CUSTOMER_CURRENT AS
SELECT customer_id, customer_name, city, source_commit_ts, source_sequence
FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY customer_id
        ORDER BY source_commit_ts DESC, source_sequence DESC, event_id DESC) AS rn
    FROM RAW.CUSTOMER_CDC_LEDGER
)
WHERE rn = 1 AND operation <> 'DELETE';
```

For an incremental path, keep a tombstone table and filter inserts against it:

```sql
MERGE INTO SCHEMATIZED.CUSTOMER_CURRENT t
USING (
    SELECT s.*
    FROM STG_LATEST s
    LEFT JOIN SCHEMATIZED.CUSTOMER_TOMBSTONE tb
      ON tb.customer_id = s.customer_id
     AND (tb.source_commit_ts > s.source_commit_ts
          OR (tb.source_commit_ts = s.source_commit_ts AND tb.source_sequence >= s.source_sequence))
    WHERE tb.customer_id IS NULL OR s.operation = 'DELETE'
) s
ON t.customer_id = s.customer_id
WHEN MATCHED AND s.operation = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET t.city = s.city, t.source_commit_ts = s.source_commit_ts, t.source_sequence = s.source_sequence
WHEN NOT MATCHED AND s.operation <> 'DELETE' THEN INSERT (customer_id, city, source_commit_ts, source_sequence)
  VALUES (s.customer_id, s.city, s.source_commit_ts, s.source_sequence);

MERGE INTO SCHEMATIZED.CUSTOMER_TOMBSTONE tb
USING (SELECT * FROM STG_LATEST WHERE operation = 'DELETE') s ON tb.customer_id = s.customer_id
WHEN MATCHED AND s.source_commit_ts > tb.source_commit_ts THEN UPDATE SET tb.source_commit_ts = s.source_commit_ts, tb.source_sequence = s.source_sequence
WHEN NOT MATCHED THEN INSERT (customer_id, source_commit_ts, source_sequence) VALUES (s.customer_id, s.source_commit_ts, s.source_sequence);
```

A genuinely newer INSERT after the delete (reactivation) has a later position than the tombstone, passes the filter, and removes nothing from history. Mention that tombstones need a retention policy.

**S2. A customer changes address three times in one day. How do you preserve history?**

Clarify the reporting grain. If the business needs every change (audit, fraud, legal), use event-level SCD2 from the ledger (section 7.3), which yields three versions with distinct `valid_from` values and uses `source_sequence` to order same-second changes. If reports only need the end-of-day state, collapse to one version per day and say so explicitly, because an intraday fact (an order placed between changes) then maps to the day-end address, which may be wrong for that order.

```sql
SELECT customer_id, DATE(valid_from) AS d, COUNT(*) AS changes_that_day
FROM SCHEMATIZED.DIM_CUSTOMER_SCD2
GROUP BY 1, 2 HAVING COUNT(*) > 1;
```

**S3. A dbt incremental model missed records. Detect and recover.**

```sql
-- detect: rows in the source of truth that are absent from the model
SELECT s.order_id
FROM RAW_DB.ERP.ORDERS s
LEFT JOIN ANALYTICS.FCT_ORDERS f USING (order_id)
WHERE f.order_id IS NULL
  AND s.updated_at >= DATEADD('day', -14, CURRENT_DATE());
```

Recover by adding a controllable reload window to the model, then running it for the affected range. The merge is idempotent, so re-processing is safe.

```sql
{% if is_incremental() %}
    {% set reload_from = var('reload_from', none) %}
    WHERE updated_at >= {% if reload_from %} '{{ reload_from }}'::TIMESTAMP_NTZ
                        {% else %} (SELECT DATEADD('hour', -12, MAX(updated_at)) FROM {{ this }}) {% endif %}
{% endif %}
```

```bash
dbt run --select fct_orders --vars '{reload_from: 2026-10-25}'
```

Then find the root cause (late commits, wrong watermark column, clock skew) and add a scheduled reconciliation test so this is detected automatically next time.

**S4. A transformation bug has corrupted six months of data. How do you reprocess without downtime?**

Bronze is immutable, so rebuild from it into a shadow schema, validate, then swap.

```bash
dbt build --select +fct_orders+ --target shadow --vars '{full_reprocess: true}'
```

```sql
-- compare shadow and live; every difference must be explainable by the bug fix
(SELECT * FROM SCHEMATIZED.FACT_ORDERS EXCEPT SELECT * FROM SCHEMATIZED_SHADOW.FACT_ORDERS)
UNION ALL
(SELECT * FROM SCHEMATIZED_SHADOW.FACT_ORDERS EXCEPT SELECT * FROM SCHEMATIZED.FACT_ORDERS);

-- atomic cutover (re-apply grants and policies afterwards or use COPY GRANTS)
ALTER SCHEMA SCHEMATIZED SWAP WITH SCHEMATIZED_SHADOW;
```

Freeze the live pipeline's writes during the window or replay the delta that arrived during the rebuild. Announce restated numbers to finance before the swap.

**S5. An Airflow retry processes an already-completed batch. How do you prevent duplicate writes?**

Make the write step replace by batch ID, in one transaction, and move the watermark last.

```sql
BEGIN TRANSACTION;
DELETE FROM SCHEMATIZED.FACT_ORDERS WHERE load_batch_id = '{{ ds_nodash }}';
INSERT INTO SCHEMATIZED.FACT_ORDERS SELECT ..., '{{ ds_nodash }}' FROM PREPARE.PREP_ORDERS WHERE batch_id = '{{ ds_nodash }}';
COMMIT;
```

Also check `AUDIT.ETL_RUN_LOG` at the start and skip if the batch is already `SUCCESS`. For non-SQL side effects (API calls, emails), send an idempotency key derived from the batch ID.

**S6. Kafka consumer lag keeps growing. What do you do?**

```bash
kafka-consumer-groups.sh --bootstrap-server broker:9092 --describe --group customer-consumer-group
```

Read LAG per partition. One partition far behind points to a hot key or a poison message stuck in retry. All partitions behind points to insufficient throughput: check per-message processing time and downstream write latency before adding consumers, and remember parallelism is capped by the partition count. The corrected consumer pattern (dead-lettering rather than skipping and committing past a failure):

```python
from confluent_kafka import Consumer, Producer
import json

consumer = Consumer({'bootstrap.servers': 'broker:9092', 'group.id': 'cust', 'enable.auto.commit': False,
                     'auto.offset.reset': 'earliest'})
dlq = Producer({'bootstrap.servers': 'broker:9092'})
consumer.subscribe(['customer_events'])

def handle(msg):
    ev = json.loads(msg.value())
    upsert_by_event_id(ev)            # idempotent

while True:
    msg = consumer.poll(1.0)
    if msg is None or msg.error():
        continue
    try:
        handle(msg)
    except Exception as exc:
        dlq.produce('customer_events.dlq', key=msg.key(), value=msg.value(),
                    headers=[('error', str(exc).encode()), ('src_offset', str(msg.offset()).encode())])
        dlq.flush(10)                  # DLQ write must succeed before we commit past the message
    consumer.commit(message=msg, asynchronous=False)
```

The commit happens only after either a successful write or a confirmed dead-letter write, so no message is lost silently.

**S7. One Spark task runs far longer than the rest. What do you check?**

```python
from pyspark.sql import functions as F

# how skewed are the partitions after the join key shuffle?
(df.repartition('customer_id')
   .groupBy(F.spark_partition_id().alias('pid')).count().orderBy(F.desc('count')).show(5))

# which keys are hot?
df.groupBy('customer_id').count().orderBy(F.desc('count')).show(5)
```

If one key (often NULL or a default value such as -1) dominates: filter or handle nulls separately before the join, broadcast the small side, rely on AQE skew join, and salt only as a last resort (section 10.3). If skew is not the cause, check GC time, spill to disk and a single large file that cannot be split (gzip-compressed JSON is not splittable).

**S8. Tableau and Power BI show different revenue. How do you troubleshoot?**

Make both tools query the same governed view and compare at identical grain.

```sql
SELECT DATE(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', order_ts)) AS order_day_ist,
       SUM(order_amount) AS gross,
       SUM(IFF(status = 'COMPLETED', order_amount, 0)) AS completed,
       SUM(refund_amount) AS refunds,
       COUNT(*) AS rows_, COUNT(DISTINCT order_id) AS orders
FROM SCHEMATIZED.FACT_ORDERS
GROUP BY 1 ORDER BY 1;
```

Typical culprits: different time zone for day boundaries, refunds included in one and not the other, a many-to-one join duplicating rows (`rows_` greater than `orders` is the tell), a stale Power BI import versus a live Tableau connection, and a filter in one workbook. The fix is organizational as well as technical: one governed metric definition in the SEMANTIC layer.

**S9. Snowflake costs rose 40% in a month. Walk through the investigation.**

1. `WAREHOUSE_METERING_HISTORY` by warehouse and day to locate the step change.
2. `QUERY_HISTORY` grouped by `QUERY_TAG` or `QUERY_PARAMETERIZED_HASH` for that warehouse and period (queries in section 19.3).
3. Check what changed: new dashboard, a dbt model that became a full refresh, a new Snowpipe or serverless feature (`SERVERLESS_TASK_HISTORY`, `AUTOMATIC_CLUSTERING_HISTORY`, `SEARCH_OPTIMIZATION_HISTORY`).
4. Check warehouse settings: auto-suspend too long, size too large for the workload, multi-cluster `MAX_CLUSTER_COUNT` too high.
5. Fix the top offender, add a resource monitor, and add a weekly cost report so the next regression is a small one.

**S10. A source renames a column and the nightly load fails. What is your process, and how do you prevent it?**

Immediate: the contract says breaking changes need notice, so confirm with the producer, then map the new name in LANDING (`COALESCE(new_name, old_name)`) so downstream remains stable. Reload the failed batch from Bronze; no source re-extract is needed. Prevention: a data contract with compatibility checks in the producer's CI, schema registry rules, and a pre-load drift check:

```sql
SELECT column_name FROM TABLE(INFER_SCHEMA(LOCATION => '@GCS_ORDERS_STAGE/batch_date=2026-11-09/', FILE_FORMAT => 'FF_PARQUET'))
MINUS
SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = 'RAW' AND table_name = 'RAW_ORDERS';
```

**S11. Design: migrate a 10 TB Oracle schema to Snowflake with minimal downtime.**

1. Start CDC capture at a recorded log position (SCN) so no change is missed.
2. Run a consistent initial snapshot at that SCN, written as Parquet to GCS in parallel by key range.
3. Load into RAW with `COPY INTO`; then apply the CDC stream from the SCN forward using the ordering rules in this handbook.
4. Validate: row counts per table, per-partition checksums and aggregates on both sides.

```sql
-- run an equivalent hash aggregate in Oracle and Snowflake and compare by key range
SELECT FLOOR(order_id / 1000000) AS bucket, COUNT(*) AS n, SUM(order_amount) AS amt,
       SUM(HASH(order_id, customer_id, order_amount, status)) AS h
FROM RAW.ORDERS GROUP BY 1 ORDER BY 1;
```

5. When lag is near zero, freeze writes briefly, drain the last changes, run final reconciliation, repoint consumers, and keep the old system read-only for a rollback window.

**S12. Design: card-fraud alerts under 5 seconds, plus daily analytics.**

Two paths from one event stream. Hot path: application or CDC to Pub/Sub/Kafka, then a Flink/Dataflow/Spark streaming job with a feature store lookup and rules or model scoring, writing alerts to a low-latency store and an alert topic. Cold path: the same events archived to GCS Bronze, loaded to Snowflake via Snowpipe, modeled by dbt. The fraud decision never depends on Snowflake latency. Use the same event schema and IDs for both paths so analytics can audit the hot path's decisions, and reconcile hot-path counts to cold-path counts daily. Name the trade-offs: false-positive cost, late and out-of-order events in rules, and degraded-mode behavior when the scorer is down (fail open versus fail closed is a business decision).

---

## 16.6 Coding questions (SQL and PySpark)

**C1. Customers with at least three consecutive daily logins (gaps and islands).**

```sql
WITH d AS (SELECT DISTINCT customer_id, login_date FROM LOGINS),
g AS (
    SELECT customer_id, login_date,
           DATEADD('day', -ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY login_date), login_date) AS grp
    FROM d
)
SELECT customer_id, MIN(login_date) AS streak_start, MAX(login_date) AS streak_end, COUNT(*) AS days
FROM g GROUP BY customer_id, grp HAVING COUNT(*) >= 3;
```

**C2. Customers whose city changed more than twice.**

```sql
SELECT customer_id, COUNT(*) AS changes
FROM (
    SELECT customer_id, city,
           LAG(city) OVER (PARTITION BY customer_id ORDER BY source_commit_ts, source_sequence) AS prev_city
    FROM RAW.CUSTOMER_CDC_LEDGER WHERE operation <> 'DELETE'
)
WHERE prev_city IS NOT NULL AND city <> prev_city
GROUP BY customer_id HAVING COUNT(*) > 2;
```

**C3. Second highest order per customer, and the median order amount.**

```sql
SELECT * FROM ORDERS
QUALIFY DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY order_amount DESC) = 2;

SELECT MEDIAN(order_amount) AS median_amount,
       PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY order_amount) AS p95
FROM ORDERS;
```

**C4. Orders with no matching customer, and customers with no orders.**

```sql
SELECT o.order_id FROM ORDERS o LEFT JOIN CUSTOMERS c USING (customer_id) WHERE c.customer_id IS NULL;
SELECT c.customer_id FROM CUSTOMERS c WHERE NOT EXISTS (SELECT 1 FROM ORDERS o WHERE o.customer_id = c.customer_id);
```

**C5. As-of query: what was each customer's city on 2026-06-30?**

```sql
SELECT customer_id, city
FROM SCHEMATIZED.DIM_CUSTOMER_SCD2
WHERE '2026-06-30'::TIMESTAMP_NTZ >= valid_from AND '2026-06-30'::TIMESTAMP_NTZ < valid_to AND NOT is_deleted;
```

**C6. PySpark: top 3 products by revenue in each region.**

```python
from pyspark.sql import functions as F, Window

rev = sales.groupBy('region', 'product').agg(F.sum('amount').alias('revenue'))
w = Window.partitionBy('region').orderBy(F.desc('revenue'))
top3 = rev.withColumn('rk', F.dense_rank().over(w)).filter('rk <= 3')
```

**C7. Running balance that resets when a new month starts.**

```sql
SELECT account_id, txn_date, amount,
       SUM(amount) OVER (PARTITION BY account_id, DATE_TRUNC('month', txn_date)
                         ORDER BY txn_date, txn_id
                         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS month_running
FROM TXNS;
```

---

## 16.7 Advanced Snowflake questions

**Q28. A nightly pipeline takes 4 hours and the SLA is 2. How do you approach it?**

Measure before changing anything: break the run into tasks and queries, and find the critical path. Then in order: (1) remove unnecessary full refreshes and `SELECT *`; (2) fix the top three queries by elapsed time using Query Profile; (3) parallelise independent branches (dbt `--threads`, Airflow task groups) as warehouse capacity allows; (4) move the slowest steps to a right-sized warehouse; (5) convert expensive batch recomputation to incremental. Resizing is the last step and is tested per query, because many queries do not scale linearly.

```sql
SELECT query_tag, COUNT(*) AS statements, SUM(total_elapsed_time) / 60000 AS minutes
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD('day', -1, CURRENT_TIMESTAMP()) AND warehouse_name = 'ETL_WH'
GROUP BY 1 ORDER BY minutes DESC;
```

**Q29. Clustering depth, overlap and the cost of reclustering.**

A well-clustered column has micro-partitions with narrow, mostly non-overlapping value ranges. `SYSTEM$CLUSTERING_INFORMATION` reports average depth and overlap; lower is better. Automatic clustering consumes serverless credits each time DML disturbs the order, so a high-churn table with a clustering key can cost more than it saves. Use clustering on large, mostly append-ordered tables with stable filter columns, and consider sorting during load (`INSERT ... ORDER BY`) as a free alternative for batch tables.

**Q30. Why use secure views, and what do they cost?**

Secure views hide the view definition and prevent the optimizer from pushing user predicates below the security filter in ways that could leak data through error messages or timing. They are required for sharing. The cost is that some optimizations are disabled, so they can be slower than regular views. Use them for data that crosses a trust boundary (shares, row-level security), not as a default.

**Q31. Design end-to-end PII protection.**

Classify columns (tags such as `PII_TYPE`), attach masking policies to the tags, grant the unmasked role (`ROLE_PII_READER`) narrowly, restrict row visibility with row access policies, keep PII out of marts where analytics does not need it (use tokenised or hashed keys), audit reads through `ACCESS_HISTORY`, and encrypt with customer-managed keys where required. In Bronze, minimise PII at ingestion if the source allows, and apply bucket-level controls. Test policies with a non-privileged role in CI.

**Q32. How do you honour a right-to-be-forgotten (erasure) request across a lake and warehouse?**

It is hard because immutable Bronze, Time Travel and Fail-safe all retain data. A workable design: map the subject to a stable internal key; delete from every serving layer by key; for Bronze files either rewrite affected partitions without the subject (expensive) or use crypto-shredding (encrypt per-subject or per-partition data with keys you can destroy); shorten Time Travel on tables holding sensitive data or accept the documented retention window (Fail-safe cannot be purged on demand, so legal must approve the stated retention). Keep a deletion ledger as evidence, and ensure backups and replays do not resurrect the subject (replays must consult the erasure ledger).

```sql
CREATE TABLE AUDIT.ERASURE_LEDGER (subject_key VARCHAR, requested_at TIMESTAMP_NTZ, completed_at TIMESTAMP_NTZ);

DELETE FROM SCHEMATIZED.DIM_CUSTOMER_SCD2 WHERE customer_id IN (SELECT subject_key::NUMBER FROM AUDIT.ERASURE_LEDGER WHERE completed_at IS NULL);
DELETE FROM RAW.CUSTOMER_CDC_LEDGER WHERE customer_id IN (SELECT subject_key::NUMBER FROM AUDIT.ERASURE_LEDGER WHERE completed_at IS NULL);
UPDATE AUDIT.ERASURE_LEDGER SET completed_at = CURRENT_TIMESTAMP() WHERE completed_at IS NULL;
```

**Q33. Query Acceleration Service, Search Optimization, larger warehouse: how do you choose for a slow scan-heavy query with outliers?**

Query Acceleration offloads parts of eligible scan-heavy queries to serverless compute, which helps when a few big outliers share a warehouse sized for typical queries. A larger warehouse helps everything but costs more for all queries. Search Optimization is for selective lookups, not broad scans. Check `SYSTEM$ESTIMATE_QUERY_ACCELERATION` and compare credits, because the right answer depends on how many queries are outliers.

**Q34. What happens to streams, tasks and policies when you clone a database or schema?**

Cloning copies objects including streams (with their offsets at clone time) and tasks (created suspended), and keeps masking and row access policies attached. Pipes referencing external stages may need re-creation, and cloned tasks must be resumed deliberately. After cloning production for testing, make sure test tasks do not point at production external locations.

**Q35. Snowflake Scripting, JavaScript procedures or Snowpark: when each?**

Snowflake Scripting (SQL) for control flow around SQL (loops over tables, transactions, audit). Snowpark Python when you need Python libraries or complex logic that stays in the warehouse. JavaScript procedures only for legacy code. Orchestration across systems stays in Airflow; do not hide a multi-hour business workflow inside a stored procedure.

**Q36. External tables, Iceberg tables or native tables on GCS data?**

Native tables give the best performance and features, at the cost of loading. External tables read files in place with metadata refresh and no DML, suitable for rarely queried archives. Iceberg tables keep an open table format that Spark and others can also read and write, with Snowflake features partly limited; choose them when multi-engine sharing is a requirement, not by default.

---

## 16.8 Advanced dbt questions

**Q37. How would you structure a very large dbt project with multiple teams?**

Split by domain using folders or separate projects, define `groups` and model `access` (`public`, `protected`, `private`) so teams only depend on published models, enforce `contract` on public models, version breaking changes, and use cross-project refs (dbt Mesh) where available. Keep a shared package for macros and tests. Enforce naming and layering in CI with a linter and `dbt-project-evaluator`.

```yaml
models:
  - name: dim_customer
    group: customer_domain
    access: public
    config: {contract: {enforced: true}}
```

**Q38. How do you find the slowest dbt models and reduce build time?**

```python
import json
run = json.load(open('target/run_results.json'))
slow = sorted(run['results'], key=lambda r: r['execution_time'], reverse=True)[:10]
for r in slow:
    print(round(r['execution_time'], 1), r['unique_id'])
```

Then apply: incremental for large models, `delete+insert` or `merge` with predicates, avoid views stacked many layers deep (materialise hot intermediates), raise `--threads` only if the warehouse has capacity, and use selectors so jobs run only relevant models.

**Q39. How do you handle hard deletes in an incremental dbt model?**

Incremental models do not remove rows that disappeared from the source. Options: carry a soft-delete flag from CDC and filter it downstream; use `delete+insert` over a window and compare with a source key list; run a periodic reconcile model that deletes keys absent from the source; for snapshots set `hard_deletes: invalidate`. State the choice and the lag before the delete takes effect.

**Q40. How should test severity and failure storage be configured?**

```yaml
data_tests:
  - not_null:
      config: {severity: error}
  - accepted_values:
      arguments: {values: ['NEW', 'PAID', 'REFUNDED']}
      config: {severity: warn, warn_if: '>10', error_if: '>100', store_failures: true}
```

Block the pipeline for keys, uniqueness and referential integrity; warn on soft quality rules; store failing rows to a schema so owners can triage without re-running the test. Review warnings weekly or they become noise.

**Q41. Airflow with dbt: BashOperator, Cosmos, or dbt Cloud jobs?**

`BashOperator` running `dbt build` is simplest but gives one opaque task and coarse retries. Cosmos renders the dbt DAG as Airflow tasks, giving per-model retries and visibility with more scheduler load. Triggering dbt Cloud jobs offloads execution and artifacts. Choose by team skills and whether you need per-model retry and lineage in Airflow.

**Q42. Jinja and macro pitfalls.**

Code that calls `run_query` runs at parse time unless guarded by `{% if execute %}`; whitespace control (`{%-`) matters for generated SQL; macros that build table names as strings hide dependencies from the DAG; overly clever macros hurt readability. Prefer simple SQL, add macros only for repeated logic, and always inspect compiled SQL in `target/compiled`.

**Q43. Snapshot, custom SCD2, Dynamic Table or ledger: pick one for tracking customer attribute history.**

| Requirement | Pick |
| --- | --- |
| Source exposes only latest state, daily history is enough | dbt snapshot |
| Full change history, late events, deletes | Custom ledger-driven SCD2 (chapter 7.3) |
| Always-fresh current view, no history | Dynamic table or current-state merge |
| Regulated audit of every change | Immutable ledger plus derived SCD2 |

---

## 16.9 Data engineering architecture and design questions

**Q44. Design a platform supporting 500 source tables without writing 500 pipelines.**

Use a metadata-driven framework: a config table (or YAML) describing each source table, keys, load type, schedule, quality rules and owners, and generic code that reads it. Airflow generates tasks from config; dbt uses macros and packages for repeated patterns (staging generation with `dbt-codegen`).

```python
import yaml
from datetime import datetime
from airflow.decorators import dag, task

TABLES = yaml.safe_load(open('/opt/config/tables.yaml'))['tables']

@dag(schedule='@hourly', start_date=datetime(2026, 1, 1), catchup=False, max_active_runs=1)
def cdc_framework():
    @task(pool='snowflake_loads', max_active_tis_per_dag=8)
    def load_table(cfg: dict, ds=None):
        run_generic_load(cfg, ds)   # same code path for every table
    load_table.expand(cfg=TABLES)

cdc_framework()
```

Benefits: consistent quality and audit behavior, quick onboarding of a new table by adding a config entry. Risks: hard-to-debug generic code, and one bug that affects every table, so invest in tests and canary tables.

**Q45. Design a multi-domain platform (data mesh style).**

Each domain owns its ingestion and models and publishes data products with contracts, owners, SLAs and quality checks. A central platform team provides self-service infrastructure (Terraform modules, CI templates, Snowflake roles, observability), governance (classification, masking, lineage) and a catalog. Shared conformed dimensions (customer, product, date) are owned by one domain and consumed by others through public contracts. Pitfalls: inconsistent KPI definitions, duplicated pipelines, and domains without engineering capacity, so adopt it only when the organisation is big enough to support domain teams.

**Q46. How do you roll out data contracts without blocking teams?**

Start with the most critical datasets, define schema, semantics, freshness and ownership in YAML in the producer's repository, run a compatibility check in producer CI (breaking versus additive changes), alert consumers on proposed breaking changes, and provide a deprecation window with versioned datasets. Measure adoption (percentage of critical datasets under contract) and incidents prevented.

**Q47. How do you charge costs back to teams?**

One warehouse per team or workload where practical, `QUERY_TAG` for shared warehouses, `WAREHOUSE_METERING_HISTORY` and tagged queries joined into a monthly report, storage by schema/database, and budgets or resource monitors per team. Chargeback changes behavior, so publish it and agree on rules (shared platform costs allocated by usage share).

**Q48. Billing events must be counted exactly once. How do you design for it?**

Assign a deterministic event ID at the source, store it with a unique constraint or use `MERGE` on it at the first durable store, process with at-least-once delivery, and derive downstream counts from the deduplicated ledger. Reconcile counts and amounts daily against the source system of record, and make corrections as reversal entries rather than updates (chapter 8.5).

**Q49. Backpressure: the downstream system is slower than the stream.**

Bound the input (`maxOffsetsPerTrigger`, Pub/Sub flow control), buffer in the durable broker (not in memory), scale consumers up to the partition limit, and fix the slow sink (batch writes, larger files). Alert on lag, not CPU. Never drop events to catch up; if latency becomes unacceptable, degrade by deferring non-critical processing.

---

## 16.10 Lead-level and behavioral questions (outline answers)

| Question | What a strong answer covers |
| --- | --- |
| Tell me about a major production incident you led | Detection, blast radius, mitigation, root cause, permanent fix, what you changed in process |
| How do you decide build versus buy | Total cost, team skills, lock-in, time to value, operational burden |
| How do you manage technical debt in pipelines | Make it visible, link to incidents and cost, allocate recurring capacity, prioritise by risk |
| A stakeholder wants real-time data. What do you do | Ask which decision needs it, quantify value, offer micro-batch first, state cost and operational trade-off |
| How do you onboard and review engineers | Standards, templates, PR checklists (idempotency, tests, observability), pairing, blameless post-mortems |
| How do you prioritise reliability versus features | Error budgets and SLAs, tie reliability work to business impact |

---

# Appendix A. Chapter 15: Production Troubleshooting Runbooks

| Symptom | First checks | Typical root cause |
| --- | --- | --- |
| GCS files not appearing in RAW | `LIST @stage`; `COPY_HISTORY`; `SYSTEM$PIPE_STATUS`; Pub/Sub subscription backlog | Missing IAM on the Snowflake service account, wrong path or file format, pipe paused, notification not wired |
| COPY loads zero rows | `VALIDATE` or `COPY ... VALIDATION_MODE = RETURN_ERRORS`; check load metadata | Files already loaded (idempotency), `ON_ERROR` skipping, pattern mismatch |
| Task not running | `TASK_HISTORY`, `SHOW TASKS` state | Task suspended, stream has no data, warehouse missing, privilege `EXECUTE TASK` revoked |
| Stream is stale | `SHOW STREAMS` column `STALE` | Consumer paused longer than the source table's retention |
| Duplicate current rows in SCD2 | Integrity tests (section 7.3) | Concurrent runs, non-atomic expire and insert, same-timestamp events |
| Spark stage stuck on one task | Spark UI stage summary; key distribution | Skew, non-splittable file, GC pressure |
| Streaming job restarts and reprocesses | Checkpoint path, `startingOffsets`, query ID | Checkpoint deleted or moved, logic change |
| Airflow DAG runs a day late or backfills unexpectedly | Logical date, `catchup`, `schedule` | Misunderstanding data intervals, `catchup=True` after downtime |
| Dashboard numbers changed overnight | `QUERY_HISTORY` for the object; Time Travel diff | Late-arriving data, restatement, a model refresh |

```sql
-- Snowpipe health and recent load errors
SELECT SYSTEM$PIPE_STATUS('RAW.CUSTOMER_PIPE');

SELECT file_name, status, first_error_message, first_error_line_number, row_count, error_count
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'RAW_CUSTOMERS', START_TIME => DATEADD('hour', -6, CURRENT_TIMESTAMP())))
WHERE error_count > 0 OR status <> 'Loaded';

-- Task failures
SELECT name, state, error_message, scheduled_time
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(SCHEDULED_TIME_RANGE_START => DATEADD('day', -1, CURRENT_TIMESTAMP())))
WHERE state = 'FAILED' ORDER BY scheduled_time DESC;

-- Time Travel diff after a suspicious change: what rows changed in the last hour?
(SELECT * FROM SCHEMATIZED.FACT_ORDERS EXCEPT SELECT * FROM SCHEMATIZED.FACT_ORDERS AT (OFFSET => -3600))
UNION ALL
(SELECT * FROM SCHEMATIZED.FACT_ORDERS AT (OFFSET => -3600) EXCEPT SELECT * FROM SCHEMATIZED.FACT_ORDERS);
```

```sql
-- Corrected governance query: who read the email column in the last 7 days
SELECT ah.query_start_time, ah.user_name, ah.query_id
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah,
     LATERAL FLATTEN(INPUT => ah.base_objects_accessed) o,
     LATERAL FLATTEN(INPUT => o.value:columns) c
WHERE ah.query_start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  AND UPPER(o.value:objectName::STRING) = 'ENTERPRISE_DW.SCHEMATIZED.DIM_CUSTOMER'
  AND UPPER(c.value:columnName::STRING) = 'EMAIL';
```

---

# Closing notes

The pattern running through every chapter is the same set of ideas: keep an immutable raw record, order by source position and not arrival time, make every write replay-safe, advance watermarks last, reconcile continuously, and be able to rebuild any layer from Bronze. If you can explain those six ideas and demonstrate them with code like the examples above, you can answer almost any senior data engineering question from first principles.

Verify version-sensitive syntax (dbt 1.9 and 1.10 config changes, Snowflake feature availability by edition, Terraform provider versions, Airflow 3 import paths) against your installed versions and Snowflake account edition before using it in production.
