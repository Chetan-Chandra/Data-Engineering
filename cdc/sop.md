# SOP: End-to-End CDC Pipeline
## SQL Server → Debezium → Kafka → Python → GCS → BigQuery

**Purpose:** Capture changes happening in a SQL Server table, transfer those changes to Google Cloud Storage, and process them in BigQuery to maintain both CDC history and the latest customer records.

**Project:** AdventureWorksDW2022  
**Source table:** dbo.CDC_LearningCustomer  
**GCP project:** electric-tesla-507710-k2  
**GCS bucket:** advworks-dev-ingestion  
**BigQuery dataset:** cdc_raw

---

# 1. Understand the architecture first

The pipeline has these components:

1. SQL Server CDC captures database changes.
2. Debezium reads those changes and creates CDC events.
3. Kafka stores those events in a topic.
4. Python reads events from Kafka.
5. Python uploads events to GCS as JSONL files.
6. BigQuery loads those files into a staging table.
7. BigQuery creates a structured CDC history table.
8. BigQuery creates a current-state table containing the latest customer records.

**Important:** Kafka is not directly updating BigQuery. Python transfers the events to GCS, and BigQuery processes them separately.

---

# 2. Step 1 — Prepare SQL Server

We used:

- SQL Server 2022 Developer Edition.
- SQL Server Management Studio (SSMS).
- Database: AdventureWorksDW2022.
- Table: dbo.CDC_LearningCustomer.

Our table contains:

| Column | Purpose |
|---|---|
| CustomerID | Unique customer ID |
| CustomerName | Customer name |
| City | Customer city |
| Email | Customer email |
| UpdatedAt | Last updated time |

The primary key is CustomerID.

## Enable CDC at database level

Run in SSMS:

```sql
USE AdventureWorksDW2022;
GO

EXEC sys.sp_cdc_enable_db;
GO
```

This enables Change Data Capture for the database.

## Enable CDC at table level

```sql
USE AdventureWorksDW2022;
GO

EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'CDC_LearningCustomer',
    @role_name = NULL,
    @supports_net_changes = 0;
GO
```

This enables CDC for our customer table.

SQL Server creates CDC metadata and a change table.

The capture instance used in our setup was:

```text
dbo_CDC_LearningCustomer
```

The associated change table is:

```text
cdc.dbo_CDC_LearningCustomer_CT
```

## Understand how SQL Server CDC works

When a record is inserted, updated or deleted:

1. SQL Server writes the transaction to its transaction log.
2. SQL Server Agent runs the CDC capture job.
3. The capture job reads committed changes from the transaction log.
4. It stores the changes in CDC change tables.

The capture job runs periodically. Our configured polling interval was 5 seconds.

**Important:** Debezium reads the CDC information; it does not itself capture changes from the SQL Server transaction log in this configuration.

---

# 3. Step 2 — Start SQL Server Agent

SQL Server Agent is responsible for running the CDC capture job.

In SSMS:

1. Connect to SQL Server.
2. Expand SQL Server Agent.
3. Right-click SQL Server Agent.
4. Select Start.

Check the job status:

```sql
EXEC msdb.dbo.sp_help_job;
```

We verified that the CDC capture job was running.

---

# 4. Step 3 — Debezium and Kafka setup

We used Docker containers.

Our setup was already created in:

```text
C:\Users\Admin\debezium-lab
```

The main components were:

| Component | Container |
|---|---|
| Kafka broker | dbz-kafka |
| Kafka Connect | dbz-connect |

The important connection details:

```text
Kafka internal address: kafka:9092
Kafka external address: localhost:29092
Kafka Connect REST API: localhost:8083
```

Kafka Connect runs the Debezium SQL Server connector.

## Check Docker containers

```powershell
docker ps
```

Both Kafka and Kafka Connect should be running.

## Check Kafka topics

```powershell
docker exec -it dbz-kafka /kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
```

Our CDC topic:

```text
sqlserver_lab.AdventureWorksDW2022.dbo.CDC_LearningCustomer
```

## Check Debezium connector status

```powershell
curl.exe http://localhost:8083/connectors/sqlserver-cdc/status
```

Expected status:

```text
RUNNING
```

## Retrieve the actual connector configuration

```powershell
curl.exe http://localhost:8083/connectors/sqlserver-cdc/config
```

This displays the configuration of the connector already registered in your environment.

Do not register another connector unnecessarily. We already had a working connector named sqlserver-cdc.

## Understand Debezium operations

Debezium converts source changes into events.

| Operation | Meaning |
|---|---|
| r | Snapshot/read |
| c | Create/insert |
| u | Update |
| d | Delete |

An example event:

```json
{
  "payload": {
    "before": null,
    "after": {
      "CustomerID": 12,
      "CustomerName": "Test Customer",
      "City": "Kolkata"
    },
    "op": "c"
  }
}
```

Meaning:

- before = null: no previous record.
- after = new customer information.
- op = c: insert operation.

For an update, before contains the old record and after contains the new record.

For a delete, before contains the deleted record and after is null.

---

# 5. Step 4 — Understand Kafka

Kafka stores events in topics.

Our topic:

```text
sqlserver_lab.AdventureWorksDW2022.dbo.CDC_LearningCustomer
```

Kafka message details:

| Field | Meaning |
|---|---|
| Topic | Where events are stored |
| Key | CustomerID |
| Value | Debezium event |
| Partition | Topic partition |
| Offset | Position of a message within a partition |

## Read Kafka messages

```powershell
docker exec -it dbz-kafka /kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic sqlserver_lab.AdventureWorksDW2022.dbo.CDC_LearningCustomer --from-beginning --property print.key=true --property print.offset=true
```

Press Ctrl+C to stop.

We observed offsets 0 through 9.

The important concept:

Kafka stores messages. Python consumers read messages from Kafka.

Kafka does not automatically update the downstream database.

## Understand tombstones

When Debezium publishes a delete event, it can also publish a Kafka tombstone.

A tombstone has:

- A message key.
- A null message value.

It is used by Kafka log compaction.

It is not another SQL Server delete operation.

We preserved it as a separate record type in GCS, but excluded it from business CDC history.

---

# 6. Step 5 — Create the Python environment

Our working folder:

```text
C:\Users\Admin\Documents\Courses\Pipeline\Data-pipeline-end-to-end\cdc
```

Open PowerShell and navigate to the folder:

```powershell
cd C:\Users\Admin\Documents\Courses\Pipeline\Data-pipeline-end-to-end\cdc
```

Create a virtual environment if not already created:

```powershell
python -m venv .venv
```

Activate it:

```powershell
.venv\Scripts\Activate.ps1
```

Install required libraries:

```powershell
pip install confluent-kafka==2.16.1 google-cloud-storage==3.17.1
```

Verify installation:

```powershell
pip show confluent-kafka
pip show google-cloud-storage
```

## Understand the libraries

- confluent-kafka: Connects Python to Kafka and reads messages.
- google-cloud-storage: Connects Python to GCS and uploads files.

---

# 7. Step 6 — Configure Google Cloud authentication

We used GCP project:

```text
electric-tesla-507710-k2
```

Check the active project:

```powershell
gcloud config get-value project
```

Set it if required:

```powershell
gcloud config set project electric-tesla-507710-k2
```

Authenticate:

```powershell
gcloud auth application-default login
```

Verify authentication:

```powershell
gcloud auth application-default print-access-token | Out-Null

if ($LASTEXITCODE -eq 0) {
    "ADC authentication successful"
} else {
    "ADC authentication needs setup"
}
```

ADC means Application Default Credentials.

The Python Google Cloud library uses these credentials to authenticate with GCS.

---

# 8. Step 7 — Prepare the GCS bucket

Our existing bucket:

```text
gs://advworks-dev-ingestion
```

CDC destination:

```text
gs://advworks-dev-ingestion/cdc/customer_events/
```

Check the bucket:

```powershell
gcloud storage ls gs://advworks-dev-ingestion/
```

Check the CDC folder:

```powershell
gcloud storage ls gs://advworks-dev-ingestion/cdc/customer_events/
```

GCS uses object names rather than traditional folders. The slash-separated prefix makes the objects appear organized into folders.

Our files were organized like this:

```text
cdc/
  customer_events/
    partition=0/
      offsets-000000000000-000000000004.jsonl
      offsets-000000000005-000000000008.jsonl
      offsets-000000000009-000000000009.jsonl
```

Each file contains a batch of CDC events.

---

# 9. Step 8 — Python Kafka-to-GCS consumer

File:

```text
consumer\kafka_to_gcs.py
```

This is the most important Python component.

Its responsibilities:

1. Connect to Kafka.
2. Read events from the CDC topic.
3. Extract customer information.
4. Identify insert, update, delete and tombstone events.
5. Convert records into JSONL.
6. Upload files to GCS.
7. Commit Kafka offsets after successful upload.

## Kafka configuration used

```python
KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:29092",
    "group.id": "cdc-to-gcs-consumer",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
}
```

Topic:

```python
TOPIC = "sqlserver_lab.AdventureWorksDW2022.dbo.CDC_LearningCustomer"
```

Important settings:

- bootstrap.servers: Kafka broker address.
- group.id: Consumer group name.
- auto.offset.reset: Start from earliest available message if no committed offset exists.
- enable.auto.commit=False: We manually commit offsets after GCS upload.

## How the consumer works

The consumer receives Kafka messages.

For a normal event:

```python
import json

key = json.loads(message.key().decode("utf-8"))
event = json.loads(message.value().decode("utf-8"))

customer_id = key["payload"]["CustomerID"]

payload = event["payload"]

operation = payload["op"]
before_data = payload["before"]
after_data = payload["after"]
```

Operation mapping:

```python
operation_mapping = {
    "r": "SNAPSHOT",
    "c": "INSERT",
    "u": "UPDATE",
    "d": "DELETE"
}
```

For a tombstone:

```python
if message.value() is None:
    record_type = "tombstone"
```

For a normal CDC event:

```python
record_type = "cdc_event"
```

The Python consumer stores the event along with its Kafka metadata.

Example output structure:

```json
{
  "record_type": "cdc_event",
  "customer_id": 12,
  "event": {
    "payload": {
      "op": "c",
      "before": null,
      "after": {
        "CustomerID": 12,
        "CustomerName": "Test Customer",
        "City": "Kolkata"
      }
    }
  },
  "kafka_topic": "sqlserver_lab.AdventureWorksDW2022.dbo.CDC_LearningCustomer",
  "kafka_partition": 0,
  "kafka_offset": 9
}
```

## Batching and uploading

Our consumer used:

```text
Batch size: 5 records
Flush interval: 10 seconds
File format: JSONL
```

JSONL means JSON Lines: one JSON object per line.

The important upload logic is:

```python
blob.upload_from_string(
    jsonl_data + "\n",
    content_type="application/x-ndjson"
)
```

After successful upload:

```python
consumer.commit(
    offsets=[
        TopicPartition(
            TOPIC,
            partition,
            last_offset + 1
        )
    ],
    asynchronous=False
)
```

The offset is committed only after the GCS upload succeeds.

This reduces the risk of losing an event if the upload fails.

**Important:** This is the reference logic for the consumer, not a replacement for your already-working complete Python file. Keep your existing working `consumer\kafka_to_gcs.py` unless you intentionally change it.

## Run the Python consumer

```powershell
python consumer\kafka_to_gcs.py
```

Expected output:

```text
Listening to Kafka topic:
sqlserver_lab.AdventureWorksDW2022.dbo.CDC_LearningCustomer

GCS destination:
gs://advworks-dev-ingestion/cdc/customer_events/

Received: Customer=12, Offset=9

GCS upload successful:
gs://advworks-dev-ingestion/cdc/customer_events/partition=0/offsets-000000000009-000000000009.jsonl

Kafka offset committed: 10
```

This confirms that Python successfully read the event, uploaded it and committed the offset.

## Verify GCS files

```powershell
gcloud storage ls gs://advworks-dev-ingestion/cdc/customer_events/partition=0/
```

To inspect a file:

```powershell
gcloud storage cat gs://advworks-dev-ingestion/cdc/customer_events/partition=0/offsets-000000000009-000000000009.jsonl
```

---

# 10. Step 9 — Prepare BigQuery

We used BigQuery in the same GCP project.

Check BigQuery API:

```powershell
gcloud services list --enabled --project=electric-tesla-507710-k2 --filter="config.name:bigquery.googleapis.com"
```

List datasets:

```powershell
bq ls --project_id=electric-tesla-507710-k2
```

We created:

```text
cdc_raw
```

Dataset location:

```text
northamerica-northeast1
```

Create dataset:

```powershell
bq --location=northamerica-northeast1 mk --dataset electric-tesla-507710-k2:cdc_raw
```

Do not run this again if the dataset already exists.

## Create the staging table

Staging stores the raw JSONL structure.

```powershell
bq mk --table --project_id=electric-tesla-507710-k2 cdc_raw.customer_events_stage record_type:STRING,customer_id:INT64,event:JSON,kafka_topic:STRING,kafka_partition:INT64,kafka_offset:INT64
```

Columns:

| Column | Meaning |
|---|---|
| record_type | CDC event or tombstone |
| customer_id | Customer key |
| event | Complete Debezium JSON event |
| kafka_topic | Kafka topic |
| kafka_partition | Kafka partition |
| kafka_offset | Kafka offset |

## Create the CDC history table

```powershell
bq mk --table --project_id=electric-tesla-507710-k2 cdc_raw.customer_events customer_id:INT64,operation:STRING,before_data:JSON,after_data:JSON,kafka_topic:STRING,kafka_partition:INT64,kafka_offset:INT64,event_timestamp:TIMESTAMP
```

This table stores structured CDC history.

---

# 11. Step 10 — Load GCS files into BigQuery staging

We used the BigQuery native load command.

Initial load:

```powershell
bq load --source_format=NEWLINE_DELIMITED_JSON electric-tesla-507710-k2:cdc_raw.customer_events_stage "gs://advworks-dev-ingestion/cdc/customer_events/partition=0/*.jsonl"
```

This loaded the original files.

For subsequent new files, we used append mode:

```powershell
bq load --source_format=NEWLINE_DELIMITED_JSON --noreplace electric-tesla-507710-k2:cdc_raw.customer_events_stage "gs://advworks-dev-ingestion/cdc/customer_events/partition=0/offsets-000000000009-000000000009.jsonl"
```

**Important:** Do not repeatedly load the same files without controlling duplicates. Loading the same files again in append mode creates duplicate staging rows.

## Verify staging

```powershell
bq query --use_legacy_sql=false "SELECT record_type, COUNT(*) AS total FROM cdc_raw.customer_events_stage GROUP BY record_type"
```

After our first load:

```text
cdc_event    8
tombstone    1
```

After customer 12 was inserted and loaded:

```text
cdc_event    9
tombstone    1
```

Total: 10 staging records.

---

# 12. Step 11 — Transform staging into CDC history

Now we convert the raw Debezium JSON into structured columns.

We use:

- JSON_VALUE to extract scalar values.
- JSON_QUERY to extract JSON objects.
- TIMESTAMP_MILLIS to convert event timestamps.
- MERGE to insert only events that do not already exist.

Run in PowerShell:

```powershell
@'
MERGE `electric-tesla-507710-k2.cdc_raw.customer_events` T
USING (
    SELECT
        customer_id,
        JSON_VALUE(event, '$.payload.op') AS operation,
        JSON_QUERY(event, '$.payload.before') AS before_data,
        JSON_QUERY(event, '$.payload.after') AS after_data,
        kafka_topic,
        kafka_partition,
        kafka_offset,
        TIMESTAMP_MILLIS(
            SAFE_CAST(JSON_VALUE(event, '$.payload.ts_ms') AS INT64)
        ) AS event_timestamp
    FROM `electric-tesla-507710-k2.cdc_raw.customer_events_stage`
    WHERE record_type = 'cdc_event'
) S
ON T.kafka_topic = S.kafka_topic
AND T.kafka_partition = S.kafka_partition
AND T.kafka_offset = S.kafka_offset

WHEN NOT MATCHED THEN
INSERT (
    customer_id,
    operation,
    before_data,
    after_data,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    event_timestamp
)
VALUES (
    S.customer_id,
    S.operation,
    S.before_data,
    S.after_data,
    S.kafka_topic,
    S.kafka_partition,
    S.kafka_offset,
    S.event_timestamp
);
'@ | bq query --use_legacy_sql=false
```

## Why do we use topic + partition + offset?

Together, these identify a Kafka record's position.

If we run the MERGE again, existing records are not inserted again.

This is called idempotent processing.

## Verify CDC history

```powershell
bq query --use_legacy_sql=false "SELECT operation, COUNT(*) AS total FROM cdc_raw.customer_events GROUP BY operation ORDER BY operation"
```

Our final result:

| Operation | Count |
|---|---:|
| c | 3 |
| d | 1 |
| r | 3 |
| u | 1 |
| Total | 8 |

After inserting customer 12:

| Operation | Count |
|---|---:|
| c | 4 |
| d | 1 |
| r | 3 |
| u | 1 |
| Total | 9 |

The history table contains 9 actual change events. Tombstones are excluded.

---

# 13. Step 12 — Create the current-state table

The CDC history table stores every change.

But business users often need the latest customer information.

For that, we created:

```text
cdc_raw.customer_current
```

It contains one latest active record per customer.

The logic:

1. Find the latest event for every customer.
2. If the latest event is delete, exclude that customer.
3. Extract the latest customer information.
4. Update an existing customer or insert a new customer.

The latest event is identified using Kafka offset because our lab uses one Kafka partition.

## Refresh current-state table

Run this complete script:

```powershell
@'
CREATE TEMP TABLE latest_customers AS
SELECT
    customer_id,
    operation,
    after_data,
    kafka_offset,
    event_timestamp
FROM `electric-tesla-507710-k2.cdc_raw.customer_events`
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY customer_id
    ORDER BY kafka_offset DESC
) = 1;

MERGE `electric-tesla-507710-k2.cdc_raw.customer_current` T
USING (
    SELECT
        customer_id,
        JSON_VALUE(after_data, '$.CustomerName') AS customer_name,
        JSON_VALUE(after_data, '$.City') AS city,
        JSON_VALUE(after_data, '$.Email') AS email,
        TIMESTAMP_MICROS(
            DIV(
                SAFE_CAST(JSON_VALUE(after_data, '$.UpdatedAt') AS INT64),
                1000
            )
        ) AS updated_at,
        kafka_offset AS last_kafka_offset,
        event_timestamp AS last_event_timestamp
    FROM latest_customers
    WHERE operation != 'd'
) S
ON T.customer_id = S.customer_id

WHEN MATCHED THEN UPDATE SET
    customer_name = S.customer_name,
    city = S.city,
    email = S.email,
    updated_at = S.updated_at,
    last_kafka_offset = S.last_kafka_offset,
    last_event_timestamp = S.last_event_timestamp

WHEN NOT MATCHED THEN INSERT (
    customer_id,
    customer_name,
    city,
    email,
    updated_at,
    last_kafka_offset,
    last_event_timestamp
)
VALUES (
    S.customer_id,
    S.customer_name,
    S.city,
    S.email,
    S.updated_at,
    S.last_kafka_offset,
    S.last_event_timestamp
);
'@ | bq query --use_legacy_sql=false
```

### Understand the timestamp conversion

Debezium represents our source UpdatedAt field as nanoseconds.

BigQuery TIMESTAMP_MICROS expects microseconds.

Therefore:

```sql
DIV(updated_at_nanoseconds, 1000)
```

converts nanoseconds into microseconds.

Then:

```sql
TIMESTAMP_MICROS(...)
```

converts that value into a BigQuery timestamp.

Do not confuse:

- after_data.UpdatedAt: timestamp from the original SQL Server record.
- payload.ts_ms: Debezium event timestamp.

They represent different things.

## Verify the final table

```powershell
bq query --use_legacy_sql=false "SELECT * FROM cdc_raw.customer_current ORDER BY customer_id"
```

Our final result:

| Customer ID | Customer Name | City |
|---:|---|---|
| 1 | Amit Sharma | Pune |
| 5 | Sneha Roy | Chennai |
| 6 | Arjun Das | Kolkata |
| 10 | Rahul Das | Kolkata |
| 11 | Priya Sharma | Delhi |
| 12 | Test Customer | Kolkata |

There are 6 active customers.

---

# 14. Step 13 — Real-time CDC test we performed

This was our end-to-end validation.

We inserted a new customer into SQL Server:

```sql
USE AdventureWorksDW2022;
GO

INSERT INTO dbo.CDC_LearningCustomer
(
    CustomerID,
    CustomerName,
    City,
    Email,
    UpdatedAt
)
VALUES
(
    12,
    'Test Customer',
    'Kolkata',
    'test12@gmail.com',
    GETDATE()
);
GO
```

SQL Server confirmed:

```text
(1 row affected)
```

Then we checked Kafka.

Debezium captured the insert and Kafka received it at:

```text
Offset: 9
```

The event had:

```text
CustomerID: 12
Operation: c
Before: null
After: Test Customer, Kolkata
Snapshot: false
```

Then we started Python:

```powershell
python consumer\kafka_to_gcs.py
```

Python confirmed:

```text
Received: Customer=12, Offset=9

GCS upload successful:
gs://advworks-dev-ingestion/cdc/customer_events/partition=0/offsets-000000000009-000000000009.jsonl

Kafka offset committed: 10
```

Next, we loaded the new file into BigQuery staging:

```powershell
bq load --source_format=NEWLINE_DELIMITED_JSON --noreplace electric-tesla-507710-k2:cdc_raw.customer_events_stage "gs://advworks-dev-ingestion/cdc/customer_events/partition=0/offsets-000000000009-000000000009.jsonl"
```

We verified that customer 12 reached staging.

Then we ran the CDC history MERGE.

Result:

```text
Number of affected rows: 1
```

Finally, we refreshed the current-state table.

Customer 12 appeared successfully:

```text
12 | Test Customer | Kolkata | test12@gmail.com
```

**This confirmed that a new source database insert could travel through the entire pipeline and reach BigQuery.**

---

# 15. Important concepts to remember for interviews

| Concept | Simple explanation |
|---|---|
| CDC | Captures changes in a source database |
| Transaction log | Records database transactions |
| SQL Server Agent | Runs the CDC capture job |
| Debezium | Reads changes and creates events |
| Kafka Connect | Runs connectors such as Debezium |
| Kafka topic | Stores events |
| Kafka key | Identifies the customer record |
| Kafka offset | Identifies a message position in a partition |
| Kafka consumer | Reads events from Kafka |
| Tombstone | Kafka message with a null value |
| JSONL | One JSON object per line |
| GCS | Stores raw CDC files |
| BigQuery staging | Holds incoming raw records |
| CDC history | Stores every captured change |
| Current-state table | Stores the latest active record |
| MERGE | Inserts or updates records based on matching conditions |
| Idempotency | Reprocessing does not create duplicate history records |

---

# 16. How this architecture connects to Snowflake

The same CDC ingestion approach can feed Snowflake instead of BigQuery.

The architecture would become:

```text
SQL Server
    |
    v
SQL Server CDC
    |
    v
Debezium
    |
    v
Kafka
    |
    v
Python Consumer
    |
    v
GCS
    |
    v
Snowflake RAW / LANDING
    |
    v
Snowflake CDC_HISTORY
    |
    v
Snowflake TARGET_CUSTOMER
```

The Snowflake components could be:

| Layer | Purpose |
|---|---|
| GCS | Stores raw CDC JSONL files |
| Snowpipe | Automatically loads new files |
| RAW/LANDING | Stores incoming events |
| CDC_HISTORY | Maintains change history |
| dbt or SQL MERGE | Processes changes |
| TARGET_CUSTOMER | Maintains latest business data |

The BigQuery part of this lab is a learning implementation of the processing logic. It is not mandatory to keep both BigQuery and Snowflake in a production architecture.

---

# 17. Important production considerations

Our lab was successful, but these are areas to improve before calling it production-ready:

1. **Multiple Kafka partitions:** Offsets from different partitions cannot be compared as one global sequence. Use source log sequence metadata and a reliable ordering strategy.
2. **Duplicate processing:** Keep idempotent processing in the destination.
3. **Failed GCS uploads:** Do not commit Kafka offsets if the upload fails.
4. **Reprocessing:** Track processed files and events to avoid duplicates.
5. **Delete handling:** Ensure a delete removes or marks the target record correctly.
6. **Schema changes:** Handle source table column additions and data type changes.
7. **Monitoring:** Monitor consumer lag, failed uploads, failed loads and processing delays.
8. **Security:** Use least-privilege service accounts and protect credentials.
9. **Automation:** Automate GCS-to-destination loading and SQL processing rather than running commands manually.
10. **Recovery:** Define how to replay events and rebuild the current-state table.

One important detail: our current-state refresh uses a single Kafka partition's offset order. It is suitable for this lab but needs a more robust source-ordering strategy for a multi-partition production pipeline.

---

# 18. Quick revision — Exact order of execution

Use this list whenever you revise the project.

1. Enable CDC on SQL Server database.
2. Enable CDC on the customer table.
3. Start SQL Server Agent and verify the capture job.
4. Start Docker containers for Kafka and Kafka Connect.
5. Verify the Debezium connector is RUNNING.
6. Verify the Kafka topic exists.
7. Prepare Python virtual environment.
8. Install confluent-kafka and google-cloud-storage.
9. Configure GCP authentication.
10. Prepare the GCS bucket and destination prefix.
11. Start the Python Kafka consumer.
12. Read CDC events from Kafka.
13. Convert events into JSONL batches.
14. Upload JSONL files to GCS.
15. Commit Kafka offsets after successful uploads.
16. Create the BigQuery dataset.
17. Create the staging table.
18. Create the CDC history table.
19. Load JSONL files into staging.
20. Run MERGE to populate CDC history.
21. Create and refresh the current-state table.
22. Insert a new SQL Server customer to test the complete flow.
23. Verify the new customer in Kafka, GCS, staging, CDC history and current-state table.

**Final understanding:**

CDC history tells us what changed over time.

Current state tells us what the data looks like now.

We keep both because they serve different business purposes.

**End of SOP.**