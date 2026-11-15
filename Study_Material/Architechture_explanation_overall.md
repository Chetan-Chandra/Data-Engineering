Complete architecture: Employee data pipeline from Boomi to Snowflake SCD Type 2
Based on all the code and configuration you have shared, we can now explain the complete DIM_EMPLOYEE pipeline: how data is extracted from the source, moved through GCS, loaded into Snowflake using Snowpipe, transformed using dbt, and finally historized using a dbt snapshot.

I will leave EMPLOYEE_ENDDATED out, as requested.

The most important distinction to understand is that your architecture has two different mechanisms for handling data changes:
1. Timestamp-based incremental extraction in Airflow, which retrieves changed employee records from Boomi.
2. SCD Type 2 history tracking in dbt snapshots, which preserves historical versions of employee records in Snowflake.
These are related, but they are not the same thing.

1. Architecture at a glance
Employee data flow:
________________________________________________________________________________________________________________________________________
|                         SOURCE SYSTEM
|                   Boomi DataHub / MDH API
|                Employee records and updatedDate
|                                ↓
|                REST API · Apigee OAuth2 · XML request/response
|
|                     ORCHESTRATION & EXTRACTION
|                         Apache Airflow
|                Daily DAG · Delta checkpoint in XCom · Pagination · JSON serialization
|                                ↓
|                       GCS target connector    
|
|                           RAW STORAGE
|                       Google Cloud Storage
|                       raw/MDH/employee/
|                Full-load and incremental JSON files
|
|
|                        EVENT NOTIFICATION
|                GCS → Pub/Sub topic → subscription
|                       New object notification
|                                ↓
|                Snowflake GCP notification integration
|
|                       AUTOMATED INGESTION
|                    Snowpipe — AUTO_INGEST = TRUE
|                External stage · JSON file format · COPY INTO                                   
|                                ↓
|                          SNOWFLAKE LANDING
|                   ENTERPRISE_LANDING.MDH.EMPLOYEE
|                      DATA VARIANT + file metadata
________________________________________________________________________________________________________________________________________
|
|    DBT TRANSFORMATION LAYERS
|
|                        1. Prepare — PREP_EMPLOYEE
|               JSON parsing, typing, deduplication, full-load baseline filtering
|                                 ↓
|                        2. Normalize — NORM_EMPLOYEE
|              Reference-data enrichment and standardization
|                                 ↓
|                        3. Schematize — DIM_EMPLOYEE
|              Employee dimension, surrogate key, manager attributes
________________________________________________________________________________________________________________________________________
|
|                                 ↓
|                      DBT SNAPSHOT — SCD TYPE 2
|                        DIM_EMPLOYEE_SNAPSHOT
|              Daily snapshot · check strategy · check_cols: all
|
|                                 ↓
|                        HISTORICAL EMPLOYEE DIMENSION
|                      Previous versions + current version|
|                  DBT_VALID_FROM · DBT_VALID_TO · DBT_SCD_ID
________________________________________________________________________________________________________________________________________

There is also a separate Snowflake cleanup Task that removes older full-load generations from the landing table. It does not perform SCD Type 2 history management.

2. Stage 1 — Extract employee data from Boomi using Airflow
DAG configuration
Your DAG is:

boomi.stream.mdh.py

The relevant configuration is:
with DAG(    dag_id='boomi.stream.mdh',    start_date=datetime(2025, 1, 1, tzinfo=local_tz),    schedule='0 5 * * *',    catchup=False,    max_active_runs=1,    tags=['scheduled', 'apigee', 'boomi', 'stream', 'gcs'],) as dag:



This means:
Setting	Meaning
DAG ID	boomi.stream.mdh
Schedule	Daily at 05:00 in the configured DAG timezone
catchup=False	Does not automatically run all missed historical schedules
max_active_runs=1	Only one active DAG run at a time
Task	extract_mdh

The source connector is:
ApigeeBoomiDataHubSourceConnector(    conn_id='apigee',    endpoint=Variable.get("BOOMI_MDH_URL_PATH"),    method='POST',    payload="""<RecordQueryRequest limit="200" offsetToken="">    </RecordQueryRequest>""",    skip_if_empty=True,    query_mode='delta',    paths_to_exclude=[        'Fields.people.government_ids.canada.sin',        'Fields.people.hired_date',        'Fields.people.gender'    ])



The data flow is:
1. Airflow starts the DAG.
2. It connects to Apigee using the configured Airflow connection.
3. Apigee provides access to the Boomi DataHub API.
4. The connector sends an XML POST request.
5. The API returns employee records.
6. The connector retrieves additional pages using offsetToken.
7. Selected fields are excluded from the extracted payload.
The request has a page limit of 200. It does not mean the entire extraction is limited to 200 records.
Full load versus incremental load
Your custom connector uses:
query_mode='delta'



But the first run and subsequent runs behave differently.

First run:
- No previous XCom checkpoint exists.
- The connector performs a full load.
- It retrieves the available employee records through pagination.
Subsequent runs:
- It retrieves the previous checkpoint from XCom.
- It adds one second to the checkpoint.
- It constructs an API filter using updatedDate.
- It retrieves records between that timestamp and the current data interval end.
Conceptually:
updatedDate >= previous_checkpoint + 1 second
AND
updatedDate <= current_data_interval_end


This is timestamp-based incremental extraction, not proven transaction-log CDC.
3. Stage 2 — How Airflow manages the incremental checkpoint
This is one of the most important parts of your implementation.

Your custom source connector tracks the maximum updatedDate returned by the API:
self._current_max_value = max(    record_updated_dates)



The BaseConnector initializes it:
self._current_max_value = None



The Extraction Operator generates this XCom key:
self.xcom_key = f"{context['dag'].dag_id}_delta"



For your DAG:
boomi.stream.mdh_delta


How the checkpoint cycle works
At the beginning of a run:

The source connector calls:
_get_last_checkpoint(context)



This retrieves the previous checkpoint from Airflow XCom.

During extraction:

The connector calculates the maximum updatedDate across all fetched API pages.

After successful target processing:

The Extraction Operator calls:
source.on_pipeline_success(context)



The base connector pushes the maximum timestamp:
context['ti'].xcom_push(    key=self.xcom_key,    value=str(self._current_max_value))



The next run uses this saved timestamp.
Example checkpoint cycle
Run	Checkpoint action
First run	Full load; maximum updatedDate = Nov 10, 14:30:00
Second run	Starts from Nov 10, 14:30:01; saves new maximum
Third run	Starts from the second run's saved maximum + 1 second


The success callback is important: the checkpoint is advanced only after the Extraction Operator completes its target processing successfully.

If the extraction or GCS writing fails, the success callback is not reached. This helps prevent the pipeline from moving past data that has not been successfully delivered to GCS.

There are two operational considerations:
- If the extraction returns no records, _current_max_value may remain None, and no new checkpoint is pushed.
- If an earlier batch was already written to GCS before a later batch failed, the earlier file may remain. A retry can produce duplicate files, so downstream loading and transformation need to tolerate reprocessing.
Also, the checkpoint + 1 second approach assumes the API timestamp precision and filter boundaries make this safe. It is worth validating against the API's actual behavior.

4. Stage 3 — Write the extracted records to GCS
Your Airflow DAG uses:
GCSTargetConnector(    conn_id='gcs-bucket-project',    table_name='employee',    gcs_path='raw/MDH')



The target connector:
1. Receives the extracted employee records.
2. Serializes them as JSON.
3. Adds pipeline metadata.
4. Uploads the file to the configured GCS bucket.
Your BaseConnector._serialize() adds these metadata fields:
{    'ipaas_updated_date': ...,    'src_system_operation': ...,    'airflow_dag_id': ...,    'airflow_run_id': ...,    'airflow_task_id': ...}



The ipaas_updated_date is an ingestion-side timestamp. It is not necessarily the same as the source system's updatedDate.

The Extraction Operator also sets the file prefix based on whether a checkpoint exists:
- No previous checkpoint: fullload
- Previous checkpoint exists: stream
This results in filenames resembling:
raw/MDH/employee/fullload_employee_<timestamp>_0.json

raw/MDH/employee/stream_employee_<timestamp>_0.json


These names are illustrative patterns based on the code you shared.

The distinction between these timestamps matters:
Timestamp	Meaning
Source updatedDate	When the source employee record was updated
ipaas_updated_date	When the integration pipeline assigned its ingestion timestamp
Filename timestamp	Timestamp embedded in the GCS file name
FILE_LAST_MODIFIED	GCS object's last-modified metadata
START_SCAN_TIME	Snowflake file scan start time

They serve different purposes in your pipeline.

5. Stage 4 — Terraform creates the GCS and Snowflake infrastructure
Your Terraform repository is responsible for provisioning the cloud and Snowflake objects used by ingestion.

This is infrastructure provisioning, not the daily employee extraction process.
GCP resources
Your main.tf creates:
- Pub/Sub topic.
- Pub/Sub subscription.
- GCS bucket notification configuration.
- IAM binding allowing the GCS service agent to publish to Pub/Sub.
It also creates the Snowflake storage integration and notification integration.
Snowflake resources
Your Terraform configuration creates:
Resource	Purpose
Storage integration	Secure access from Snowflake to GCS
External stage	Reference to the GCS location
File format	Defines JSON parsing
Landing table	Stores JSON records and file metadata
Notification integration	Connects Snowflake to GCP Pub/Sub
Snowpipe	Automatically loads new files

How the YAML drives Terraform
Your YAML configuration is:

pipes.yml
mdh:
  employee_pipe:
    stage: mdh_stg
    stage_subdir: employee
    file-format: json_ff
    table: employee


stages.yml
mdh:
  mdh_stg:
    url: /raw/MDH


file-formats.yml
mdh:
  json_ff:
    type: json


tables.yml
mdh:
  database: enterprise
  tables:
    - employee


The Terraform locals.tf reads these files using:
yamldecode(file("${var.config_dir}/pipes.yml"))


It builds resource specifications and uses for_each to create the corresponding Snowflake objects.

For the production environment, the expected landing relation is:
PROD_ENTERPRISE_LANDING.MDH.EMPLOYEE


The environment prefix is generated dynamically from var.environment.
Snowflake external stage
Your stages.tf creates a stage using:
storage_integration = snowflake_storage_integration.gcp_integration_edl_raw.id
url                 = each.value.url


The stage points to:
gcs://<bucket-name>/raw/MDH


The pipe reads from the employee subdirectory:
@<stage>/employee/


Snowpipe auto-ingestion
Your pipes.tf confirms:
auto_ingest = true


and:
integration =
  snowflake_notification_integration.gcp_notification_edl_raw.id


The GCS object notification is sent to Pub/Sub, and Snowflake receives it through the notification integration.

The pipe executes a COPY INTO operation using the configured JSON file format.

Its effective mapping is:
COPY INTO <landing_table>
FROM (
    SELECT
        $1,
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER,
        METADATA$FILE_LAST_MODIFIED,
        METADATA$START_SCAN_TIME
    FROM @<external_stage>/employee/
    (FILE_FORMAT => <json_file_format>)
)
ON_ERROR = CONTINUE;


This is the central role of Snowpipe: it automatically ingests newly arrived files without Airflow having to execute a Snowflake COPY INTO command.
What gets stored in Snowflake?
Your tables.tf creates:
DATA               VARIANT
FILENAME           VARCHAR
FILE_ROW_NUMBER    INT
FILE_LAST_MODIFIED TIMESTAMP_NTZ
START_SCAN_TIME    TIMESTAMP_LTZ


The DATA column contains the employee JSON record.

The remaining four columns contain file-level ingestion metadata.

This is a raw, semi-structured landing design. It allows the source JSON structure to be retained without requiring a separate Snowflake table column for every employee attribute.

6. Stage 5 — Snowflake cleanup Task maintains the full-load baseline
Your two dbt macros are responsible for deploying Snowflake Tasks:
- generate_fullload_delete_tasks_ddl.sql
- deploy_fullload_delete_tasks.sql
The generator scans dbt source definitions and checks:
with_fullload_delete: true


For an eligible source, it generates a Snowflake Task.

The task is scheduled with:
USING CRON 0/15 * * * * UTC


That means every 15 minutes in UTC.

The task identifies the latest full-load generation using the filename and deletes landing records belonging to older file generations.
Why is this needed?
Imagine the landing table contains:
fullload_employee_Nov01.json
stream_employee_Nov02.json
stream_employee_Nov03.json
fullload_employee_Nov10.json
stream_employee_Nov11.json


Once the November 10 full load has arrived, the earlier full-load generation and its intervening stream files are no longer needed as the baseline.

The cleanup task retains the latest full load and files from that generation onward.

This is a landing-table retention strategy. It does not create historical employee versions and it does not replace the dbt snapshot.

The two mechanisms have different purposes:
- Cleanup Task: controls old file generations in the landing table.
- dbt Snapshot: preserves historical employee attribute versions.
One important operational point: the task's SQL relies on the filename format and its extraction logic. Also, the exact cleanup behavior depends on the actual FILENAME values, not just the illustrative GCS paths.

7. Stage 6 — dbt Prepare layer: parse and deduplicate employee data
Now the data is in Snowflake.

Your first dbt transformation is:

dbt/models/prepare/enterprise/mdh/prep_employee.sql

This model reads from:
{{ source('landing_mdh', 'EMPLOYEE') }}


The source is declared in landing.yml:
sources:
  - name: landing_mdh
    database: ...
    schema: MDH
    tables:
      - name: EMPLOYEE


For production, this resolves to the production landing database and MDH schema.
What does Prepare do?
Your model has several responsibilities.

A. Extract employee attributes from JSON

For example:
DATA:recordId::VARCHAR AS RECORDID
DATA:createdDate::TIMESTAMP AS CREATED_DATE
DATA:updatedDate::TIMESTAMP AS UPDATED_DATE


It also extracts metadata such as:
- IPAAS_UPDATED_DATE
- SRC_SYSTEM_OPERATION
- AIRFLOW_DAG_ID
- AIRFLOW_TASK_ID
- AIRFLOW_RUN_ID
This converts semi-structured JSON into a structured relational dataset.

B. Identify the latest full-load generation

Your model compares the filename timestamps against the latest full-load filename timestamp.

This is important because the landing table can contain multiple full-load and incremental file generations.

The model uses the latest full load as its baseline and considers files from that generation onward.

C. Deduplicate employees

Your model uses:
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY UPPER(DATA:recordId)
    ORDER BY IPAAS_UPDATED_DATE DESC
) = 1


The partition is by employee recordId.

If multiple versions of an employee are present, it keeps the record with the latest IPAAS_UPDATED_DATE.

For example:
RECORDID	IPAAS_UPDATED_DATE	FIRST_NAME
EMP001	10:00	John
EMP001	11:00	Jonathan
EMP002	10:30	David

After deduplication:
RECORDID	IPAAS_UPDATED_DATE	FIRST_NAME
EMP001	11:00	Jonathan
EMP002	10:30	David

The key point: Prepare produces the latest available employee state per record ID, not a complete event-by-event history.

This is important when we get to the snapshot.
Prepare model materialization
Your model uses:
materialized = "dynamic_table"
target_lag = "downstream"


That means Snowflake manages the refresh of this model as a Dynamic Table, with its freshness target driven by downstream dependencies.

It is not simply a dbt table rebuilt only when an Airflow DAG runs.

8. Stage 7 — Normalize layer: standardize business attributes
Your next model is:

dbt/models/normalize/enterprise/mdh/norm_employee.sql

It reads from:
{{ ref('prep_employee') }}


It joins employee attributes to reference-data models, including the RDM reference data you shared.

The purpose is to standardize values such as:
- Financial company
- Contract type
- Country
- Cost center
- Line of business
- Personnel status
For example, different source representations of a country or contract type can be mapped to a common enterprise-standard value.

Your model uses logic such as:
IFNULL(reference_data_value, 'Not Found')


for unmapped values when the source value is non-null.

The result is a standardized employee dataset.
Difference between Prepare and Normalize
Prepare	Normalize
Reads raw JSON	Reads prepared employee records
Extracts attributes	Standardizes business values
Converts data types	Enriches with reference data
Deduplicates by record ID	Applies enterprise mappings
Establishes latest source state	Produces standardized employee state

Your Normalize model is also configured as a Snowflake Dynamic Table with target_lag='downstream'.

It is still a current-state transformation, not an SCD Type 2 model.

9. Stage 8 — Schematize layer: create DIM_EMPLOYEE
Your model is:

dbt/models/schematize/enterprise/dimensions/sche_dim_Employee.sql

This model reads from:
{{ ref('norm_employee') }}


It creates the employee dimension with a consistent enterprise schema.
Surrogate key generation
Your code uses:
dbt_utils.generate_surrogate_key(
    ['employee.RECORDID']
) AS EMPLOYEE_SK


This generates a deterministic surrogate key from the employee's business identifier.

For example:
RECORDID	EMPLOYEE_SK
EMP001	Generated hash A
EMP002	Generated hash B
EMP003	Generated hash C

The same RECORDID produces the same key, assuming the input value and key-generation logic remain unchanged.
Manager information
Your model also joins the employee dataset to itself to retrieve manager details.

Conceptually:
FROM norm_employee employee

LEFT JOIN norm_employee manager
    ON employee.manager_record_id = manager.record_id


This enriches the employee dimension with manager-related attributes.
Security
Your model uses post-hooks for:
- Masking policies on sensitive columns.
- Row access policy.
These are applied to the dimension after dbt builds it.
Important: DIM_EMPLOYEE itself is not SCD Type 2
Your Schematize model is a Dynamic Table representing the latest employee state.

It generates the employee surrogate key and applies the business model.

It does not itself maintain the old and new employee versions.

That responsibility belongs to the dbt snapshot.

This distinction is central to understanding your design:
DIM_EMPLOYEE

Current employee state

One current row per employee business key, subject to the model's source uniqueness


DIM_EMPLOYEE_SNAPSHOT

Historical employee state

Multiple versions of an employee when tracked attributes change



10. Stage 9 — dbt Snapshot: where SCD Type 2 is implemented
This is the most important part of your question.

Your snapshot configuration is:

Airflow/DAGS/dbt/snapshots/schematize/enterprise/dimensions/dim_employee_snapshot.yml

You shared:
snapshots:
  - name: dim_employee_snapshot
    relation: source('source_schematize', 'DIM_EMPLOYEE')
    config:
      schema: DIMENSIONS
      unique_key: [EMPLOYEE_SK]
      strategy: check
      check_cols: all
      dbt_valid_to_current: "to_date('9999-12-31')"
      hard_deletes: invalidate
      post_hook:
        - masking policies on sensitive columns
        - row access policy on EMPLOYEE_STATUS_CODE


Let's understand every setting.
A. relation
relation: source('source_schematize', 'DIM_EMPLOYEE')


The snapshot reads the DIM_EMPLOYEE relation declared as a dbt source.

This is worth distinguishing from ref():
- ref() references a dbt-managed model.
- source() references a declared dbt source relation.
Therefore, the snapshot's input is the Schematize DIM_EMPLOYEE source, not directly NORM_EMPLOYEE.
B. unique_key
unique_key: [EMPLOYEE_SK]


This identifies the employee whose versions dbt should track.

For example:
EMP001 → EMPLOYEE_SK = abc123


When the tracked attributes change, dbt recognizes that it is the same logical employee because the unique key remains the same.

This assumes that EMPLOYEE_SK remains stable for the employee.

If the key changes because the underlying RECORDID changes, dbt may interpret that as a different entity.
C. strategy: check
strategy: check


This tells dbt to detect changes by comparing tracked column values between snapshot executions.

This is different from the timestamp strategy.
Strategy	Change detection
check	Compares configured columns
timestamp	Uses a designated updated-at timestamp

Your implementation uses check.
D. check_cols: all
check_cols: all


This means dbt checks all eligible columns in the source relation for changes.

For example, consider the following source record:

Snapshot execution 1
EMPLOYEE_SK	EMPLOYEE_NAME	DEPARTMENT	CITY
abc123	John	Finance	Kolkata

Snapshot execution 2
EMPLOYEE_SK	EMPLOYEE_NAME	DEPARTMENT	CITY
abc123	John	HR	Kolkata

The department has changed from Finance to HR.

Because check_cols: all detects a difference, dbt:
1. Closes the old snapshot version.
2. Inserts a new snapshot version.
3. Sets the new version's validity start timestamp.
The result is:
EMPLOYEE_SK	EMPLOYEE_NAME	DEPARTMENT	DBT_VALID_FROM	DBT_VALID_TO
abc123	John	Finance	Nov 1	Nov 10
abc123	John	HR	Nov 10	9999-12-31

SCD Type 2
 The old value is preserved rather than overwritten.
E. dbt_valid_to_current
dbt_valid_to_current: "to_date('9999-12-31')"


This sets the validity end date of the current snapshot record to December 31, 9999.

Instead of using NULL to identify the current version, your implementation uses a far-future date.

For example:
Employee	Department	DBT_VALID_FROM	DBT_VALID_TO	Meaning
abc123	Finance	Nov 1	Nov 10	Historical
abc123	HR	Nov 10	9999-12-31	Current

To retrieve the current record:
SELECT *
FROM DIM_EMPLOYEE_SNAPSHOT
WHERE DBT_VALID_TO = TO_DATE('9999-12-31');


F. hard_deletes: invalidate
hard_deletes: invalidate


This controls what happens when an employee record disappears from the snapshot's input relation.

If an employee is no longer present in the source relation when the snapshot runs, dbt invalidates the current snapshot version by closing its validity period.

It does not create a new tombstone row with a separate delete event.

This is different from a soft delete, where the employee remains in the source with a status such as INACTIVE.
G. Post-hooks
Your snapshot also applies masking policies and row access policies.

This is consistent with the security controls used in the Schematize dimension.

11. How is the snapshot executed?
You shared the Airflow DAG:

Airflow/DAGS/Systems/dbt/schematize/enterprise/dbt.snapshot.dim_employee.py

It uses the Cosmos integration to execute the dbt snapshot.

The DAG selects the snapshot project path and schedules execution daily at 11:00.

The important distinction is:
Process	Execution mechanism
API extraction	Airflow DAG boomi.stream.mdh
GCS-to-Snowflake ingestion	Snowpipe auto-ingest
Prepare / Normalize / Schematize	Snowflake Dynamic Tables
Employee SCD Type 2 snapshot	Separate Airflow Cosmos/dbt snapshot DAG
Landing full-load cleanup	Snowflake Task every 15 minutes

Your snapshot DAG is therefore a separate process from the source extraction DAG.

It does not have to execute the entire employee ingestion pipeline itself.

The source-to-Snowflake data must already be available for the snapshot to observe it.

Schedule caveat: Your snapshot DAG declares local_tz, but the code you shared does not apply it to the DAG schedule. Therefore, the effective timezone needs to be confirmed from the Airflow configuration. Do not automatically describe 11:00 as Kolkata or Toronto time.
12. How the Dynamic Tables and snapshot work together
This is another important architectural distinction.

Your Prepare, Normalize and Schematize models are configured as Snowflake Dynamic Tables.

For example:
{{ config(
    materialized="dynamic_table",
    target_lag="downstream"
) }}


A Dynamic Table is maintained by Snowflake based on its query and upstream dependencies.

target_lag='downstream' means Snowflake can use downstream freshness requirements when determining when to refresh the Dynamic Table.

It does not mean:
- Refresh every time Airflow runs.
- Refresh at exactly the same time as the source extraction.
- Refresh only once per day.
Your daily snapshot DAG separately reads the current DIM_EMPLOYEE relation.

The conceptual sequence is:
1. Snowpipe loads new employee files.
2. The Prepare Dynamic Table processes available landing data.
3. The Normalize Dynamic Table applies reference-data mappings.
4. The Schematize Dynamic Table updates DIM_EMPLOYEE.
5. The snapshot DAG executes dbt snapshot logic.
6. dbt compares the current dimension against the previously captured snapshot version.
7. Changed records generate new historical versions.
The snapshot should observe a sufficiently refreshed DIM_EMPLOYEE relation before it runs. Your supplied code does not establish an explicit Airflow dependency or readiness check between the Dynamic Tables and the snapshot DAG.

That is an important operational consideration.

13. Understand the complete SCD Type 2 lifecycle with an example
Let's say an employee named John joins the company in the Finance department.
Day 1 — Initial snapshot
The Schematize dimension contains:
EMPLOYEE_SK	EMPLOYEE_NAME	DEPARTMENT
abc123	John	Finance

The first snapshot execution inserts the initial version:
EMPLOYEE_SK	EMPLOYEE_NAME	DEPARTMENT	DBT_VALID_FROM	DBT_VALID_TO
abc123	John	Finance	Nov 1	9999-12-31

Day 5 — Employee changes department
The source employee record is updated in Boomi.

Airflow extracts the changed record through its timestamp-based incremental mechanism.

The record is written to GCS.

Snowpipe loads it into the landing table.

The Dynamic Tables process the latest employee state.

The Schematize dimension now contains:
EMPLOYEE_SK	EMPLOYEE_NAME	DEPARTMENT
abc123	John	HR

The next dbt snapshot execution detects that DEPARTMENT has changed.

It closes the Finance version and creates a new HR version.
EMPLOYEE_SK	EMPLOYEE_NAME	DEPARTMENT	DBT_VALID_FROM	DBT_VALID_TO
abc123	John	Finance	Nov 1	Nov 5
abc123	John	HR	Nov 5	9999-12-31

Day 10 — Employee changes location
The employee moves from Kolkata to Bangalore.

Another snapshot execution detects the change.
EMPLOYEE_SK	EMPLOYEE_NAME	DEPARTMENT	CITY	DBT_VALID_FROM	DBT_VALID_TO
abc123	John	Finance	Kolkata	Nov 1	Nov 5
abc123	John	HR	Kolkata	Nov 5	Nov 10
abc123	John	HR	Bangalore	Nov 10	9999-12-31

Historical versions retained
This is the fundamental behavior of SCD Type 2.
What happens if an employee is deleted?
With hard_deletes: invalidate, when the employee disappears from the snapshot input, dbt closes the current version.

The previous versions remain available for historical reporting.

However, because your snapshot runs daily, it captures the state visible at snapshot execution time. It is not a complete event history of every source-system change.

For example, if an employee changes from Finance to HR and then back to Finance between two snapshot runs, the snapshot might see only Finance and not capture the intermediate HR state.

That is an important difference between snapshot-based history and transaction-level CDC.

14. Final architecture: responsibilities of each technology
Here is the complete division of responsibility in your implementation.
Technology	What it does in this project
Boomi DataHub	Provides employee source data
Apigee	API access and authentication integration
Apache Airflow	Orchestrates employee extraction
Custom Python connectors	Handle API requests, pagination, transformation and GCS writing
Airflow XCom	Stores the last successful extraction checkpoint
GCS	Stores extracted JSON files
GCP Pub/Sub	Delivers GCS object notifications
Terraform	Provisions GCP and Snowflake infrastructure
Snowflake storage integration	Provides Snowflake access to GCS
Snowflake external stage	References GCS files
Snowflake file format	Defines JSON parsing
Snowpipe	Automatically loads new files
Snowflake landing table	Stores raw JSON and file metadata
Snowflake Task	Removes older full-load generations
dbt Prepare	Parses JSON, types attributes and deduplicates
dbt Normalize	Standardizes employee business attributes
dbt Schematize	Creates the current employee dimension
dbt Snapshot	Implements SCD Type 2 history
Snowflake Dynamic Tables	Maintain the transformation layers
Snowflake masking and row access policies	Protect sensitive employee data

15. Full architecture: which processes run when?
Execution timeline and orchestration
1. Daily — 05:00 configured Airflow timezone
   
   boomi.stream.mdh
   - Extract full load initially, then timestamp-based deltas.
   - Write JSON files to GCS.
   - Persist successful checkpoint in XCom.
2. On GCS object creation
   
   GCS notification → Pub/Sub → Snowflake notification integration.
   
   Snowpipe receives the notification and processes eligible files.
3. Snowflake-managed refresh
   
   Prepare → Normalize → Schematize Dynamic Tables.
   
   Snowflake manages refresh based on their target-lag settings and dependencies.
4. Every 15 minutes UTC
   
   Full-load cleanup Task.
   
   Removes older file generations from the landing table.
5. Daily — snapshot DAG schedule declared as 11:00
   
   Cosmos/dbt snapshot.
   
   Compares the current employee dimension with the previous snapshot state and records changes.

The exact order of the cleanup Task, Dynamic Table refreshes and snapshot execution is not guaranteed simply by these schedules. For production reliability, the snapshot should run only after the dimension has reached the desired freshness.

16. Important points you should understand before explaining this in an interview
These are the distinctions that demonstrate a proper understanding of the architecture.
Question	Correct explanation
Is Airflow implementing SCD Type 2?	No. It handles extraction and checkpointing. dbt snapshots implement SCD Type 2.
Is Snowpipe implementing CDC?	No. It automatically loads files from GCS into Snowflake.
Is updatedDate the SCD2 validity date?	Not in the snapshot configuration you shared. The snapshot uses the check strategy and dbt-managed validity timestamps.
Is DIM_EMPLOYEE historical?	The Schematize model represents current employee state. The snapshot preserves historical versions.
Why use VARIANT?	To store the raw JSON without defining every employee attribute as a landing column.
Why use Terraform?	To provision repeatable GCP and Snowflake infrastructure.
Why use Pub/Sub?	To notify Snowflake when new GCS objects arrive.
Why use XCom?	To retain the last successful incremental extraction high-water mark.
Why use check_cols: all?	To detect changes in any eligible source column included in the snapshot.
Why use hard_deletes: invalidate?	To close the current snapshot version when its source record disappears.
Why use Dynamic Tables?	To maintain the transformation relations automatically in Snowflake.

17. Five technical caveats worth knowing
These are not necessarily defects, but they are things you should understand rather than making assumptions.

1. Incremental extraction is timestamp-based. The code you shared does not prove transaction-level CDC or explicit handling of source DELETE events.

2. The snapshot is only as complete as its input and execution frequency. Daily snapshots can miss intermediate changes that occur and are reversed between runs.

3. EMPLOYEE_SK stability matters. Your snapshot tracks the employee using a hash generated from RECORDID. If the business identifier changes, the snapshot may treat it as a different employee.

4. Snapshot scheduling and Dynamic Table freshness must be coordinated. The DAG schedule alone does not guarantee that the input dimension has refreshed before the snapshot starts.

5. Snowpipe error handling requires monitoring. ON_ERROR = CONTINUE permits ingestion to continue despite errors. Load history and rejected records need to be monitored.

18. Interview-ready explanation — complete end-to-end answer
This is the explanation I would recommend learning. It is structured around the actual implementation you shared, rather than describing a generic data engineering architecture.
Explain the end-to-end employee data pipeline
We have implemented an end-to-end employee data pipeline using Apache Airflow, Google Cloud Storage, Terraform, Snowflake Snowpipe and dbt.
1. Source extraction using Airflow
The employee data originates from Boomi DataHub and is accessed through Apigee.

We have a custom Airflow DAG called boomi.stream.mdh, which executes daily. It uses a custom API source connector to send XML POST requests and retrieve employee records through pagination using an offset token.

The initial execution performs a full load. Subsequent executions use timestamp-based incremental extraction based on the source updatedDate.

We maintain the last successful high-water mark in Airflow XCom. During extraction, the connector tracks the maximum updatedDate. After successful target processing, the maximum timestamp is pushed to XCom and used as the starting checkpoint for the next execution.
2. Store extracted data in GCS
The extracted employee records are serialized as JSON and written to Google Cloud Storage.

We use different filename prefixes for full-load and incremental files, such as fullload and stream.

The ingestion framework also adds metadata such as the Airflow DAG ID, task ID, run ID and integration timestamp.
3. Automated ingestion using Snowpipe
We use Terraform to provision the GCP and Snowflake infrastructure.

On GCP, we configure a Pub/Sub topic, subscription and GCS object notifications.

On Snowflake, we create a storage integration, notification integration, external stage, JSON file format, landing table and Snowpipe.

Snowpipe is configured with AUTO_INGEST = TRUE. When a new file arrives in GCS, GCS publishes an event to Pub/Sub. Snowflake receives the notification and automatically executes its configured COPY INTO statement.

The employee JSON record is stored in a VARIANT column, together with metadata such as filename, file row number, file last-modified timestamp and scan time.
4. dbt transformation layers
We follow a layered architecture.

Prepare: We parse JSON attributes, convert them into appropriate data types, extract ingestion metadata, identify the latest full-load generation and deduplicate employees using their record ID and ingestion timestamp.

Normalize: We enrich employee data using enterprise reference data to standardize attributes such as financial company, contract type, country, cost center, line of business and personnel status.

Schematize: We create the DIM_EMPLOYEE dimension, generate a surrogate key using the employee record ID, enrich manager details and apply data security policies.

These transformation models are implemented as Snowflake Dynamic Tables.
5. Historical tracking using dbt snapshots
The Schematize DIM_EMPLOYEE represents the latest employee state. To maintain historical employee versions, we use a separate dbt snapshot called dim_employee_snapshot.

The snapshot uses the check strategy with check_cols: all and EMPLOYEE_SK as its unique key.

Whenever an employee attribute changes between snapshot executions, dbt closes the previous version and inserts a new version.

We use dbt_valid_to_current to represent the current record with a far-future date of 9999-12-31. We also configure hard_deletes: invalidate so that records disappearing from the snapshot source have their current validity period closed.

The snapshot is executed through a separate Airflow DAG using the Cosmos integration.
6. Landing-table cleanup
We also have a metadata-driven dbt macro that generates Snowflake Tasks for eligible landing sources. These tasks periodically remove older full-load generations based on the filename timestamp, keeping the latest full-load baseline and subsequent incremental files.

Overall, Airflow handles extraction and orchestration, GCS provides raw file storage, Pub/Sub enables event notifications, Snowpipe handles automated ingestion, Terraform provisions infrastructure, dbt performs transformations and historical tracking, and Snowflake provides the storage and processing platform.





19. My recommendation: understand these three layers especially well
For a Data Engineer / Senior Data Engineer interview, the most important thing is to explain the difference between these three employee representations:
Layer 1
Landing — Raw employee records

JSON records and file metadata. Multiple records and file generations can exist.


Layer 2
DIM_EMPLOYEE — Current employee state

Prepared, standardized and enriched employee attributes. The current-state dimension.


Layer 3
DIM_EMPLOYEE_SNAPSHOT — Historical employee state

Previous and current versions, with validity dates maintained by dbt.


If you can clearly explain how one employee record moves through these three layers, why XCom is used, how Snowpipe gets triggered, and why the snapshot is separate from the Dynamic Table, you understand the core of this architecture.

One final distinction to remember: your company has implemented timestamp-based incremental ingestion followed by snapshot-based SCD Type 2 history. Do not describe the entire pipeline as transaction-level CDC. That would overstate what the code you shared actually implements.