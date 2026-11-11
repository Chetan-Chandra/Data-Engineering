A. Data Modelling and Data Warehousing
Highest priority
You already have:
- [x] Dimensions and fact tables
- [x] Star Schema and Snowflake Schema
- [x] SCD Type 1 and Type 2
- [x] Surrogate keys
- [x] Grain of fact tables
- [x] Data products
Additional topics to prepare:
- [ ] Conceptual, Logical and Physical Data Modelling
- [ ] Normalization: 1NF, 2NF, 3NF and BCNF
- [ ] Denormalization
- [ ] Transactional vs Analytical data models (OLTP vs OLAP)
- [ ] Types of Fact Tables:
  - Transaction Fact
  - Periodic Snapshot Fact
  - Accumulating Snapshot Fact
- [ ] Additive, Semi-additive and Non-additive measures
- [ ] Degenerate Dimensions
- [ ] Junk Dimensions
- [ ] Role-playing Dimensions
- [ ] Conformed Dimensions
- [ ] Outrigger Dimensions
- [ ] Bridge Tables
- [ ] Many-to-Many relationships
- [ ] Hierarchies (Product, Geography, Organization)
- [ ] Late-arriving Dimensions
- [ ] Early-arriving Facts
- [ ] Inferred Dimensions
- [ ] SCD Type 0, 3, 4 and 6 (at least understand)
- [ ] Factless Fact Tables
- [ ] Data Vault Modelling: Hubs, Links and Satellites
- [ ] Enterprise Data Warehouse vs Data Mart
- [ ] One Big Table (OBT) vs Star Schema
Particularly important: Be able to design a complete dimensional model from a business requirement, explain its grain, and justify every relationship.
B. dbt – Data Transformation and Development
Highest priority
You already have:
- [x] dbt Models
- [x] Views
- [x] Tables
- [x] Incremental Models
- [x] Ephemeral Models
- [x] Dynamic Tables
- [x] Snapshots
- [x] Generic Tests
- [x] Singular Tests
- [x] Macros
Additional topics:
1. dbt project fundamentals
- [ ] dbt architecture and execution workflow
- [ ] dbt_project.yml
- [ ] profiles.yml
- [ ] Model directory structure
- [ ] source() vs ref()
- [ ] Dependency DAG
- [ ] Model selection and execution order
- [ ] dbt run vs dbt build vs dbt test
- [ ] dbt compile vs dbt run
2. Advanced dbt features
- [ ] Seeds
- [ ] Sources
- [ ] Source freshness
- [ ] Documentation (schema.yml)
- [ ] Model and column descriptions
- [ ] Exposures
- [ ] Packages
- [ ] Hooks: pre-hook and post-hook
- [ ] Variables (var())
- [ ] Environment variables (env_var())
- [ ] Jinja: loops, conditions, expressions
- [ ] Reusable macros
- [ ] Custom generic tests
- [ ] Unit tests
- [ ] Model contracts
- [ ] Model versions
- [ ] Tags and selectors
- [ ] Grants and access configurations
- [ ] Persist docs
- [ ] dbt artifacts: manifest.json, run_results.json, catalog.json
3. Advanced incremental processing
- [ ] Incremental strategies:
  - Append
  - Merge
  - Delete+Insert
  - Insert Overwrite (adapter-dependent)
- [ ] unique_key
- [ ] is_incremental()
- [ ] {{ this }}
- [ ] incremental_predicates
- [ ] Handling duplicate source records
- [ ] Late-arriving records
- [ ] Lookback windows
- [ ] Full refresh
- [ ] Backfilling historical data
- [ ] Idempotency
- [ ] Handling deleted source records
4. dbt production and CI/CD
- [ ] Git branching strategy
- [ ] Pull requests and code reviews
- [ ] Development vs QA vs Production environments
- [ ] CI/CD integration
- [ ] Slim CI
- [ ] State-based selection
- [ ] state:modified
- [ ] defer
- [ ] dbt deployment and scheduling
- [ ] Failure handling and retries
One important clarification: Macros are reusable Jinja functions. Generic tests are implemented using macros. Singular tests are standalone SQL queries. Unit tests are another separate dbt testing capability.
C. Snowflake – Advanced Database Concepts
Highest priority
You already have:
- [x] Snowflake architecture
- [x] Virtual warehouses
- [x] Micro-partitions
- [x] Clustering
- [x] Streams
- [x] Tasks
- [x] Time Travel
- [x] Zero-copy cloning
- [x] Dynamic tables
Additional topics:
1. Snowflake ingestion
- [ ] Internal vs External Stages
- [ ] Named vs User vs Table Stages
- [ ] File Formats
- [ ] COPY INTO
- [ ] Snowpipe
- [ ] Snowpipe Streaming
- [ ] Auto-ingest using cloud notifications
- [ ] Storage integrations
- [ ] External tables
- [ ] File ingestion metadata
- [ ] Handling duplicate files
- [ ] Error handling during ingestion
2. Streams and Tasks
- [ ] Standard Streams
- [ ] Append-only Streams
- [ ] Insert, Update and Delete CDC records
- [ ] Stream offsets
- [ ] Stream staleness
- [ ] Stream consumption
- [ ] Task scheduling
- [ ] Task graphs
- [ ] Root and child tasks
- [ ] Serverless vs user-managed warehouse tasks
- [ ] Conditional task execution
- [ ] Stream + Task + MERGE architecture
- [ ] Handling task failures
3. Snowflake SQL and database objects
- [ ] Temporary vs Transient vs Permanent Tables
- [ ] Views vs Materialized Views
- [ ] Dynamic Tables vs Materialized Views
- [ ] Stored Procedures
- [ ] User-defined Functions (UDFs)
- [ ] SQL scripting
- [ ] MERGE
- [ ] QUALIFY
- [ ] FLATTEN
- [ ] VARIANT, OBJECT and ARRAY
- [ ] Semi-structured JSON processing
- [ ] Transactions and ACID properties
- [ ] Constraints in Snowflake
4. Snowflake performance and cost
- [ ] Query Profile
- [ ] Query History
- [ ] Micro-partition pruning
- [ ] Partition scanning
- [ ] Warehouse resizing
- [ ] Multi-cluster warehouses
- [ ] Auto-suspend and auto-resume
- [ ] Query spilling
- [ ] Result cache
- [ ] Warehouse cache
- [ ] Materialized views
- [ ] Search Optimization Service
- [ ] Clustering depth
- [ ] Resource monitors
- [ ] Cost optimization
5. Security and governance
- [ ] Role-Based Access Control (RBAC)
- [ ] System-defined vs custom roles
- [ ] Role hierarchy
- [ ] Database/schema/table privileges
- [ ] Masking policies
- [ ] Row access policies
- [ ] Secure views
- [ ] Object tagging
- [ ] Data classification
- [ ] Data lineage
- [ ] Data governance

D. CDC and Data Integration
High priority
You already have CDC, but expand it beyond the definition.
- [ ] What is CDC?
- [ ] Full load vs Incremental load
- [ ] Log-based CDC
- [ ] Timestamp-based CDC
- [ ] Trigger-based CDC
- [ ] CDC using SQL Server
- [ ] CDC using Snowflake Streams
- [ ] CDC using Kafka/Debezium (conceptual understanding)
- [ ] INSERT, UPDATE and DELETE handling
- [ ] Before and after images
- [ ] Event ordering
- [ ] Duplicate events
- [ ] Out-of-order events
- [ ] Idempotent processing
- [ ] Watermark/checkpoint management
- [ ] CDC to SCD Type 2 implementation
- [ ] Source CDC vs warehouse CDC
A particularly important interview question:

How would you implement an end-to-end CDC pipeline using SQL Server CDC, Airflow, Snowflake Streams, Tasks and dbt?

You should be able to draw and explain the architecture.
E. SQL – Advanced Snowflake SQL
Highest priority
You have already practised running totals and window functions. Add these:
- [ ] Complex joins
- [ ] Self joins
- [ ] Anti joins
- [ ] Semi joins
- [ ] Multiple CTEs
- [ ] Recursive CTEs
- [ ] ROW_NUMBER, RANK, DENSE_RANK
- [ ] LAG and LEAD
- [ ] FIRST_VALUE and LAST_VALUE
- [ ] SUM, AVG and COUNT window functions
- [ ] Running totals
- [ ] Rolling averages
- [ ] MTD, QTD and YTD
- [ ] Year-over-year growth
- [ ] Month-over-month growth
- [ ] Deduplication using QUALIFY
- [ ] Conditional aggregation
- [ ] PIVOT and UNPIVOT
- [ ] MERGE statements
- [ ] NULL handling
- [ ] Date functions
- [ ] Query optimization
- [ ] Gaps and islands problems
- [ ] Consecutive-day problems
- [ ] Top-N per group
F. Data Quality, Metadata and Governance
You already have generic and singular tests. Add:
- [ ] dbt unit tests
- [ ] Referential integrity
- [ ] Uniqueness and null validation
- [ ] Accepted values
- [ ] Source freshness
- [ ] Data reconciliation
- [ ] Source-to-target validation
- [ ] Record count validation
- [ ] Duplicate detection
- [ ] Schema drift
- [ ] Data contracts
- [ ] Metadata management
- [ ] Technical vs business metadata
- [ ] Data lineage
- [ ] Data catalog
- [ ] Data observability
- [ ] SLA and SLO
- [ ] Data quality incident management


2. Important comparisons you must be able to explain
These are particularly valuable in technical interviews because interviewers often ask you to distinguish similar technologies.
Comparison	Priority
Star Schema vs Snowflake Schema	Critical
Fact vs Dimension	Critical
SCD Type 1 vs Type 2	Critical
Natural Key vs Surrogate Key	Critical
Transaction vs Snapshot Fact	High
dbt Incremental vs Snapshot	Critical
dbt Incremental vs Dynamic Table	Critical
dbt View vs Table	High
dbt Ephemeral vs View	High
dbt Generic vs Singular vs Unit Tests	High
ref() vs source()	Critical
dbt run vs dbt build	High
Snowflake Streams vs Tasks	Critical
Streams vs Snowpipe	Critical
Snowpipe vs Snowpipe Streaming	High
Dynamic Table vs Materialized View	High
Temporary vs Transient vs Permanent	High
Warehouse Scaling Up vs Scaling Out	High
Micro-partitioning vs Clustering	Critical
Full Load vs Incremental Load	Critical
CDC vs SCD	Critical
ETL vs ELT	Critical
CTE vs Temporary Table	High
View vs Materialized View	High

3. Most important: Understand how these technologies work together
Knowing each feature individually is not enough for a senior data modeller role.

You should understand the following integrated architecture:
Enterprise Snowflake + dbt architecture:
___________________________________________________________________________________
|                        Source Systems
|                Oracle / SQL Server / APIs / Files
|                                ↓
|                        CDC / Full Load
|                                ↓
|                            Ingestion
|                Airflow / Python / Snowpipe / COPY INTO
|                                ↓
|                         Snowflake RAW
|                Source-aligned tables / CDC records
|                                ↓
|                Streams + Tasks, where applicable
|                                ↓
|                            dbt STAGING
|                Cleaning / Deduplication / Standardization
|                                ↓
|                            dbt INTERMEDIATE
|                Business rules / Joins / Transformations / SCD
|                                ↓
|                           dbt MARTS
|                FACT_SALES / DIM_CUSTOMER / DIM_PRODUCT / DIM_DATE
|                                ↓
|                            Data Products
|                Business-ready datasets / Aggregations / KPIs
|                                ↓
|                            BI / Analytics
|                Tableau / Power BI / Reporting
|___________________________________________________________________________________

Cross-cutting capabilities: Data quality, lineage, documentation, security, monitoring, CI/CD and governance.

One important architectural distinction:
- Snowflake Streams and Tasks can implement Snowflake-native CDC processing.
- dbt handles warehouse transformations and model dependencies.
- Airflow can orchestrate ingestion and dbt execution.
- Dynamic Tables are Snowflake-managed declarative transformations, whereas dbt incremental models are explicitly defined transformation logic.
These are complementary capabilities, not interchangeable features.