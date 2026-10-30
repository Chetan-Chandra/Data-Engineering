Agreed. We should pause the SQL module and focus on your weaker areas: Azure, GCP, and how their services are used to build real-world data engineering platforms.

Rather than learning Azure and GCP separately, we'll study them side by side by functionality. This is a better approach because the underlying data engineering concepts are largely transferable, even when the services and implementation details differ.

We'll use your actual Canam experience as the GCP/Snowflake reference point and your Formula 1 project as the Azure/Databricks reference point. We'll distinguish professional experience from personal-project experience when preparing interview answers.

# Azure + GCP Data Engineering — Learning Roadmap

## 1. Cloud services: understand the equivalent services

We'll start with the purpose of each service, why it exists, how it works, and when to use it.

| Functionality               | Microsoft Azure                           | Google Cloud (GCP)             |
| --------------------------- | ----------------------------------------- | ------------------------------ |
| Object storage / Data lake  | Azure Data Lake Storage Gen2 (ADLS)       | Google Cloud Storage (GCS)     |
| Data warehouse              | Azure Synapse Analytics                   | BigQuery                       |
| Data processing             | Azure Databricks / Azure Synapse Spark    | Dataproc / Dataflow / BigQuery |
| Managed Spark               | Azure Databricks                          | Dataproc                       |
| Stream processing           | Azure Stream Analytics / Azure Databricks | Dataflow                       |
| Messaging / Event streaming | Azure Event Hubs / Service Bus            | Pub/Sub                        |
| Orchestration               | Azure Data Factory                        | Cloud Composer (Airflow)       |
| Serverless compute          | Azure Functions                           | Cloud Functions / Cloud Run    |
| Relational database         | Azure SQL Database                        | Cloud SQL                      |
| NoSQL database              | Cosmos DB                                 | Firestore / Bigtable           |
| Secrets management          | Azure Key Vault                           | Secret Manager                 |
| Identity and access         | Microsoft Entra ID / Azure RBAC           | Cloud IAM                      |
| Infrastructure as Code      | Terraform / ARM / Bicep                   | Terraform                      |
| CI/CD                       | Azure DevOps                              | Cloud Build / Cloud Deploy     |
| Monitoring                  | Azure Monitor                             | Cloud Monitoring / Logging     |

Important: These are functional comparisons, not exact one-to-one replacements. For example, BigQuery is a serverless analytical warehouse, whereas Dataproc provides managed Spark clusters. They solve different processing needs.

## 2. Core cloud data engineering concepts

We'll learn each concept once, then implement and compare it in Azure and GCP.

| Topic                         | What you need to understand                                          |
| ----------------------------- | -------------------------------------------------------------------- |
| Data lakes                    | Why companies store raw data in cloud object storage                 |
| Data warehouses               | Analytical storage, querying and reporting                           |
| Lakehouse architecture        | Combining data lake flexibility with warehouse capabilities          |
| Batch processing              | Processing data at scheduled intervals                               |
| Stream processing             | Processing events continuously                                       |
| ETL vs ELT                    | Where transformations happen and why                                 |
| Full load vs incremental load | Loading everything versus only changed data                          |
| CDC                           | Capturing inserts, updates and deletes from source systems           |
| SCD Type 1                    | Overwriting historical values                                        |
| SCD Type 2                    | Maintaining historical versions of records                           |
| Partitioning                  | Dividing data to improve query and processing efficiency             |
| Clustering                    | Organizing data to improve data access                               |
| File formats                  | CSV, JSON, Parquet, Avro, ORC                                        |
| Compression                   | Snappy, Gzip and their trade-offs                                    |
| Schema evolution              | Handling changes in source data structures                           |
| Data modelling                | Facts, dimensions, star and snowflake schemas                        |
| Medallion architecture        | Bronze, Silver and Gold layers                                       |
| Data quality                  | Completeness, uniqueness, validity and consistency                   |
| Idempotency                   | Safely rerunning pipelines without duplicate or inconsistent results |
| Slowly changing pipelines     | Handling late-arriving and out-of-order data                         |
| Security and governance       | IAM, encryption, masking, lineage and access control                 |

## 3. How we'll connect the concepts to real projects

We'll build two comparable end-to-end architectures.

Architecture A — Azure Data Engineering

Your Formula 1 project reference

![Azure_Data_Engineering](Images/Azure_Data_Engineering.png)

Source data

CSV / JSON / APIs

ADLS Gen2

Raw data storage — Bronze

Azure Databricks + PySpark

Cleansing, deduplication, CDC handling — Silver

Delta Lake

Business transformations and aggregations — Gold

Synapse / Power BI

Analytics and reporting

Architecture B — GCP Data Engineering

Your Canam project reference

![GCP_Data_Engineering](Images/GCP_Data_Engineering.png)

Source systems

SQL Server / Oracle / Progress

Cloud Composer / Airflow + Python

Extraction and orchestration

Google Cloud Storage

Landing and raw files

Pub/Sub + Snowpipe

Event notification and automated ingestion into Snowflake

Snowflake + dbt

Transformations and curated data layers

Tableau

Analytics and reporting

These are learning architectures, not claims that every component was deployed in both of your projects. We'll also study the alternative GCP-native architecture using BigQuery.

# 4. How each topic will be taught

For every topic, we'll follow the same sequence:

1. Fundamentals: What it is, why it exists, and the problem it solves.
2. Azure implementation: Services, configuration, architecture and practical example.
3. GCP implementation: Equivalent services and implementation.
4. Comparison: Similarities, differences, limitations and trade-offs.
5. Hands-on exercise: SQL, Python, PySpark, configuration or architecture design.
6. Real-world scenario: How it works in an enterprise data pipeline.
7. Interview questions: Fundamentals, follow-ups, troubleshooting and senior-level design questions.

For example, when we reach CDC, you won't just memorize its definition. You'll learn how to capture changes from SQL Server or Oracle, move them into cloud storage, process inserts/updates/deletes, maintain the latest state, and preserve historical records using SCD Type 2.

# 5. Our starting point: cloud foundations

Before jumping into partitioning or CDC, we need to understand the storage and compute foundations. Otherwise, advanced concepts become disconnected memorization.

We'll start with:

Module 1 — Cloud data engineering foundations

- What is cloud computing?
- What are regions and availability zones?
- What are storage, compute and networking?
- What is object storage?
- What is a data lake versus a data warehouse?
- ADLS Gen2 vs GCS
- Containers, buckets, directories, blobs and objects
- Storage tiers and lifecycle policies
- IAM, service accounts, managed identities and permissions
- How a data engineer connects to cloud storage programmatically

Then we'll move to data processing, orchestration, ingestion, modelling and architecture.

## Let's begin — Module 1, Lesson 1

What is the difference between storage and compute in cloud data engineering?

Imagine your company receives 500 GB of customer transaction files every day.

- Storage is where those files are kept. Examples: ADLS Gen2 and GCS.
- Compute is the processing power used to read, clean, transform and aggregate those files. Examples: Databricks clusters, Spark, Dataflow.
- Orchestration coordinates when and in what order processing happens. Examples: Azure Data Factory and Airflow.

A simple analogy:

- Storage = warehouse where goods are kept.
- Compute = workers and machines that process the goods.
- Orchestration = manager who schedules and coordinates the work.

![analogy](Images/analogy.png)

Why does this distinction matter?

Suppose a Databricks job processes 500 GB of data and finishes. You can terminate the compute cluster while keeping the data in ADLS Gen2. The data remains stored; you stop paying for the terminated compute resources, although storage charges continue.

This separation is one of the fundamental advantages of cloud data platforms.

## Your first knowledge check

1\. You upload 100 GB of CSV files to ADLS Gen2. Which cloud capability are you using?

Compute

Storage

Orchestration

CorrectADLS Gen2 stores the files. It does not automatically transform them.

2\. You use PySpark to read those files, remove duplicates and calculate monthly revenue. What is primarily doing the work?

Storage

Compute

IAM

CorrectSpark uses compute resources to read and transform the data.

3\. You want a job to run every day at 2 AM, and then trigger another job after it succeeds. What do you need?

Object storage

Orchestration

Data partitioning

CorrectAn orchestrator such as Airflow or Azure Data Factory schedules and coordinates tasks.

4\. After a Databricks job completes, you terminate its cluster. What happens to files stored in ADLS Gen2?

They are automatically deleted

They remain stored

They move into Databricks

CorrectStorage and compute are separate. Terminating compute does not delete the stored files.

Score: 4/4

Retry questions

We'll move forward only after you've understood the foundations. I want you to be able to explain not just what ADLS, GCS, Databricks, BigQuery and Airflow are, but also why an architect chooses one service over another and what happens behind the scenes.