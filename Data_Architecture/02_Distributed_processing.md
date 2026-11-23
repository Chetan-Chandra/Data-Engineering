# **Module 5 — Distributed Data Processing**

**Apache Spark | PySpark | Distributed Computing | Batch Processing | Stream Processing**

This will be a comprehensive learning programme, progressing from beginner to advanced, expert and specialist level.

The objective is not simply to learn PySpark syntax. You should understand how distributed processing engines work internally, how to design large-scale processing architectures, how to troubleshoot production failures, and how to make performance and architectural decisions.

We'll include coding, practical implementation, real-world engineering scenarios and interview preparation.

We'll use your existing AdventureWorks and data pipeline experience as a practical foundation, adding Spark-specific projects without unnecessarily rebuilding your existing pipeline.

# **Part 1 — Complete learning syllabus**

## **Level 1: Beginner — Distributed computing foundations**

**Goal:** Understand why distributed processing exists and how Spark solves large-scale data processing problems.

### **Step 1. Introduction to distributed computing**

- What is distributed computing?
- What is parallel computing?
- Vertical scaling vs horizontal scaling.
- Single-machine processing vs distributed processing.
- Why traditional Python and SQL processing become bottlenecks.
- CPU, memory, disk and network limitations.
- Distributed storage vs distributed processing.
- Data-intensive vs compute-intensive workloads.
- Batch processing vs stream processing.
- Real-world applications of distributed computing.

### **Step 2. Apache Spark fundamentals**

- What is Apache Spark?
- History and evolution of Spark.
- Apache Spark architecture.
- Spark ecosystem and components.
- Spark Core.
- Spark SQL.
- Spark DataFrame API.
- Spark Structured Streaming.
- MLlib.
- GraphX.
- Spark vs Hadoop MapReduce.
- Spark vs Pandas.
- Spark vs Dask.
- Spark vs Flink.
- When Spark is appropriate and when it isn't.

### **Step 3. PySpark fundamentals**

- What is PySpark?
- Python and JVM interaction.
- PySpark architecture.
- SparkSession.
- SparkContext.
- DataFrame and Dataset concepts.
- RDD fundamentals.
- Creating DataFrames.
- Reading CSV, JSON, Parquet and ORC.
- Writing DataFrames.
- Schema inference vs explicit schemas.
- Data types.
- Column expressions.
- Transformations and actions.
- Lazy evaluation.
- Basic DataFrame operations.
- Spark SQL.

**Practical implementation:** Build a local Spark environment and process AdventureWorks customer and sales data.

## **Level 2: Intermediate — Spark architecture and execution**

**Goal:** Understand what Spark actually does internally when you execute PySpark code.

### **Step 4. Internal Spark architecture**

- Driver program.
- Executors.
- Cluster manager.
- Worker nodes.
- Spark applications.
- Jobs.
- Stages.
- Tasks.
- Slots and task concurrency.
- SparkSession lifecycle.
- SparkContext lifecycle.
- Application execution flow.
- Cluster deployment modes.
- Client mode vs cluster mode.

### **Step 5. Spark execution model**

- Transformations vs actions.
- Narrow vs wide transformations.
- Lazy evaluation.
- DAG (Directed Acyclic Graph).
- DAG Scheduler.
- Task Scheduler.
- Jobs and stages.
- Stage boundaries.
- Pipeline execution.
- Physical execution plans.
- Logical plans.
- Catalyst Optimizer.
- Tungsten execution engine.
- Whole-stage code generation.

### **Step 6. RDD, DataFrame and Dataset**

- Resilient Distributed Datasets.
- RDD creation.
- RDD transformations and actions.
- RDD lineage.
- RDD partitioning.
- DataFrame architecture.
- Dataset concept and typed APIs.
- DataFrame vs RDD.
- DataFrame vs Dataset.
- When to use each.
- Why DataFrames are generally preferred for structured ETL.
- Python UDFs vs built-in Spark functions.

### **Step 7. Partitioning and parallelism**

- What is a partition?
- How data is divided across partitions.
- Input partitions.
- Execution partitions.
- Partition count.
- Partition sizing.
- `repartition()`.
- `coalesce()`.
- Hash partitioning.
- Range partitioning.
- Round-robin partitioning.
- Partition pruning.
- Parallelism and task execution.
- Data locality.
- Uneven partition sizes.
- Small partitions vs large partitions.
- Partition management strategies.

**Practical implementation:** Examine Spark execution plans and observe how partition counts change during operations.

## **Level 3: Advanced — Shuffling, joins and performance engineering**

**Goal:** Understand Spark performance deeply and solve problems encountered in large production pipelines.

### **Step 8. Shuffle architecture**

- What is a shuffle?
- Why Spark performs shuffles.
- Narrow vs wide transformations.
- Shuffle write and shuffle read.
- Shuffle files.
- Network transfer.
- Disk spilling.
- Shuffle partitions.
- Shuffle service.
- Shuffle compression.
- Shuffle optimization.
- Shuffle-related failures.
- How to identify expensive shuffles.

Operations that commonly trigger shuffles:

- `groupBy()`
- `distinct()`
- `repartition()`
- `orderBy()`
- `join()`
- `reduceByKey()`

### **Step 9. Joins in Spark**

- Inner join.
- Left join.
- Right join.
- Full outer join.
- Left semi join.
- Left anti join.
- Cross join.
- Equi-join vs non-equi join.
- Broadcast joins.
- Sort-merge joins.
- Shuffle hash joins.
- Broadcast hash joins.
- Join hints.
- Join optimization.
- Data skew during joins.
- Handling large-to-small table joins.
- Handling large-to-large table joins.
- Choosing join strategies.

### **Step 10. Data skew and skew optimization**

- What is data skew?
- Why skew occurs.
- Uneven key distribution.
- Hot keys.
- Skewed joins.
- Skewed aggregations.
- Salting techniques.
- Key splitting.
- Broadcast alternatives.
- Adaptive Query Execution (AQE).
- Skew join optimization.
- Diagnosing long-running tasks.
- Handling extreme data skew in production.

### **Step 11. Spark SQL and Catalyst Optimizer**

- Logical query plans.
- Analyzed logical plans.
- Optimized logical plans.
- Physical plans.
- Catalyst optimization rules.
- Predicate pushdown.
- Projection pruning.
- Constant folding.
- Join reordering.
- Partition pruning.
- Column pruning.
- Filter pushdown.
- Explain plans.
- Cost-based optimization.
- Statistics collection.
- Query plan troubleshooting.

### **Step 12. Spark performance tuning**

- Executor memory.
- Driver memory.
- Executor cores.
- Number of executors.
- Memory overhead.
- JVM garbage collection.
- Serialization.
- Kryo serialization.
- Disk spill.
- Cache and persistence.
- Storage levels.
- Broadcast variables.
- Accumulators.
- Adaptive Query Execution.
- Dynamic resource allocation.
- Parallelism tuning.
- File sizing.
- Small-file optimization.
- Spark UI analysis.
- Query Profile interpretation.
- Performance benchmarking.

**Practical implementation:** Optimize a large customer-orders join, identify a skewed key and compare execution plans before and after tuning.

## **Level 4: Advanced — Data engineering with PySpark**

**Goal:** Build production-quality distributed ETL pipelines.

### **Step 13. PySpark transformations and data engineering**

- `select()`, `filter()`, `where()`.
- `withColumn()`, `drop()`, `alias()`.
- `groupBy()`, `agg()`.
- Window functions.
- Ranking functions.
- `row_number()`, `rank()`, `dense_rank()`.
- `lag()`, `lead()`.
- Running totals.
- Deduplication.
- Null handling.
- Conditional logic.
- Complex data types.
- Arrays.
- Structs.
- Maps.
- Explode and flatten.
- Date and timestamp handling.
- String and regular expressions.
- UDFs and Pandas UDFs.
- Built-in function optimization.

### **Step 14. File formats and storage integration**

- CSV.
- JSON.
- Parquet.
- ORC.
- Avro.
- Compression codecs.
- Schema evolution.
- Partitioned data.
- Reading partitioned datasets.
- Writing partitioned datasets.
- Append vs overwrite.
- Dynamic partition overwrite.
- Predicate pushdown.
- Column pruning.
- Small files and compaction.

### **Step 15. Incremental and CDC processing with Spark**

- Full load processing.
- Incremental processing.
- CDC event processing.
- Insert, update and delete handling.
- MERGE operations.
- Deduplication.
- Event ordering.
- Late-arriving records.
- Watermarks.
- Checkpointing.
- Replay.
- SCD Type 1.
- SCD Type 2.
- Handling out-of-order CDC events.
- Exactly-once processing boundaries.

### **Step 16. Lakehouse processing**

- Apache Iceberg.
- Delta Lake.
- Apache Hudi.
- ACID transactions.
- Transaction logs.
- Snapshot isolation.
- Time travel.
- Schema evolution.
- Partition evolution.
- Compaction.
- Upserts and deletes.
- Concurrent writes.
- Table maintenance.
- Spark integration with lakehouse formats.

We will study these technologies conceptually and implement at least one table format practically.

## **Level 5: Expert — Distributed systems and production engineering**

**Goal:** Understand how Spark behaves in real production clusters, including failure scenarios.

### **Step 17. Memory management and execution internals**

- JVM heap and off-heap memory.
- Execution memory.
- Storage memory.
- Unified memory management.
- Memory fraction.
- Garbage collection.
- Object allocation.
- Python worker memory.
- Arrow memory.
- Executor out-of-memory errors.
- Driver out-of-memory errors.
- Memory pressure.
- Disk spilling.
- Memory tuning strategies.

### **Step 18. Fault tolerance and recovery**

- RDD lineage.
- Task retries.
- Stage retries.
- Executor failures.
- Driver failures.
- Node failures.
- Speculative execution.
- Checkpointing.
- Reliable checkpointing.
- Cache recovery.
- Shuffle recovery.
- Application recovery.
- Idempotent processing.
- Data loss prevention.
- Recovery testing.

### **Step 19. Spark cluster deployment**

- Standalone cluster.
- YARN.
- Kubernetes.
- Databricks.
- Google Cloud Dataproc.
- AWS EMR.
- Azure Synapse Spark.
- Managed vs self-managed clusters.
- Cluster sizing.
- Autoscaling.
- Dynamic allocation.
- Resource isolation.
- Multi-tenant clusters.
- Cluster security.
- Production deployment patterns.

### **Step 20. Production monitoring and troubleshooting**

- Spark UI.
- Event logs.
- Driver logs.
- Executor logs.
- Task metrics.
- Stage metrics.
- SQL metrics.
- Executor utilization.
- GC time.
- Shuffle metrics.
- Spill metrics.
- Data skew detection.
- Failed tasks.
- Slow stages.
- Application monitoring.
- Alerting.
- Root cause analysis.

### **Step 21. PySpark engineering practices**

- Project structure.
- Modular code.
- Reusable transformations.
- Configuration-driven pipelines.
- Environment management.
- Logging.
- Exception handling.
- Unit testing.
- Integration testing.
- Data quality testing.
- Dependency management.
- Packaging.
- CI/CD.
- Code review.
- Production deployment.
- Secrets and credentials.
- Development vs production configurations.

## **Level 6: Specialist — Spark Structured Streaming**

**Goal:** Design continuous processing systems that handle real-time events reliably.

### **Step 22. Structured Streaming fundamentals**

- Streaming DataFrames.
- Micro-batch architecture.
- Continuous processing mode.
- Sources and sinks.
- Streaming queries.
- Trigger modes.
- Output modes.
- Processing guarantees.
- Streaming execution model.
- Batch vs streaming DataFrames.

### **Step 23. Streaming sources and sinks**

- Kafka source.
- File source.
- Rate source.
- Socket source for learning.
- Console sink.
- Memory sink.
- File sink.
- Kafka sink.
- Database integration.
- Cloud storage integration.
- Snowflake integration options.

### **Step 24. Event-time processing**

- Event time.
- Processing time.
- Ingestion time.
- Tumbling windows.
- Sliding windows.
- Session windows.
- Watermarks.
- Late-arriving events.
- State management.
- Stateful aggregations.
- Deduplication in streaming.
- Out-of-order events.

### **Step 25. Streaming reliability**

- Checkpoint locations.
- Offset management.
- Recovery after failure.
- State store.
- Exactly-once limitations.
- Idempotent sinks.
- Kafka offset coordination.
- Duplicate processing.
- Backpressure.
- Trigger intervals.
- Rate control.
- Failure recovery.
- DLQ integration.

### **Step 26. Advanced streaming architecture**

- Kafka + Spark Structured Streaming.
- CDC + Kafka + Spark.
- Stream-stream joins.
- Stream-static joins.
- Stateful processing.
- Event enrichment.
- Real-time aggregations.
- Sessionization.
- Fraud detection architecture.
- Real-time customer analytics.
- Streaming data quality.

## **Level 7: Specialist — Advanced distributed processing**

### **Step 27. Advanced Spark internals**

- Spark scheduler internals.
- DAG scheduling in depth.
- Task locality.
- Stage submission.
- Shuffle dependency.
- Map-side and reduce-side operations.
- Tungsten internals.
- Whole-stage code generation.
- UnsafeRow.
- Columnar processing.
- Vectorized execution.
- Arrow integration.
- Python-JVM serialization overhead.
- AQE internals.

### **Step 28. Advanced performance engineering**

- CPU profiling.
- Memory profiling.
- GC tuning.
- Network bottlenecks.
- Disk I/O bottlenecks.
- Executor sizing.
- Partition tuning at scale.
- Join strategy selection.
- Skew mitigation.
- Cost-based optimization.
- Workload isolation.
- Benchmarking.
- Performance regression testing.
- Cost/performance trade-offs.

### **Step 29. Advanced data processing patterns**

- Distributed aggregation.
- Incremental aggregation.
- Large-scale deduplication.
- Slowly changing dimensions.
- Sessionization.
- Complex event processing concepts.
- Graph processing fundamentals.
- Feature engineering.
- Large-scale ML preprocessing.
- Data migration.
- Backfills and historical reprocessing.
- Multi-source reconciliation.

### **Step 30. Alternative distributed processing technologies**

Understand how Spark compares with:

- Apache Flink.
- Apache Beam.
- Hadoop MapReduce.
- Trino.
- Presto.
- Dask.
- Ray.

We will focus on architecture and selection criteria, not attempt to become specialists in every competing engine.

# **Part 2 — Hands-on implementation roadmap**

Concepts alone are insufficient. We'll build progressively.

| Project                      | Implementation                               | Skills developed                   |
| ---------------------------- | -------------------------------------------- | ---------------------------------- |
| 1. Local Spark lab           | Install PySpark and process CSV/JSON/Parquet | Fundamentals                       |
| 2. AdventureWorks processing | Customers, products and orders               | DataFrames, SQL, transformations   |
| 3. Distributed ETL           | Large synthetic dataset                      | Partitions, parallelism, shuffling |
| 4. Performance lab           | Joins, skew, AQE, caching                    | Performance engineering            |
| 5. Incremental pipeline      | Watermarks and incremental processing        | Reliable ETL                       |
| 6. CDC processing            | Kafka CDC events → Spark                     | Change processing                  |
| 7. Lakehouse project         | Spark + Iceberg or Delta Lake                | ACID, MERGE, time travel           |
| 8. Streaming project         | Kafka → Structured Streaming → storage       | Real-time processing               |
| 9. Production simulation     | Failure recovery, monitoring, retries        | Operations                         |
| 10. Enterprise capstone      | Complete distributed data platform           | Architecture and engineering       |

## **Recommended practical environment**

Start locally, then progress to distributed clusters.

**Stage 1 — Local machine**

- Python.
- Java.
- PySpark.
- Jupyter or VS Code.
- Local CSV, JSON and Parquet files.

**Stage 2 — Distributed environment**

- Docker-based Spark cluster.
- Multiple workers.
- Spark UI.
- Kafka integration.

**Stage 3 — Cloud**

- GCP Dataproc or Dataproc Serverless.
- GCS.
- BigQuery.
- Pub/Sub.
- Optional Snowflake integration.

Your existing SQL Server, GCS, Kafka and CDC labs will provide useful source data and integration points. Since you haven't used PySpark in production, we'll build the practical experience from fundamentals rather than assume prior Spark expertise.

# **Part 3 — Real-world industrial scenarios**

These scenarios will be incorporated into the lessons and projects.

1. **Retail:** Process billions of sales records and build customer purchase behaviour.
2. **Banking:** Process millions of transactions and identify suspicious patterns.
3. **Manufacturing:** Process machine sensor events and calculate equipment performance.
4. **Healthcare:** Process large datasets with strict privacy and data quality requirements.
5. **Telecommunications:** Process call detail records and calculate usage metrics.
6. **E-commerce:** Process orders, payments, cancellations and customer activity.
7. **Financial analytics:** Build daily financial aggregation and reconciliation pipelines.
8. **IoT:** Process continuous sensor streams with late-arriving events.

We will cover real operational problems, not just ideal examples:

- Executor OOM.
- Driver OOM.
- Data skew.
- Shuffle explosion.
- Small-file explosion.
- Slow joins.
- Duplicate CDC events.
- Out-of-order events.
- Lost checkpoints.
- Schema changes.
- Late-arriving data.
- Cluster resource contention.
- Failed backfills.
- Incorrect partitioning.
- Unexpected cloud costs.
- Streaming consumer lag.

# **Part 4 — Interview preparation**

Since you also want interview material, each major topic will include questions and answers at three levels.

**Beginner**

- What is Apache Spark?
- What is PySpark?
- What is a DataFrame?
- What is lazy evaluation?
- What is a partition?
- What is a transformation?

**Advanced**

- Explain Spark's execution architecture.
- What causes a shuffle?
- Explain narrow vs wide transformations.
- How does Catalyst optimize queries?
- Explain broadcast joins.
- How do you handle data skew?
- How do you tune executor memory?
- What is AQE?

**Expert / Specialist**

- How would you process 5 TB of data daily?
- How would you troubleshoot a Spark job where one task runs much longer than others?
- How would you recover from executor failure?
- How do you handle out-of-order CDC events?
- How would you design an exactly-once streaming pipeline?
- How would you optimize a large skewed join?
- How would you choose between Spark and Flink?
- How would you design a multi-tenant Spark platform?

We will also include:

- SQL and PySpark coding exercises.
- Architecture design questions.
- Debugging exercises.
- Scenario-based questions.
- Mock system-design assessments.
- Practical assignments with evaluation.

# **Part 5 — How we will progress**

We'll teach each topic using this sequence:

1. Concept — understand what it is and why it exists.
2. Internal architecture — understand how it works.
3. Example — follow a simple real-world scenario.
4. Code — implement it using PySpark.
5. Execution — inspect what Spark actually does.
6. Production application — understand enterprise usage.
7. Edge cases — study failures and limitations.
8. Optimization — improve the implementation.
9. Interview material — test your understanding.
10. Practical exercise — demonstrate that you can apply it.

We won't rush through topics just to finish the syllabus. We'll advance when the concepts and practical exercises are understood.

# **Part 6 — Start learning: Lesson 1**

## **Distributed computing — Why do we need Apache Spark?**

Before writing PySpark code, understand the problem Spark was created to solve.

Imagine a company has 500 GB of sales data and needs to:

- Read all transactions.
- Remove duplicates.
- Join customer and product information.
- Calculate daily sales.
- Identify the top-performing products.
- Write the results to analytical storage.

A traditional Python script might attempt to load the entire dataset into memory.

```
import pandas as pddf = pd.read_csv("sales.csv")result = df.groupby("product_id")["amount"].sum()
```

This is straightforward, but what if:

- The dataset is 500 GB?
- Your machine has only 16 GB RAM?
- The processing takes several hours?
- You need to process 5 TB tomorrow?

A single machine has finite CPU, memory and storage bandwidth.

### **1. Vertical scaling vs horizontal scaling**

Vertical scaling means increasing the capacity of one machine.

```
One machine
   |
   +-- More CPU
   +-- More RAM
   +-- Faster storage
```

Horizontal scaling means adding multiple machines.

```
             Master / Driver
                    |
        +-----------+-----------+
        |           |           |
        v           v           v
      Node 1      Node 2      Node 3
        |           |           |
      Data 1      Data 2      Data 3
        |           |           |
      Process     Process     Process
        +-----------+-----------+
                    |
                    v
              Combined Result
```

Instead of processing the entire dataset on one machine, Spark distributes work across multiple machines.

### **2. What is distributed processing?**

Distributed processing divides a large workload into smaller tasks and executes them across multiple computing resources.

Example:

Suppose you have 300 million sales records.

Instead of processing all 300 million records on one machine:

```
300 million records
         |
         v
  Divide into partitions
         |
    +----+----+----+
    |    |    |    |
    v    v    v    v
  P1    P2   P3   P4
  75M   75M  75M  75M
    |    |    |    |
    v    v    v    v
 Worker Worker Worker Worker
    |    |    |    |
    +----+----+----+
         |
         v
    Final Result
```

Each worker processes its assigned data independently where possible.

This is parallel processing.

### **3. Important distinction: Distributed does not mean everything runs in parallel**

Some operations require data to be exchanged between workers.

For example:

```
df.groupBy("product_id").sum("sales_amount")
```

Suppose product 101 exists in all four partitions.

To calculate its total sales, Spark must bring together the relevant partial results.

This can require a shuffle.

```
Partition 1 ----+
Partition 2 ----+
Partition 3 ----+---- Shuffle ---- Group by product
Partition 4 ----+
```

Shuffling introduces network and disk overhead.

This is why understanding partitioning, joins and shuffle is essential to becoming proficient in Spark.

### **4. Five fundamental distributed computing concepts**

| Concept               | Meaning                                                        |
| --------------------- | -------------------------------------------------------------- |
| Distributed computing | Processing across multiple machines                            |
| Parallelism           | Executing independent tasks simultaneously                     |
| Partitioning          | Dividing data into smaller pieces                              |
| Shuffling             | Redistributing data across partitions                          |
| Fault tolerance       | Recovering from failures without losing the entire computation |

These five concepts form the foundation of the entire module.

**Remember this:** Spark is not simply Python running faster. It is a distributed execution engine that coordinates computation across multiple resources.

Lesson 1 — Assessment and corrections
1. Why Pandas may fail at 500 GB on 16 GB RAM
Correct
Exactly. Pandas primarily processes data in the memory of one machine. A 500 GB dataset cannot fit into 16 GB RAM.

One additional point: memory usage can be significantly higher than the source file size because parsing, intermediate DataFrames, joins and aggregations create additional objects.

2. Vertical vs horizontal scaling
Correct
Your explanation is right.
- Vertical scaling: increase resources on one machine (RAM, CPU, storage).
- Horizontal scaling: add more machines to distribute the workload.
An important architectural distinction: vertical scaling has hardware limits and can become expensive. Horizontal scaling introduces distributed-systems complexity, including network communication, coordination and failures.

3. Why partition data?
Correct
Correct. Partitioning divides a large dataset into smaller units that can be processed concurrently.

One refinement: partitioning doesn't automatically prevent out-of-memory errors. A partition can itself be too large, and an individual executor can still run out of memory.

4. Does adding workers guarantee faster execution?
Correct
Good example.

Other reasons additional workers might not help:
- The job has too little parallelism.
- A large shuffle creates network bottlenecks.
- Data skew causes one worker to process much more data.
- The workload is limited by storage or network throughput.
- Coordination overhead exceeds the benefit of extra workers.
5. What happens during a shuffle?
Correct concept; refine the terminology.
Your product-sales example is exactly right.

Suppose the data is distributed like this:
Worker 1                 Worker 2
---------                ---------
Product A = 100          Product B = 200
Product B = 150          Product A = 300
Product C = 50           Product C = 100


We need total sales per product.

Spark must redistribute records so that all records belonging to the same product are processed together:
Before shuffle:

Worker 1                 Worker 2
A=100                    B=200
B=150                    A=300
C=50                     C=100

          ↓ SHUFFLE

After shuffle:

Worker 1                 Worker 2
A=100                    B=200
A=300                    B=150

C=50                     C=100

          ↓ AGGREGATION

A = 400
B = 350
C = 150


The actual distribution depends on Spark's partitioning strategy.

Why is shuffle expensive?
1. Data may need to move across the network.
2. Intermediate data may be written to and read from disk.
3. Serialization and deserialization consume CPU.
4. Large shuffles consume memory and can cause disk spilling.
5. Uneven distribution can create straggler tasks.
Important: A shuffle is not simply data being exchanged randomly. It is a redistribution of records according to the requirements of an operation, such as groupBy, distinct or certain joins.

Lesson 1 — Final takeaway
You now understand the five foundational building blocks:
Concept	Purpose
Distributed computing	Process workloads across multiple machines
Parallelism	Execute independent work simultaneously
Partitioning	Divide data into manageable processing units
Shuffle	Redistribute data when operations require it
Fault tolerance	Recover from failures without necessarily restarting everything

# **Module 5 — Distributed Data Processing**

## **Lesson 2: Apache Spark Fundamentals**

Let's understand Spark from the ground up before writing PySpark code.

### **1. What is Apache Spark?**

**Apache Spark is a distributed data processing engine** used to process large datasets across multiple machines, either in memory or using disk when necessary.

In simple terms:

Imagine you have 500 GB of sales data and need to:

- Clean millions of records.
- Join customers with orders.
- Calculate monthly revenue.
- Identify duplicate transactions.
- Build analytical datasets.

A traditional Python script may struggle to process this efficiently on one machine.

Spark distributes the processing workload across multiple machines.

```
             500 GB Sales Data
                     |
                     v
               Apache Spark
                     |
          +----------+----------+
          |          |          |
          v          v          v
       Worker 1   Worker 2   Worker 3
       170 GB     165 GB     165 GB
          |          |          |
          v          v          v
       Process    Process    Process
          |          |          |
          +----------+----------+
                     |
                     v
              Combined Result
```

The numbers above are illustrative, not a guarantee that Spark divides data equally.

### **2. What problems does Spark solve?**

Spark addresses several important data engineering challenges.

| Problem                       | How Spark helps                           |
| ----------------------------- | ----------------------------------------- |
| Large datasets                | Distributed processing                    |
| Slow transformations          | Parallel execution and query optimization |
| Complex joins                 | Distributed join algorithms               |
| Repeated calculations         | Caching and persistence                   |
| Machine failures              | Task retries and lineage-based recovery   |
| Batch and streaming workloads | Spark SQL and Structured Streaming        |

Spark is not automatically faster than every alternative. For small datasets, Pandas or SQL may be simpler and faster.

### **3. Spark is an engine, not a database**

This distinction is important.

Spark does not need to permanently store your data. It reads data from storage systems, processes it and writes results to a destination.

For example:

```
                 DATA SOURCES
             +-------+-------+
             |       |       |
           GCS    Kafka   SQL Server
             |       |       |
             +-------+-------+
                     |
                     v
              APACHE SPARK
              (Processing)
                     |
             +-------+-------+
             |       |       |
             v       v       v
            GCS   BigQuery Snowflake
```

Spark is responsible for processing, not acting as the permanent database in this architecture.

This distinction helps when designing enterprise data platforms.

### **4. Apache Spark ecosystem**

Spark provides several components for different workloads.

| Component            | Purpose                      | Example                                     |
| -------------------- | ---------------------------- | ------------------------------------------- |
| Spark Core           | Distributed execution engine | Task scheduling, memory management          |
| Spark SQL            | Structured data processing   | SQL queries, DataFrames                     |
| PySpark              | Python API for Spark         | Python-based ETL                            |
| Structured Streaming | Stream processing            | Processing Kafka events                     |
| MLlib                | Machine learning library     | Classification, clustering                  |
| GraphX               | Graph processing             | Graph algorithms (primarily Scala/Java API) |

One clarification: PySpark is not a separate processing engine. It is the Python interface to Apache Spark.

**For our learning track, Spark SQL and PySpark will be particularly important.**

### **5. Spark vs other processing technologies**

Let's understand where each technology fits.

| Technology       | Primary purpose                       | Typical use                               |
| ---------------- | ------------------------------------- | ----------------------------------------- |
| Pandas           | Single-machine data analysis          | Small/medium datasets                     |
| Apache Spark     | Distributed processing                | Large-scale ETL and analytics             |
| Hadoop MapReduce | Distributed batch processing          | Traditional large-scale batch jobs        |
| Dask             | Parallel/distributed Python computing | Python analytics and scientific computing |
| Apache Flink     | Distributed stream processing         | Stateful, low-latency event processing    |
| Apache Beam      | Unified programming model             | Portable batch and streaming pipelines    |

Some important differences:

**Spark vs Pandas**

- Pandas generally processes data on one machine.
- Spark can distribute processing across multiple machines.
- Spark has additional execution and coordination overhead.

**Spark vs Hadoop MapReduce**

- MapReduce writes intermediate results to disk between processing stages.
- Spark can retain intermediate data in memory, when useful.
- Spark provides higher-level APIs such as DataFrames and Spark SQL.

**Spark vs Flink**

- Spark Structured Streaming primarily uses micro-batch execution by default.
- Flink is designed around continuous stream processing.
- Both support stateful distributed processing, but their execution models differ.

We'll study these alternatives in depth later.

### **6. Where Spark fits in your data engineering architecture**

Consider an enterprise pipeline:

```
SQL Server / Oracle / APIs / Kafka
                 |
                 v
             Ingestion
                 |
                 v
           GCS / Data Lake
                 |
                 v
          APACHE SPARK
        (Distributed ETL)
                 |
                 v
          Processed Data
                 |
                 v
        Snowflake / BigQuery
                 |
                 v
             dbt Models
                 |
                 v
          Tableau / Power BI
```

Spark can handle complex transformations, large joins, aggregations, CDC processing and data preparation.

It can complement your existing Airflow, GCS, Snowflake and dbt architecture rather than replacing all of them.

### **7. Practical example: When would you choose Spark?**

Consider three scenarios:

| Scenario                               | My recommendation | Reason                                    |
| -------------------------------------- | ----------------- | ----------------------------------------- |
| 50 MB CSV transformation               | Pandas            | Spark overhead isn't justified            |
| 500 GB historical sales transformation | Spark             | Distributed processing can help           |
| Millions of real-time financial events | Spark or Flink    | Depends on latency and state requirements |

The key architectural principle:

**Choose the processing engine based on data volume, latency, complexity, operational requirements and cost—not simply because the technology is popular.**

Lesson 2 — Assessment
1. What is Apache Spark?
Correct
Exactly right.

One refinement: Spark doesn't necessarily require multiple machines. It can run locally on a single machine or across a cluster.

It is called distributed because its execution model allows workloads to be divided and executed across multiple workers.
2. Is Spark a database or a processing engine?
Correct
Correct. One nuance: Spark can cache data in memory and write intermediate or final results to storage, but it isn't itself a permanent database.

Think of it this way:
- GCS: stores files.
- Snowflake: stores and queries analytical data.
- Spark: processes data.
- dbt: transforms and models data inside platforms such as Snowflake.
3. Difference between Spark Core and Spark SQL
Let's learn this now
This is an important distinction.

Spark Core is the underlying execution foundation of Apache Spark.

It provides capabilities such as:
- Distributed task execution.
- Scheduling jobs and tasks.
- Memory management.
- Fault tolerance.
- RDD operations.
- Communication between executors.
Spark SQL is the structured data processing component built on top of Spark Core.

It allows you to process structured data using:
- SQL queries.
- DataFrames.
- Dataset APIs (primarily Scala and Java).
- Query optimization through Catalyst.
Think of the architecture:
          PySpark / Scala / Java
                    |
          +---------+---------+
          |                   |
       Spark SQL           Spark Core APIs
          |                   |
     DataFrames              RDDs
          |                   |
          +---------+---------+
                    |
                Spark Core
                    |
       Distributed Execution Engine
                    |
        Executors / Cluster Resources


Example:

Using Spark SQL:
df.createOrReplaceTempView("sales")result = spark.sql("""    SELECT product_id, SUM(amount) AS total_sales    FROM sales    GROUP BY product_id""")



Using the DataFrame API:
from pyspark.sql import functions as Fresult = (    df.groupBy("product_id")      .agg(F.sum("amount").alias("total_sales")))



Both use Spark SQL's structured processing capabilities and eventually execute through Spark's underlying execution engine.

Remember: Spark Core provides the execution foundation; Spark SQL provides optimized structured-data processing.
4. Why Pandas for 50 MB instead of Spark?
Correct
Your reasoning is right.

A small correction: Pandas uses DataFrames too. Spark also has DataFrames, but they are distributed DataFrames.

For a 50 MB CSV, Pandas generally avoids the overhead of:
- Starting a Spark application.
- Creating a SparkSession.
- Scheduling distributed tasks.
- Managing executors and partitions.
- Coordinating execution.
Use the simplest tool that meets the requirements.
5. Where does Spark fit into your architecture?
Correct direction; refine the responsibilities
You suggested two possibilities:
1. GCS → Spark → Snowflake.
2. Source systems → Spark → GCS.
Both are valid.

However, Spark is not necessarily your ingestion tool. It is primarily a processing engine, although it can read directly from sources and write to destinations.

Consider this architecture:
SQL Server / Oracle
        |
        v
    Airflow
        |
        v
       GCS
   (Raw/Landing)
        |
        v
      Spark
 (Distributed ETL)
        |
        v
       GCS
 (Processed Data)
        |
        v
    Snowflake
        |
        v
       dbt
        |
        v
  Tableau / Power BI


Responsibilities:
Component	Responsibility
Airflow	Orchestrates pipeline execution
GCS	Stores raw and processed files
Spark	Performs distributed transformations
Snowflake	Analytical storage and SQL processing
dbt	Builds tested, documented analytical models

An important architectural decision: if your transformations are mostly SQL operations already handled efficiently by Snowflake, you may not need Spark at all.

Don't introduce Spark simply to make an architecture look more advanced.

Lesson 2 — What you should remember
1. Spark is a distributed processing engine.
2. Spark is not a permanent database.
3. Spark Core provides the underlying execution capabilities.
4. Spark SQL handles structured data processing and query optimization.
5. PySpark provides a Python interface to Spark.
6. Spark should be introduced when its processing capabilities justify its operational overhead.

# **Module 5 — Distributed Data Processing**

## **Lesson 3: PySpark Fundamentals — From Python to Spark Execution**

Now we start the practical side of distributed processing.

The objective is not just to learn PySpark commands. You need to understand what happens internally when you execute them.

### **1. What is PySpark?**

PySpark is the Python API for Apache Spark.

Apache Spark is primarily implemented in Scala and runs on the Java Virtual Machine (JVM). PySpark allows Python developers to interact with Spark's distributed processing capabilities.

For example, you write:

```
df = spark.read.csv("sales.csv", header=True)
```

Although you write Python, Spark performs the actual distributed data processing through its execution engine.

The architecture looks like this:

```
          YOUR PYTHON CODE
                 |
                 v
             PySpark API
                 |
                 v
       Py4J / Python-JVM Bridge
                 |
                 v
          SPARK APPLICATION
                 |
                 v
          Spark Execution Engine
                 |
          +------+------+ 
          |             |
          v             v
       Executor 1    Executor 2
          |             |
          v             v
       Process       Process
       Partition     Partition
```

Py4J is the bridge that allows Python to communicate with the JVM.

This architecture is important because Python code does not necessarily execute directly on the Spark executors. We'll explore Python workers, serialization and Arrow later.

### **2. Python vs JVM — why does it matter?**

Let's understand the two environments.

| Python                       | JVM                                     |
| ---------------------------- | --------------------------------------- |
| Runs Python code             | Runs Java/Scala bytecode                |
| PySpark API is exposed here  | Spark engine runs here                  |
| Python driver program        | Spark driver and executor processes     |
| Python UDFs can execute here | Native Spark SQL execution happens here |

Consider this:

```
result = df.groupBy("product_id").sum("amount")
```

You write the command in Python.

But Spark SQL constructs an execution plan, optimizes it and executes the distributed computation.

This is one reason DataFrame operations are generally preferred over ordinary Python UDFs: Spark can understand and optimize built-in DataFrame expressions.

We'll study this in detail later.

### **3. What is SparkSession?**

SparkSession is the main entry point for working with Spark DataFrames and Spark SQL.

Think of it as the interface through which your application communicates with Spark.

Example:

```
from pyspark.sql import SparkSessionspark = (    SparkSession.builder    .appName("MyFirstSparkApplication")    .master("local[*]")    .getOrCreate())
```

Let's understand each part.

| Code                   | Meaning                                              |
| ---------------------- | ---------------------------------------------------- |
| `SparkSession.builder` | Starts configuring a Spark application               |
| `appName()`            | Assigns an application name                          |
| `master("local[*]")`   | Runs Spark locally using available logical CPU cores |
| `getOrCreate()`        | Creates a new session or reuses an existing one      |

**Important:** `local[*]` does not create a distributed cluster. It runs Spark locally using multiple threads.

Later, we'll run Spark across multiple worker processes and eventually multiple machines.

### **4. SparkSession vs SparkContext**

These two terms are frequently confused.

**SparkContext**

The older, lower-level entry point for Spark Core functionality. It provides access to the underlying execution environment and RDD operations.

**SparkSession**

The higher-level entry point introduced in Spark 2.0. It provides access to DataFrames, Spark SQL and other Spark functionality. It also exposes the underlying SparkContext through `spark.sparkContext`.

```
sc = spark.sparkContextprint(sc.appName)print(sc.master)
```

In modern PySpark development, you generally create a SparkSession rather than creating a SparkContext independently.

### **5. What is a DataFrame in PySpark?**

A Spark DataFrame is a distributed collection of data organized into named columns.

Consider this dataset:

| customer_id | customer_name | city    |
| ----------- | ------------- | ------- |
| 101         | Amit          | Kolkata |
| 102         | Rahul         | Delhi   |
| 103         | Priya         | Mumbai  |

A Spark DataFrame represents this structured dataset.

Unlike a typical Pandas DataFrame, a Spark DataFrame is distributed across partitions and can be processed by multiple executors.

Example:

```
data = [    (101, "Amit", "Kolkata"),    (102, "Rahul", "Delhi"),    (103, "Priya", "Mumbai")]columns = ["customer_id", "customer_name", "city"]df = spark.createDataFrame(data, columns)df.show()
```

Output:

```
+-----------+-------------+-------+
|customer_id|customer_name|city   |
+-----------+-------------+-------+
|101        |Amit         |Kolkata|
|102        |Rahul        |Delhi  |
|103        |Priya        |Mumbai |
+-----------+-------------+-------+
```

Remember: this example is tiny and local. We use it to understand the API, not demonstrate distributed performance.

### **6. What is an RDD?**

RDD stands for Resilient Distributed Dataset.

It is a fundamental Spark Core abstraction representing a distributed collection of elements.

Three important characteristics:

- Resilient: Spark can recover lost partitions using lineage in many failure scenarios.
- Distributed: data can be spread across partitions and machines.
- Dataset: a collection of records.

Example:

```
numbers = [1, 2, 3, 4, 5]rdd = spark.sparkContext.parallelize(numbers)result = rdd.map(lambda x: x * 2)print(result.collect())
```

Output:

```
[2, 4, 6, 8, 10]
```

Here:

- `parallelize()` creates an RDD from a local collection.
- `map()` transforms each element.
- `collect()` brings all results back to the driver.

**Production warning:** `collect()` can cause driver memory exhaustion if the dataset is large. Use it only when the result is known to be small.

### **7. RDD vs DataFrame vs Dataset**

| Feature       | RDD                                | DataFrame               | Dataset                       |
| ------------- | ---------------------------------- | ----------------------- | ----------------------------- |
| Abstraction   | Low-level                          | High-level              | High-level, typed             |
| Structure     | Unstructured records               | Named columns           | Typed records                 |
| Optimization  | Limited automatic SQL optimization | Catalyst optimization   | Catalyst optimization         |
| APIs          | Python, Scala, Java                | Python, Scala, Java, R  | Scala, Java                   |
| Typical usage | Specialized low-level operations   | Modern data engineering | Typed Scala/Java applications |

One important clarification: PySpark supports RDDs and DataFrames, but does not expose the strongly typed Dataset API available in Scala and Java.

**Our default choice will be DataFrames and Spark SQL.** RDDs are still useful to understand Spark's foundations.

### **8. Reading files using PySpark**

Let's work with actual files.

**Read CSV**

```
df = (    spark.read    .option("header", True)    .option("inferSchema", True)    .csv("data/customers.csv"))
```

**Read JSON**

```
df = spark.read.json("data/customers.json")
```

**Read Parquet**

```
df = spark.read.parquet("data/customers.parquet")
```

Why Parquet is especially important:

- Columnar storage.
- Compression.
- Efficient analytical queries.
- Column pruning.
- Predicate pushdown.

We'll dedicate an entire lesson to file formats and storage.

### **9. Schema inference vs explicit schema**

When Spark reads a CSV, it can infer column data types.

```
.option("inferSchema", True)
```

However, schema inference requires Spark to inspect the data and may involve additional reading.

For production pipelines, defining the schema explicitly is often safer.

Example:

```
from pyspark.sql.types import (    StructType,    StructField,    IntegerType,    StringType)schema = StructType([    StructField("customer_id", IntegerType(), True),    StructField("customer_name", StringType(), True),    StructField("city", StringType(), True)])df = (    spark.read    .option("header", True)    .schema(schema)    .csv("data/customers.csv"))
```

Benefits:

1. Predictable data types.
2. Better control over schema changes.
3. Reduced reliance on inference.
4. Easier validation and data quality checks.

One production consideration: a declared schema does not guarantee every input value is valid. You must still handle malformed records and conversion errors.

_____________________________________________________________________________________________________________________________________________________________________
---------------------------------------------------------------------------------------------------------------------------------------------------------------------
_____________________________________________________________________________________________________________________________________________________________________

# **Module 5 — Distributed Data Processing**

## **Lesson 4: Spark architecture and execution model**

This is the foundation for everything else in Spark.

### **1. Spark architecture**

A Spark application has these main components:

| Component       | Responsibility                                       |
| --------------- | ---------------------------------------------------- |
| Driver          | Controls the application and coordinates execution   |
| SparkSession    | Main entry point for DataFrame and SQL operations    |
| SparkContext    | Connects the driver to Spark's execution environment |
| Cluster Manager | Allocates resources across the cluster               |
| Worker Node     | Machine that provides resources for execution        |
| Executor        | Process that executes tasks and stores data          |
| Task            | Smallest unit of execution sent to an executor       |

Architecture:

```
             SPARK APPLICATION
                    |
             +-------------+
             |   DRIVER    |
             |             |
             | SparkSession|
             | SparkContext|
             | DAG Scheduler|
             | Task Scheduler|
             +-------------+
                    |
             Cluster Manager
                    |
          +---------+---------+
          |                   |
     Worker Node 1        Worker Node 2
          |                   |
     +----------+         +----------+
     | Executor |         | Executor |
     |          |         |          |
     | Task 1   |         | Task 3   |
     | Task 2   |         | Task 4   |
     +----------+         +----------+
```

**Driver:** The brain of the application. It builds execution plans, coordinates tasks, and tracks application progress. The driver does not normally perform all the data processing itself.

**Executor:** Runs tasks on behalf of the driver. Executors also maintain cached data and report execution status.

**Cluster Manager:** Allocates CPU and memory resources. Examples include Spark Standalone, YARN and Kubernetes.

Important distinction: a worker is a machine; an executor is a process running on that machine.

### **2. Job, Stage and Task**

These three terms are fundamental.

- **Job:** Created when an action triggers execution.
- **Stage:** A group of tasks that can execute without crossing a shuffle boundary.
- **Task:** Processes one partition of data.

Example:

```
result = (    df.filter(F.col("amount") > 5000)      .groupBy("city")      .agg(F.sum("amount").alias("total")))result.show()
```

Conceptual execution:

```
Action: show()
     |
     v
   JOB
     |
     v
  STAGE 1
  Read + Filter + Partial Aggregate
     |
     v
  SHUFFLE
     |
     v
  STAGE 2
  Final Aggregate
     |
     v
   OUTPUT
```

A stage boundary is generally introduced by a shuffle dependency.

If Spark has 100 partitions to process in a stage, it can create approximately 100 tasks for that stage.

Remember:

- One job can contain multiple stages.
- One stage can contain multiple tasks.
- Tasks process individual partitions.
- Tasks within a stage can run in parallel, subject to available resources.

### **3. DAG — Directed Acyclic Graph**

Spark represents processing operations as a DAG.

- Directed: operations have a defined direction.
- Acyclic: the dependency graph does not contain cycles.
- Graph: operations and their dependencies are represented as connected nodes.

Example:

```
Read customers
      |
   Filter
      |
   Select
      |
     Join <---- Read orders
      |
   GroupBy
      |
   Write
```

Spark uses this dependency graph to plan and execute processing.

The DAG Scheduler divides work into stages. The Task Scheduler launches individual tasks on available executors.

### **4. What happens when you submit a Spark application?**

Typical lifecycle:

1. Python application starts.
2. SparkSession is created.
3. Driver initializes SparkContext.
4. Resources are requested from the cluster manager.
5. Executors are started.
6. You define DataFrame transformations.
7. Spark builds a logical plan.
8. An action triggers execution.
9. Catalyst optimizes the logical plan.
10. Spark generates a physical execution plan.
11. DAG Scheduler creates stages.
12. Task Scheduler distributes tasks to executors.
13. Executors process data.
14. Results are returned or written to the destination.
15. Application shuts down and releases resources.

This is the complete high-level execution lifecycle.

### **5. Local mode versus cluster mode**

You currently use:

```
.master("local[*]")
```

This means Spark runs on your own machine, using available logical CPU cores.

It is useful for learning and development, but it is not a distributed multi-machine cluster.

| Mode       | Description                               |
| ---------- | ----------------------------------------- |
| `local`    | One execution thread                      |
| `local[4]` | Four execution threads                    |
| `local[*]` | Uses available logical CPU cores          |
| Standalone | Spark's own cluster manager               |
| YARN       | Hadoop cluster resource manager           |
| Kubernetes | Runs Spark driver/executors as containers |
| Dataproc   | Google Cloud managed Spark service        |
| EMR        | AWS managed big-data platform             |

In a production environment, your Spark application might run on Dataproc or Kubernetes, read data from GCS, transform it and write to BigQuery or another destination.

## **Lesson 5: DataFrame, RDD and Dataset**

Spark has different abstractions for processing data.

### **RDD — Resilient Distributed Dataset**

RDD is Spark's lower-level distributed data abstraction.

- Resilient: can recover from certain failures.
- Distributed: data can be spread across partitions.
- Dataset: collection of records.

Example:

```
rdd = spark.sparkContext.parallelize(    [10, 20, 30, 40, 50])result = rdd.map(lambda x: x * 2)print(result.collect())
```

Output:

```
[20, 40, 60, 80, 100]
```

Important RDD operations:

| Operation       | Type           | Purpose                      |
| --------------- | -------------- | ---------------------------- |
| `map()`         | Transformation | Transform each record        |
| `flatMap()`     | Transformation | Produce zero or more records |
| `filter()`      | Transformation | Filter records               |
| `reduceByKey()` | Transformation | Aggregate by key             |
| `groupByKey()`  | Transformation | Group values by key          |
| `collect()`     | Action         | Bring all records to driver  |
| `count()`       | Action         | Count records                |
| `reduce()`      | Action         | Aggregate into one result    |

Avoid using `collect()` on large datasets because it brings all records into driver memory.

### **DataFrame**

A DataFrame is a distributed table with named columns and a schema.

Example:

```
df = spark.createDataFrame(    [(1, "Amit", 5000), (2, "Rahul", 7000)],    ["id", "name", "amount"])df.filter(F.col("amount") > 5000).show()
```

DataFrames are the preferred abstraction for most modern PySpark ETL workloads.

Why?

- Schema awareness.
- Catalyst optimization.
- Efficient execution through Spark SQL.
- Column-based operations.
- Better optimization opportunities than arbitrary Python functions.

### **Dataset**

Spark Datasets provide a strongly typed API, primarily in Scala and Java.

PySpark does not expose the same strongly typed Dataset API.

### **Comparison**

| Feature               | RDD                    | DataFrame      | Dataset                    |
| --------------------- | ---------------------- | -------------- | -------------------------- |
| Abstraction           | Low-level              | Structured     | Structured and typed       |
| Schema                | No fixed schema        | Yes            | Yes                        |
| Catalyst optimization | Limited                | Yes            | Yes                        |
| Type safety           | Language-dependent     | Runtime schema | Compile-time in Scala/Java |
| Typical use           | Specialized processing | Modern ETL     | Scala/Java applications    |

**Practical recommendation:** Use DataFrames and Spark SQL for most PySpark pipelines. Use RDDs only when a lower-level operation is genuinely needed.

## **Lesson 6: Partitions and parallelism**

A partition is a logical division of a dataset.

Suppose a dataset has one million records and 10 partitions.

```
Dataset: 1,000,000 records

Partition 1: 100,000
Partition 2: 100,000
Partition 3: 100,000
...
Partition 10: 100,000
```

Spark can process different partitions concurrently.

### **Why partitions matter**

Too few partitions:

- Limited parallelism.
- Underutilized executors.
- Larger individual tasks.

Too many partitions:

- Scheduling overhead.
- Excessive small tasks.
- More task-management overhead.

Uneven partitions:

- Some tasks finish quickly.
- One task takes much longer.
- The entire stage waits for the slow task.

This is called the straggler problem.

### **Inspect partitions**

```
print(df.rdd.getNumPartitions())
```

### **Repartition**

```
df2 = df.repartition(10)
```

Redistributes records across 10 partitions. Generally involves a shuffle.

### **Coalesce**

```
df3 = df.coalesce(5)
```

Reduces partitions, generally avoiding a full shuffle when reducing the number.

| repartition                         | coalesce                     |
| ----------------------------------- | ---------------------------- |
| Can increase or decrease partitions | Primarily reduces partitions |
| Usually causes shuffle              | Usually avoids full shuffle  |
| More expensive                      | Generally cheaper            |

### **Partitioning a dataset by columns**

When writing data:

```
df.write \  .mode("overwrite") \  .partitionBy("city") \  .parquet("output/customers")
```

This creates directory-based partitioning:

```
customers/
    city=Kolkata/
    city=Delhi/
    city=Mumbai/
```

This is storage partitioning, which is related to but different from Spark's in-memory execution partitions.

## **Lesson 7: Shuffle architecture**

Shuffle is one of the most important concepts in distributed processing.

A shuffle redistributes data across partitions, usually across executors, based on a key or another distribution requirement.

Operations that commonly cause shuffles:

- `groupBy`
- `distinct`
- `orderBy`
- `repartition`
- Many joins
- `reduceByKey`

Example:

```
df.groupBy("city").agg(F.sum("amount"))
```

Spark must bring records with the same city together to calculate their final aggregate.

### **Shuffle lifecycle**

```
Executor 1                 Executor 2
     |                          |
 Partial results           Partial results
     |                          |
     +------------+-------------+
                  |
             Shuffle Write
                  |
           Network transfer
                  |
             Shuffle Read
                  |
          New partition layout
                  |
          Final aggregation
```

Shuffle involves:

- Serialization.
- Network transfer.
- Disk I/O.
- Memory usage.
- Sorting or hashing, depending on the operation and execution path.

### **Why shuffle is expensive**

Imagine 500 GB of records distributed across 20 executors. A groupBy may require moving significant amounts of data between executors.

This creates network and disk pressure.

### **How to reduce shuffle**

1. Filter unnecessary rows early.
2. Select only required columns.
3. Pre-aggregate where appropriate.
4. Avoid unnecessary `distinct()`.
5. Use suitable partitioning.
6. Broadcast small tables during joins.
7. Avoid repeatedly repartitioning data without a reason.

One critical point: filtering early is useful, but Spark's Catalyst optimizer can already push many filters toward the data source. This is called predicate pushdown.

## **Lesson 8: Joins in Spark**

Joins combine records from two datasets using a condition.

Example:

```
customers = spark.createDataFrame(    [(1, "Amit"), (2, "Rahul"), (3, "Priya")],    ["customer_id", "name"])orders = spark.createDataFrame(    [(101, 1, 5000), (102, 1, 3000), (103, 2, 7000)],    ["order_id", "customer_id", "amount"])result = customers.join(    orders,    on="customer_id",    how="inner")result.show()
```

### **Join types**

| Join       | Result                                      |
| ---------- | ------------------------------------------- |
| Inner      | Matching records only                       |
| Left       | All left records and matching right records |
| Right      | All right records and matching left records |
| Full outer | All records from both sides                 |
| Left semi  | Left records having a match                 |
| Left anti  | Left records without a match                |
| Cross      | Cartesian product                           |

### **Spark join strategies**

This is important for understanding performance.

**1. Broadcast Hash Join**

Used when one side is small enough to broadcast to executors.

```
from pyspark.sql.functions import broadcastresult = large_df.join(    broadcast(small_df),    "customer_id")
```

Concept:

```
             Small table
                  |
             Broadcast
                  |
       +----------+----------+
       |          |          |
    Executor 1 Executor 2 Executor 3
       |          |          |
    Large data  Large data  Large data
       |          |          |
       +----------+----------+
                  |
              Join result
```

The small dataset is distributed to executors, reducing the need for a large shuffle.

**2. Sort Merge Join**

Commonly used when both datasets are large and join keys can be partitioned and sorted.

```
Large table A         Large table B
     |                     |
Partition by key      Partition by key
     |                     |
    Sort                  Sort
     |                     |
     +---------+-----------+
               |
          Merge join
```

**3. Shuffle Hash Join**

Data is shuffled by join key and a hash table is constructed for one side within each partition.

**4. Nested Loop Join**

Can occur for certain non-equi joins and cross joins. It may be expensive for large datasets.

### **Broadcast join considerations**

Broadcasting is not automatically better.

Potential problems:

- Large broadcast memory consumption.
- Executor memory pressure.
- Broadcast timeout.
- Excessive network transfer.

Use it when the smaller side is genuinely small enough for the cluster's resources.

## **Lesson 9: Data skew**

Data skew occurs when some partitions contain significantly more data than others.

Example:

| Customer        | Records |
| --------------- | ------- |
| Customer A      | 900,000 |
| Customer B      | 30,000  |
| Customer C      | 20,000  |
| Other customers | 50,000  |

If a join or groupBy partitions data by customer, Customer A might dominate one partition.

Consequences:

- One executor runs much longer.
- Other executors finish early.
- Memory pressure and possible OOM.
- Stage completion is delayed.

### **Skew-handling strategies**

1. Enable Adaptive Query Execution.
2. Use skew join optimization.
3. Salt skewed keys.
4. Pre-aggregate data where appropriate.
5. Handle null/default keys separately when they dominate.
6. Revisit data distribution and join strategy.

**Salting example:**

Instead of sending all records for one hot key to a single partition, append a salt value to distribute the records.

Concept:

```
Original key: CUSTOMER_A

Salted keys:
CUSTOMER_A_0
CUSTOMER_A_1
CUSTOMER_A_2
CUSTOMER_A_3
```

When joining, the other dataset must be expanded or adjusted consistently so that the join remains correct.

Salting is a powerful technique, but it adds complexity. Do not apply it to every join.

## **Lesson 10: Catalyst optimizer and Tungsten**

Spark SQL has an optimizer that improves execution plans.

### **Catalyst**

Catalyst is Spark SQL's query optimization framework.

Simplified processing:

```
DataFrame / SQL
       |
       v
Unresolved Logical Plan
       |
       v
Analyzed Logical Plan
       |
       v
Optimized Logical Plan
       |
       v
Physical Plan
       |
       v
Code Generation / Execution
```

Important optimizations:

| Optimization         | Purpose                                             |
| -------------------- | --------------------------------------------------- |
| Predicate pushdown   | Apply filters closer to the source                  |
| Projection pruning   | Read only required columns                          |
| Constant folding     | Evaluate constant expressions early                 |
| Constant propagation | Use known values to simplify expressions            |
| Join reordering      | Choose a more efficient join order where applicable |
| Partition pruning    | Skip irrelevant storage partitions                  |
| Column pruning       | Avoid unnecessary column processing                 |

### **Tungsten**

Tungsten is a collection of Spark execution-engine optimizations focused on CPU and memory efficiency.

Important concepts:

- Binary processing and memory management.
- Reduced object overhead.
- Whole-stage code generation.
- More efficient CPU execution.

For PySpark users, the practical lesson is:

Prefer built-in DataFrame functions over Python UDFs whenever possible.

Example:

```
# Preferreddf.withColumn(    "amount_with_tax",    F.col("amount") * 1.18)
```

Instead of:

```
from pyspark.sql.functions import udffrom pyspark.sql.types import DoubleTypedef add_tax(amount):    return amount * 1.18tax_udf = udf(add_tax, DoubleType())df.withColumn("amount_with_tax", tax_udf("amount"))
```

A conventional Python UDF can introduce Python/JVM serialization overhead and limit optimization.

## **Lesson 11: Adaptive Query Execution (AQE)**

AQE optimizes parts of the execution plan using runtime statistics.

It can:

- Coalesce small shuffle partitions.
- Optimize certain skewed joins.
- Change join strategies when runtime statistics justify it.

Check:

```
spark.conf.get("spark.sql.adaptive.enabled")
```

Enable:

```
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

AQE is commonly enabled by default in modern Spark configurations, but verify the actual configuration in your environment.

## **Lesson 12: Spark performance tuning**

A practical performance framework:

| Area          | What to examine                          |
| ------------- | ---------------------------------------- |
| Input         | File size, number of files, format       |
| Partitions    | Number and size of partitions            |
| Filtering     | Filter early, avoid unnecessary rows     |
| Columns       | Avoid selecting unused columns           |
| Joins         | Broadcast, sort merge, skew              |
| Shuffle       | Minimize unnecessary exchanges           |
| Memory        | Executor memory, overhead, caching       |
| Serialization | Avoid unnecessary Python/JVM conversions |
| Output        | Avoid too many tiny files                |
| Execution     | Inspect Spark UI and query plans         |

### **Caching and persistence**

If a DataFrame is reused multiple times, caching can avoid recomputing it.

```
df.cache()df.count()df.filter(F.col("amount") > 5000).show()df.groupBy("city").count().show()df.unpersist()
```

Important:

- `cache()` is lazy.
- The first action materializes the cached data.
- Caching consumes memory and potentially disk.
- Do not cache everything.

### **Storage levels**

```
from pyspark import StorageLeveldf.persist(StorageLevel.MEMORY_AND_DISK)
```

This allows Spark to keep cached data in memory where possible and spill to disk as necessary.

### **Explain plans**

Use:

```
df.explain()df.explain("extended")df.explain("formatted")
```

Understand the difference between:

- Logical plan.
- Optimized logical plan.
- Physical plan.

### **Spark UI**

The Spark UI is essential for production troubleshooting.

Default local URL:

```
http://localhost:4040
```

It provides:

- Jobs.
- Stages.
- Tasks.
- SQL queries.
- Executors.
- Storage.
- Environment.
- Event timelines.

Important metrics:

- Task duration.
- Shuffle read/write.
- Input/output bytes.
- Spill to memory/disk.
- Executor memory.
- Failed tasks.
- Skew between task durations.

## **Lesson 13: File formats and data layout**

Spark works with many formats.

| Format  | Characteristics                 | Typical use              |
| ------- | ------------------------------- | ------------------------ |
| CSV     | Plain text, weak schema support | Data exchange            |
| JSON    | Semi-structured                 | API and event data       |
| Avro    | Row-oriented, schema-based      | Event pipelines          |
| Parquet | Columnar                        | Analytics and data lakes |
| ORC     | Columnar                        | Analytical workloads     |
| Delta   | Parquet + transaction log       | Lakehouse                |
| Iceberg | Open table format               | Large analytical tables  |
| Hudi    | Open table format               | Incremental data lakes   |

### **Why Parquet matters**

Parquet stores data in columnar form.

If a table has 100 columns and your query needs only 5, Spark may read only those required columns.

This is called column pruning.

Example:

```
df.write.mode("overwrite").parquet("output/customers")
```

Read:

```
df = spark.read.parquet("output/customers")
```

Partitioned output:

```
df.write \  .mode("overwrite") \  .partitionBy("year", "month") \  .parquet("output/sales")
```

Be careful not to partition by extremely high-cardinality columns such as customer IDs. That can create excessive directories and small files.

## **Lesson 14: Incremental processing and CDC**

Batch processing often reads all records repeatedly. Incremental processing reads only newly created or changed data.

Example:

```
Initial load:
1 million records

Next run:
Only 5,000 new/changed records
```

### **Watermark-based incremental loading**

A common approach is tracking the last processed timestamp.

```
SELECT *
FROM dbo.Customers
WHERE UpdatedAt > ?
```

The pipeline stores the last successful watermark.

Important production edge cases:

- Late-arriving records.
- Equal timestamps.
- Source deletes.
- Failed loads.
- Duplicate records.
- Time zone consistency.
- Watermark update before or after successful commit.

Use a lookback window when appropriate:

```
Last successful watermark: 10:00

Next extraction:
Read records from 09:55 onwards
Deduplicate and merge
```

This helps capture delayed updates.

### **CDC**

Change Data Capture tracks inserts, updates and deletes.

Typical architecture:

```
SQL Server CDC
      |
      v
Debezium
      |
      v
Kafka
      |
      v
Spark Structured Streaming
      |
      v
Bronze / Raw
      |
      v
Silver / Cleaned
      |
      v
Gold / Analytics
```

Spark can process CDC events, but correct handling requires:

- Primary keys.
- Operation type.
- Event ordering.
- Event timestamps.
- Deduplication.
- Delete propagation.
- Idempotent writes.

**Important:** A CDC event stream is not automatically a current-state table. You must apply events in the correct order and maintain the latest state.

## **Lesson 15: Lakehouse architecture**

A lakehouse combines data lake storage with table-management and analytical capabilities.

Common architecture:

```
Sources
   |
   v
Ingestion
   |
   v
Bronze - Raw data
   |
   v
Silver - Cleaned and validated
   |
   v
Gold - Business-ready data
   |
   v
BI / ML / Analytics
```

### **Delta Lake**

Key features:

- ACID transactions.
- Schema enforcement.
- Schema evolution.
- Time travel.
- MERGE/upsert.
- Deletes and updates.
- Transaction log.

### **Apache Iceberg**

Key features:

- Open table format.
- Snapshot-based table management.
- Schema and partition evolution.
- Hidden partitioning.
- Time travel.
- Multi-engine support.

### **Apache Hudi**

Key features:

- Incremental processing.
- Upserts and deletes.
- Record-level indexing.
- Change data capture patterns.
- Copy-on-write and merge-on-read table types.

### **Comparison**

| Feature                | Delta | Iceberg | Hudi         |
| ---------------------- | ----- | ------- | ------------ |
| ACID                   | Yes   | Yes     | Yes          |
| Time travel            | Yes   | Yes     | Yes          |
| Schema evolution       | Yes   | Yes     | Yes          |
| Upserts                | Yes   | Yes     | Yes          |
| Incremental processing | Yes   | Yes     | Strong focus |
| Open ecosystem         | Yes   | Yes     | Yes          |

The right choice depends on engine compatibility, workload, ecosystem, governance and operational requirements.

## **Lesson 16: Memory management and fault tolerance**

### **Memory**

Spark memory is used for:

- Execution.
- Caching.
- Shuffle processing.
- Joins.
- Aggregations.
- Serialization and other runtime overhead.

Common problems:

**Driver OOM**

- Large `collect()`.
- Huge query results.
- Excessive metadata or task scheduling.
- Large objects held in the driver.

**Executor OOM**

- Oversized partitions.
- Large joins.
- Skew.
- Too much caching.
- Memory-intensive UDFs.

### **Fault tolerance**

Spark can recover from failures through:

- Task retries.
- Partition recomputation.
- RDD lineage.
- Executor replacement, depending on deployment.
- Checkpointing in appropriate workloads.

**Lineage:** Spark tracks the operations needed to reconstruct lost partitions.

**Checkpointing:** Saves data to reliable storage and truncates long lineage.

```
spark.sparkContext.setCheckpointDir("checkpoint")checkpointed_df = df.checkpoint()
```

Checkpointing is especially useful for long-running iterative processing and streaming state management, but it has storage and execution costs.

## **Lesson 17: Deployment and orchestration**

Spark execution and workflow orchestration are different responsibilities.

| Tool       | Responsibility                   |
| ---------- | -------------------------------- |
| Spark      | Distributed data processing      |
| Airflow    | Workflow orchestration           |
| Kubernetes | Container orchestration          |
| YARN       | Cluster resource management      |
| Dataproc   | Managed Spark on GCP             |
| EMR        | Managed big-data services on AWS |

Example enterprise workflow:

```
Airflow DAG
    |
    +---- Extract source data
    |
    +---- Submit Spark job
    |
    +---- Validate output
    |
    +---- Run dbt transformations
    |
    +---- Run data quality tests
    |
    +---- Notify downstream consumers
```

Spark can be submitted through `spark-submit`.

Example:

```
spark-submit \
  --master local[*] \
  --name customer_processing \
  customer_pipeline.py
```

Production deployment adds resource configurations, environment-specific settings, secrets management, monitoring and retry policies.

## **Lesson 18: Structured Streaming**

Structured Streaming is Spark's high-level streaming engine using the DataFrame and SQL model.

Instead of manually processing individual events, you define a streaming DataFrame and transformations.

Example:

```
stream_df = (    spark.readStream    .format("json")    .schema(schema)    .load("input/events"))result = stream_df.groupBy("city").count()query = (    result.writeStream    .format("console")    .outputMode("complete")    .start())query.awaitTermination()
```

### **Streaming concepts**

**Micro-batch:** Spark processes incoming data in small batches.

**Continuous processing:** An alternative execution mode with different limitations and availability; micro-batch is the common approach.

**Trigger:** Controls when processing runs.

**Output modes:**

| Mode     | Purpose                        |
| -------- | ------------------------------ |
| Append   | Writes new result rows         |
| Update   | Writes changed result rows     |
| Complete | Writes the entire result table |

### **Event time versus processing time**

- Event time: when an event actually occurred.
- Processing time: when Spark processes it.

Example:

An order was created at 10:00 but arrived at Spark at 10:08.

Event time = 10:00.

Processing time = 10:08.

This distinction is crucial for accurate streaming aggregations.

### **Watermarks**

Watermarks control how long Spark waits for late-arriving events in event-time operations.

```
windowed = (    stream_df    .withWatermark("event_time", "10 minutes")    .groupBy(        F.window("event_time", "5 minutes"),        "city"    )    .count())
```

Watermarks help manage state and late data. They do not mean that every event arriving later than the watermark is guaranteed to be discarded in every possible situation; behavior depends on the operation and output mode.

### **Checkpointing**

Streaming checkpoints preserve progress and state across restarts.

```
query = (    stream_df.writeStream    .format("parquet")    .option("path", "output/events")    .option("checkpointLocation", "checkpoint/events")    .start())
```

Use a durable checkpoint location in production.

### **Delivery semantics**

Understand these distinctions:

- At-most-once.
- At-least-once.
- Exactly-once.

Exactly-once end-to-end processing requires cooperation between the source, Spark's checkpointing and the sink's commit/idempotency capabilities. It is not guaranteed merely by using Structured Streaming.

## **Lesson 19: Advanced streaming architecture**

Production streaming needs additional design considerations.

| Concept             | Purpose                                              |
| ------------------- | ---------------------------------------------------- |
| Stateful processing | Maintains information across micro-batches           |
| Windowing           | Aggregates events over time windows                  |
| Watermarks          | Manages late events and state                        |
| Deduplication       | Removes repeated events                              |
| Checkpointing       | Recovers progress and state                          |
| Backpressure        | Manages incoming workload versus processing capacity |
| Kafka offsets       | Tracks consumed positions                            |
| Dead-letter queue   | Isolates malformed or unprocessable events           |

Common use cases:

- Real-time fraud detection.
- Clickstream processing.
- IoT event processing.
- Financial transactions.
- CDC replication.
- Real-time dashboards.

## **Lesson 20: Alternatives to Spark**

You should understand when Spark is not the right choice.

| Technology       | Primary purpose                               |
| ---------------- | --------------------------------------------- |
| Apache Flink     | Stateful real-time stream processing          |
| Apache Beam      | Unified batch and streaming programming model |
| Hadoop MapReduce | Disk-oriented distributed batch processing    |
| Trino            | Distributed SQL query engine                  |
| Presto           | Distributed SQL analytics                     |
| Dask             | Parallel Python computing                     |
| Ray              | Distributed Python and AI workloads           |
| Snowflake        | Managed cloud analytical platform             |
| BigQuery         | Serverless cloud data warehouse               |

### **Spark versus Flink**

Spark Structured Streaming commonly uses micro-batch execution.

Flink is designed around continuous stream processing and event-time/stateful processing.

### **Spark versus Trino**

Spark is a general distributed processing engine, suitable for complex transformations and batch/stream workloads.

Trino is primarily a distributed SQL query engine designed for interactive querying across data sources.

### **Spark versus Snowflake**

Spark gives you greater control over distributed processing code and infrastructure choices.

Snowflake provides a managed analytical environment with SQL-based processing and warehouse management.

If transformations can be handled efficiently using Snowflake SQL and dbt, introducing Spark may add unnecessary operational complexity.

## **Module 5 — Completion map**

Core theory covered

| Topic                          | Status  |
| ------------------------------ | ------- |
| Distributed computing          | Covered |
| Spark fundamentals             | Covered |
| PySpark fundamentals           | Covered |
| Driver and executors           | Covered |
| Jobs, stages, tasks and DAG    | Covered |
| RDD, DataFrame, Dataset        | Covered |
| Partitions and parallelism     | Covered |
| Shuffle                        | Covered |
| Joins and join strategies      | Covered |
| Data skew                      | Covered |
| Catalyst and Tungsten          | Covered |
| AQE                            | Covered |
| Performance tuning             | Covered |
| File formats                   | Covered |
| Incremental processing and CDC | Covered |
| Delta, Iceberg, Hudi           | Covered |
| Memory and fault tolerance     | Covered |
| Deployment                     | Covered |
| Structured Streaming           | Covered |
| Advanced streaming             | Covered |
| Spark alternatives             | Covered |

This completes the main conceptual coverage of Module 5. We will still need to implement and inspect these concepts in practice; reading about them does not replace hands-on understanding.

## **What comes next**

The remaining programme should now focus on the other architecture domains, followed by implementation and the enterprise capstone. The next major areas to complete are:

1. **Data ingestion architecture:** Batch, micro-batch, streaming, CDC, APIs, files, database extraction, reliability and idempotency.
2. **Data storage architecture:** Data lakes, warehouses, lakehouses, object storage, partitioning, formats and table design. We will keep your earlier decision to skip the dedicated Module 4, but cover storage concepts needed to understand the remaining architecture.
3. **Data modelling:** Conceptual, logical and physical modelling; dimensional modelling; star/snowflake schemas; facts, dimensions, grain, SCD and Data Vault.
4. **Transformation architecture:** ETL versus ELT, dbt, SQL, Spark, incremental models, testing, lineage and semantic layers.
5. **Orchestration and workflow architecture:** Airflow, DAG design, retries, dependencies, backfills, SLAs and event-driven workflows.
6. **Data governance and security:** Catalogues, metadata, lineage, RBAC, IAM, masking, encryption, PII, retention and auditability.
7. **Data quality and observability:** Validation, reconciliation, anomaly detection, freshness, completeness, monitoring and alerting.
8. **Enterprise data architecture:** Medallion, domain-oriented architecture, data mesh, data products, multi-cloud, cost management, disaster recovery and architecture trade-offs.

After those, we'll build a practical end-to-end pipeline using your existing SQL Server, Python, GCS, PySpark, Snowflake, dbt and Airflow learning environment.

**Our new rule:** complete the knowledge first, implement second, assess and prepare for interviews last.