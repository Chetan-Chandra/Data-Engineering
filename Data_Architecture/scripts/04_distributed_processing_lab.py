from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import broadcast
from pyspark.storagelevel import StorageLevel
from pyspark import TaskContext

PROJECT = "electric-tesla-507710-k2"
BUCKET = "gs://advworks-dev-ingestion"

CUSTOMER_PATH = f"{BUCKET}/curated/adventureworks/dim_customer/"
PRODUCT_PATH = f"{BUCKET}/curated/adventureworks/dim_product/"
SALES_PATH = f"{BUCKET}/curated/adventureworks/fact_sales/"

OUTPUT = f"{BUCKET}/staging/dataproc/spark_lab"

spark = (
    SparkSession.builder
    .appName("Spark-Distributed-Processing-Lab")
    .getOrCreate()
)

sc = spark.sparkContext

print("SPARK VERSION:", spark.version)
print("APPLICATION ID:", sc.applicationId)

# --------------------------------------------------
# 1. LOAD DATA
# --------------------------------------------------

customers = spark.read.parquet(CUSTOMER_PATH)
products = spark.read.parquet(PRODUCT_PATH)
sales = spark.read.parquet(SALES_PATH)

print("CUSTOMER COUNT:", customers.count())
print("PRODUCT COUNT:", products.count())
print("SALES COUNT:", sales.count())

print("CUSTOMER PARTITIONS:", customers.rdd.getNumPartitions())
print("PRODUCT PARTITIONS:", products.rdd.getNumPartitions())
print("SALES PARTITIONS:", sales.rdd.getNumPartitions())

# --------------------------------------------------
# 2. LAZY EVALUATION AND DAG
# --------------------------------------------------

print("\n========== LAZY EVALUATION ==========")

lazy_df = (
    sales
    .filter(F.col("SalesAmount") > 100)
    .select("CustomerKey", "ProductKey", "SalesAmount")
    .groupBy("CustomerKey")
    .agg(F.sum("SalesAmount").alias("customer_sales"))
)

# No action has run yet.
print("TRANSFORMATION PLAN:")
lazy_df.explain("formatted")

# Action triggers execution.
print("ACTION RESULT:")
lazy_df.show(10)

# --------------------------------------------------
# 3. NARROW AND WIDE TRANSFORMATIONS
# --------------------------------------------------

print("\n========== NARROW TRANSFORMATION ==========")

narrow_df = (
    sales
    .filter(F.col("SalesAmount") > 100)
    .select("CustomerKey", "SalesAmount")
)

narrow_df.explain("formatted")
print("NARROW COUNT:", narrow_df.count())

print("\n========== WIDE TRANSFORMATION ==========")

wide_df = (
    sales
    .groupBy("CustomerKey")
    .agg(F.sum("SalesAmount").alias("total_sales"))
)

wide_df.explain("formatted")
print("WIDE COUNT:", wide_df.count())

# --------------------------------------------------
# 4. REPARTITION AND COALESCE
# --------------------------------------------------

print("\n========== PARTITION EXPERIMENTS ==========")

print("ORIGINAL:", sales.rdd.getNumPartitions())

repartitioned = sales.repartition(16, "CustomerKey")
print("REPARTITIONED:", repartitioned.rdd.getNumPartitions())

coalesced = repartitioned.coalesce(4)
print("COALESCED:", coalesced.rdd.getNumPartitions())

repartitioned.explain("formatted")
coalesced.explain("formatted")

# --------------------------------------------------
# 5. SHUFFLE EXPERIMENT
# --------------------------------------------------

print("\n========== SHUFFLE EXPERIMENT ==========")

shuffle_df = (
    sales
    .repartition(16, "CustomerKey")
    .groupBy("CustomerKey")
    .agg(
        F.sum("SalesAmount").alias("total_sales"),
        F.count("*").alias("number_of_orders")
    )
)

shuffle_df.explain("formatted")
shuffle_df.show(10)

# --------------------------------------------------
# 6. BROADCAST HASH JOIN
# --------------------------------------------------

print("\n========== BROADCAST JOIN ==========")

broadcast_join = (
    sales.alias("s")
    .join(
        broadcast(products).alias("p"),
        F.col("s.ProductKey") == F.col("p.ProductKey"),
        "inner"
    )
    .select(
        F.col("s.ProductKey"),
        F.col("p.EnglishProductName"),
        F.col("s.SalesAmount")
    )
)

broadcast_join.explain("formatted")
print("BROADCAST JOIN COUNT:", broadcast_join.count())

# --------------------------------------------------
# 7. SORT-MERGE JOIN
# --------------------------------------------------

print("\n========== SORT MERGE JOIN ==========")

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")

sort_merge_join = (
    sales.alias("s")
    .hint("merge")
    .join(
        products.alias("p").hint("merge"),
        F.col("s.ProductKey") == F.col("p.ProductKey"),
        "inner"
    )
)

sort_merge_join.explain("formatted")
print("SORT MERGE COUNT:", sort_merge_join.count())

# Restore automatic broadcast selection.
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10MB")

# --------------------------------------------------
# 8. DATA SKEW
# --------------------------------------------------

print("\n========== DATA SKEW EXPERIMENT ==========")

skew_data = (
    spark.range(0, 500000, 1, 16)
    .withColumn(
        "join_key",
        F.when(F.col("id") < 475000, F.lit(0))
         .otherwise((F.col("id") % 100) + 1)
    )
)

skew_data.groupBy("join_key").count().orderBy(
    F.desc("count")
).show(10)

# Observe the skewed aggregation plan.
skew_data.groupBy("join_key").count().explain("formatted")

# --------------------------------------------------
# 9. SKEW MITIGATION USING SALTING
# --------------------------------------------------

print("\n========== SKEW SALTING ==========")

salt_count = 16

skew_fact = (
    skew_data
    .withColumn(
        "salt",
        F.when(
            F.col("join_key") == 0,
            F.pmod(F.col("id"), F.lit(salt_count))
        ).otherwise(F.lit(0))
    )
)

skew_dimension = spark.range(0, 101).withColumnRenamed(
    "id", "join_key"
)

salt_values = F.array(*[
    F.lit(i) for i in range(salt_count)
])

salted_dimension = (
    skew_dimension
    .withColumn(
        "salt",
        F.explode(
            F.when(
                F.col("join_key") == 0,
                salt_values
            ).otherwise(F.array(F.lit(0)))
        )
    )
)

salted_join = skew_fact.join(
    salted_dimension,
    ["join_key", "salt"],
    "inner"
)

print("SALTED JOIN COUNT:", salted_join.count())
salted_join.explain("formatted")

# --------------------------------------------------
# 10. ADAPTIVE QUERY EXECUTION
# --------------------------------------------------

print("\n========== AQE DISABLED ==========")

spark.conf.set("spark.sql.adaptive.enabled", "false")

aqe_test = (
    skew_data
    .groupBy("join_key")
    .count()
)

aqe_test.count()
aqe_test.explain("formatted")

print("\n========== AQE ENABLED ==========")

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

aqe_test = (
    skew_data
    .groupBy("join_key")
    .count()
)

aqe_test.count()
aqe_test.explain("formatted")

# --------------------------------------------------
# 11. CACHING AND PERSISTENCE
# --------------------------------------------------

print("\n========== CACHE EXPERIMENT ==========")

cache_df = (
    sales
    .filter(F.col("SalesAmount") > 100)
    .select("CustomerKey", "ProductKey", "SalesAmount")
)

cache_df.persist(StorageLevel.MEMORY_AND_DISK)

print("FIRST ACTION:", cache_df.count())
print("SECOND ACTION:", cache_df.count())

cache_df.explain("formatted")

cache_df.unpersist()

# --------------------------------------------------
# 12. CATALYST AND PHYSICAL PLAN
# --------------------------------------------------

print("\n========== CATALYST PLAN ==========")

plan_df = (
    sales
    .filter(F.col("SalesAmount") > 100)
    .select("CustomerKey", "ProductKey", "SalesAmount")
    .groupBy("ProductKey")
    .agg(F.sum("SalesAmount").alias("total_sales"))
)

plan_df.explain("extended")
plan_df.explain("formatted")

# --------------------------------------------------
# 13. PARQUET AND PARTITION PRUNING
# --------------------------------------------------

print("\n========== PARQUET OPTIMISATION ==========")

year_df = sales.withColumn(
    "order_year",
    F.year("OrderDate")
)

year_output = f"{OUTPUT}/partitioned_sales"

(
    year_df
    .write
    .mode("overwrite")
    .option("maxRecordsPerFile", 10000)
    .partitionBy("order_year")
    .parquet(year_output)
)

partitioned_sales = spark.read.parquet(year_output)

filtered_sales = partitioned_sales.filter(
    F.col("order_year") == 2013
)

filtered_sales.explain("formatted")
print("FILTERED SALES:", filtered_sales.count())

# --------------------------------------------------
# 14. MEMORY AND SPILL OBSERVATION
# --------------------------------------------------

print("\n========== MEMORY / SPILL TEST ==========")

memory_test = (
    spark.range(0, 2000000, 1, 32)
    .withColumn("group_key", F.col("id") % 1000)
    .repartition(32, "group_key")
    .groupBy("group_key")
    .agg(F.sum("id").alias("total"))
    .sort("total")
)

memory_test.count()

# Inspect task metrics in the Spark UI / Dataproc monitoring.
# Look for Memory Bytes Spilled and Disk Bytes Spilled.

# --------------------------------------------------
# 15. FAULT TOLERANCE AND TASK RETRIES
# --------------------------------------------------

print("\n========== TASK RETRY TEST ==========")
print("SKIPPED: Local execution backend does not support this retry experiment.")
print("Task retry behaviour requires a suitable cluster execution backend.")

# --------------------------------------------------
# 16. FINAL CLEANUP
# --------------------------------------------------

spark.catalog.clearCache()

print("\nALL DISTRIBUTED PROCESSING EXPERIMENTS COMPLETED")

spark.stop()