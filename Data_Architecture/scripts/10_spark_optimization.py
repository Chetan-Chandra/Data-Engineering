
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast, udf
from pyspark.sql.types import StringType
import time

spark = (
    SparkSession.builder
    .appName("Spark_Optimization_Lab")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

sales = spark.read.parquet("data/processed/fact_sales.parquet")
products = spark.read.parquet("data/processed/dim_product.parquet")

print("\n========== DATASET ==========")
print("Sales rows:", sales.count())
print("Product rows:", products.count())
print("Initial partitions:", sales.rdd.getNumPartitions())

# 1. Repartition
repartitioned = sales.repartition(4)
print("After repartition(4):", repartitioned.rdd.getNumPartitions())

# 2. Coalesce
coalesced = repartitioned.coalesce(2)
print("After coalesce(2):", coalesced.rdd.getNumPartitions())

# 3. Cache
start = time.time()
sales.cache()
sales.count()
print("Cache materialization seconds:", round(time.time() - start, 3))

start = time.time()
sales.groupBy("ProductKey").agg(
    F.sum("SalesAmount").alias("Revenue")
).count()
print("Cached aggregation seconds:", round(time.time() - start, 3))

sales.unpersist()

# 4. Regular join
regular_join = sales.join(products, "ProductKey", "inner")

print("\nRegular join execution plan:")
regular_join.explain("formatted")

# 5. Broadcast join
broadcast_join = sales.join(
    broadcast(products),
    "ProductKey",
    "inner"
)

print("\nBroadcast join execution plan:")
broadcast_join.explain("formatted")

print("Regular join rows:", regular_join.count())
print("Broadcast join rows:", broadcast_join.count())

# 6. Built-in function versus Python UDF
@udf(returnType=StringType())
def classify_sales(value):
    if value is None:
        return "Unknown"
    return "High" if value >= 1000 else "Normal"

builtin_result = sales.withColumn(
    "SalesCategory",
    F.when(F.col("SalesAmount") >= 1000, "High")
    .otherwise("Normal")
)

udf_result = sales.withColumn(
    "SalesCategory",
    classify_sales(F.col("SalesAmount"))
)

print("Built-in expression sample:")
builtin_result.select("SalesAmount", "SalesCategory").show(5)

print("Python UDF sample:")
udf_result.select("SalesAmount", "SalesCategory").show(5)

# 7. Shuffle and aggregation
print("\nAggregation execution plan:")
sales.groupBy("ProductKey").agg(
    F.sum("SalesAmount").alias("Revenue")
).explain("formatted")

# 8. Partition distribution
print("\nPartition distribution:")
print(
    sales.rdd
    .mapPartitions(lambda rows: [sum(1 for _ in rows)])
    .collect()
)

print("\nOptimization experiments completed.")

spark.stop()
