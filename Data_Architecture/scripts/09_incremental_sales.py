
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pathlib import Path

spark = (
    SparkSession.builder
    .appName("Incremental_Sales_Upsert")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

existing = spark.read.parquet("data/processed/fact_sales.parquet")

# Simulate replay of 100 existing rows
incoming = existing.orderBy(
    F.col("OrderDateKey").desc()
).limit(100)

keys = ["SalesOrderNumber", "SalesOrderLineNumber"]

new_rows = incoming.join(
    existing.select(*keys),
    on=keys,
    how="left_anti"
)

print("\n========== INCREMENTAL SALES ==========")
print("Existing rows:", existing.count())
print("Incoming replay rows:", incoming.count())
print("New business keys:", new_rows.count())

combined = existing.unionByName(incoming)

window = (
    Window.partitionBy(*keys)
    .orderBy(F.col("OrderDateKey").desc())
)

result = (
    combined
    .withColumn("_rn", F.row_number().over(window))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

duplicate_count = (
    result.groupBy(*keys)
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print("Final row count:", result.count())
print("Duplicate business keys:", duplicate_count)

output = Path("data/processed/fact_sales_incremental.parquet")
result.toPandas().to_parquet(output, index=False)

print("Incremental sales output written:", output)

spark.stop()
