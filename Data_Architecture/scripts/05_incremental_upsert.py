from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("AdventureWorks_Incremental_Upsert")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 1. Read existing customer dimension
existing = spark.read.parquet("data/processed/dim_customer.parquet")

# 2. Discover incremental files
incremental_path = Path("data/raw/incremental")

files = [
    str(p.resolve())
    for p in sorted(incremental_path.glob("*.jsonl"))
]

if not files:
    raise ValueError("No incremental customer files found")

incremental_raw = spark.read.json(files)

# 3. Apply the same customer transformation as the full load
incremental = incremental_raw.select(
    F.col("CustomerKey").cast("int").alias("CustomerKey"),
    F.col("GeographyKey").cast("int").alias("GeographyKey"),
    F.col("Gender"),
    F.col("MaritalStatus"),
    F.col("EnglishEducation").alias("Education"),
    F.col("EnglishOccupation").alias("Occupation"),
    F.col("YearlyIncome").cast("decimal(18,2)").alias("YearlyIncome"),
    F.col("NumberCarsOwned").cast("int").alias("NumberCarsOwned"),
    F.col("TotalChildren").cast("int").alias("TotalChildren")
).dropDuplicates(["CustomerKey"])

# 4. Identify inserts and updates
existing_keys = existing.select("CustomerKey").distinct()

matched = incremental.join(
    existing_keys,
    on="CustomerKey",
    how="inner"
)

new_records = incremental.join(
    existing_keys,
    on="CustomerKey",
    how="left_anti"
)

print("\n========== INCREMENTAL SUMMARY ==========")
print("Existing customer count:", existing.count())
print("Incoming records:", incremental.count())
print("Records matching existing keys (updates):", matched.count())
print("New customer records (inserts):", new_records.count())

# 5. Give incremental records priority over existing records
existing_priority = existing.withColumn("_priority", F.lit(0))
incremental_priority = incremental.withColumn("_priority", F.lit(1))

combined = existing_priority.unionByName(incremental_priority)

window = (
    Window
    .partitionBy("CustomerKey")
    .orderBy(F.col("_priority").desc())
)

updated = (
    combined
    .withColumn("_row", F.row_number().over(window))
    .filter(F.col("_row") == 1)
    .drop("_priority", "_row")
)

# 6. Validate
print("\n========== UPSERT VALIDATION ==========")
print("Final customer count:", updated.count())
print(
    "Duplicate customer keys:",
    updated.groupBy("CustomerKey")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print("\n========== INCREMENTAL CUSTOMER RECORDS ==========")
updated.filter(
    F.col("CustomerKey").isin(29484, 29485, 29486, 29487)
).orderBy("CustomerKey").show(truncate=False)

# 7. Export to a new file; preserve the original full-load output
output = Path("data/processed/dim_customer_updated.parquet")

pandas_df = updated.toPandas()
pandas_df.to_parquet(output, index=False)

print("\nUpdated customer dimension written:", output)
print("Exported rows:", len(pandas_df))

spark.stop()