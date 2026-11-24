
from pyspark.sql import SparkSession

BUCKET = "advworks-dev-ingestion"

sources = {
    "customers": f"gs://{BUCKET}/dim_customer/DimCustomer_batch_0*.jsonl",
    "products": f"gs://{BUCKET}/dim_product/DimProduct_batch_001.jsonl",
    "sales": f"gs://{BUCKET}/fact_internet_sales/FactInternetSales_batch_0*.jsonl",
}

spark = (
    SparkSession.builder
    .appName("AdventureWorks-Schema-Inspection")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

try:
    for name, path in sources.items():
        print(f"\n{'=' * 60}")
        print(f"DATASET: {name.upper()}")
        print(f"SOURCE: {path}")
        print("=" * 60)

        df = spark.read.json(path)

        print("\nSCHEMA:")
        df.printSchema()

        print("\nRECORD COUNT:")
        print(df.count())

        print("\nSAMPLE RECORDS:")
        df.show(3, truncate=False)

        print("\nNULL COUNTS:")
        from pyspark.sql.functions import col, sum as spark_sum, when

        null_counts = df.select([
            spark_sum(
                when(col(c).isNull(), 1).otherwise(0)
            ).alias(c)
            for c in df.columns
        ])

        null_counts.show(truncate=False)

finally:
    spark.stop()
