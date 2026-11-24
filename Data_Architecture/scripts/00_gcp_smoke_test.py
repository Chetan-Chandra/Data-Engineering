
from pyspark.sql import SparkSession

BUCKET = "advworks-dev-ingestion"

CUSTOMER_PATH = (
    f"gs://{BUCKET}/dim_customer/DimCustomer_batch_0*.jsonl"
)
PRODUCT_PATH = (
    f"gs://{BUCKET}/dim_product/DimProduct_batch_001.jsonl"
)
SALES_PATH = (
    f"gs://{BUCKET}/fact_internet_sales/FactInternetSales_batch_0*.jsonl"
)

OUTPUT_BASE = f"gs://{BUCKET}/staging/dataproc/smoke_test"

spark = (
    SparkSession.builder
    .appName("AdventureWorks-GCP-Full-Smoke-Test")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

try:
    print("Spark version:", spark.version)

    # Read all three source datasets
    customers = spark.read.json(CUSTOMER_PATH)
    products = spark.read.json(PRODUCT_PATH)
    sales = spark.read.json(SALES_PATH)

    # Validate schemas
    print("\nCUSTOMER SCHEMA")
    customers.printSchema()

    print("\nPRODUCT SCHEMA")
    products.printSchema()

    print("\nSALES SCHEMA")
    sales.printSchema()

    # Count records
    customer_count = customers.count()
    product_count = products.count()
    sales_count = sales.count()

    print("\nCUSTOMERS:", customer_count)
    print("PRODUCTS:", product_count)
    print("SALES:", sales_count)

    # Validate expected full-load counts
    expected = {
        "customers": 18484,
        "products": 606,
        "sales": 60398
    }

    actual = {
        "customers": customer_count,
        "products": product_count,
        "sales": sales_count
    }

    for dataset, expected_count in expected.items():
        if actual[dataset] != expected_count:
            raise ValueError(
                f"{dataset}: expected {expected_count}, "
                f"received {actual[dataset]}"
            )

    # Write Parquet outputs
    customers.write.mode("overwrite").parquet(
        f"{OUTPUT_BASE}/customers"
    )

    products.write.mode("overwrite").parquet(
        f"{OUTPUT_BASE}/products"
    )

    sales.write.mode("overwrite").parquet(
        f"{OUTPUT_BASE}/sales"
    )

    print("\nALL THREE DATASETS WRITTEN SUCCESSFULLY")

finally:
    spark.stop()
