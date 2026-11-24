
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, to_timestamp,
    when, sum as spark_sum
)
from pyspark.sql.types import StructType, StructField, StringType
from functools import reduce

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

OUTPUT_PATH = (
    f"gs://{BUCKET}/staging/dataproc/data_quality"
)

spark = (
    SparkSession.builder
    .appName("AdventureWorks-Data-Quality")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

metrics = []

def record(dataset, check, value):
    metrics.append((dataset, check, str(value)))

def duplicate_groups(df, keys):
    return (
        df.groupBy(*keys)
        .count()
        .filter(col("count") > 1)
        .count()
    )

def null_key_rows(df, keys):
    condition = reduce(
        lambda a, b: a | b,
        [col(k).isNull() for k in keys]
    )
    return df.filter(condition).count()

try:
    customers = spark.read.json(CUSTOMER_PATH)
    products = spark.read.json(PRODUCT_PATH)
    sales = spark.read.json(SALES_PATH)

    # 1. Dataset counts
    record("customers", "row_count", customers.count())
    record("products", "row_count", products.count())
    record("sales", "row_count", sales.count())

    # 2. Primary/business key checks
    record(
        "customers",
        "duplicate_customer_key_groups",
        duplicate_groups(customers, ["CustomerKey"])
    )

    record(
        "customers",
        "null_customer_key_rows",
        null_key_rows(customers, ["CustomerKey"])
    )

    record(
        "products",
        "duplicate_product_key_groups",
        duplicate_groups(products, ["ProductKey"])
    )

    record(
        "products",
        "null_product_key_rows",
        null_key_rows(products, ["ProductKey"])
    )

    sales_keys = [
        "SalesOrderNumber",
        "SalesOrderLineNumber"
    ]

    record(
        "sales",
        "duplicate_sales_line_key_groups",
        duplicate_groups(sales, sales_keys)
    )

    record(
        "sales",
        "null_sales_line_key_rows",
        null_key_rows(sales, sales_keys)
    )

    # 3. Referential integrity
    customer_keys = customers.select("CustomerKey").distinct()
    product_keys = products.select("ProductKey").distinct()

    orphan_customers = (
        sales.select("CustomerKey")
        .distinct()
        .join(
            customer_keys,
            "CustomerKey",
            "left_anti"
        )
    )

    orphan_products = (
        sales.select("ProductKey")
        .distinct()
        .join(
            product_keys,
            "ProductKey",
            "left_anti"
        )
    )

    record(
        "sales",
        "orphan_customer_key_count",
        orphan_customers.count()
    )

    record(
        "sales",
        "orphan_product_key_count",
        orphan_products.count()
    )

    # 4. Date validity
    date_checks = {
        "customers": [
            (customers, "BirthDate"),
            (customers, "DateFirstPurchase")
        ],
        "products": [
            (products, "StartDate"),
            (products, "EndDate")
        ],
        "sales": [
            (sales, "OrderDate"),
            (sales, "DueDate"),
            (sales, "ShipDate")
        ]
    }

    for dataset, checks in date_checks.items():
        for df, field in checks:
            invalid = df.filter(
                col(field).isNotNull()
                & to_timestamp(col(field)).isNull()
            ).count()

            record(
                dataset,
                f"invalid_{field}_count",
                invalid
            )

    # 5. Numeric validity
    record(
        "sales",
        "non_positive_order_quantity_rows",
        sales.filter(col("OrderQuantity") <= 0).count()
    )

    record(
        "sales",
        "negative_sales_amount_rows",
        sales.filter(col("SalesAmount") < 0).count()
    )

    # 6. Persist all metrics
    schema = StructType([
        StructField("dataset", StringType(), False),
        StructField("check_name", StringType(), False),
        StructField("metric_value", StringType(), False)
    ])

    results = spark.createDataFrame(metrics, schema)

    results.orderBy(
        "dataset", "check_name"
    ).show(100, truncate=False)

    results.coalesce(1).write.mode(
        "overwrite"
    ).json(OUTPUT_PATH)

    print("DATA QUALITY RESULTS WRITTEN TO:", OUTPUT_PATH)

finally:
    spark.stop()
