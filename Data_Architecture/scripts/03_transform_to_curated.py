from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DecimalType

PROJECT = "electric-tesla-507710-k2"
BUCKET = "gs://advworks-dev-ingestion"

CUSTOMER_SOURCE = f"{BUCKET}/dim_customer/DimCustomer_batch_0*.jsonl"
PRODUCT_SOURCE = f"{BUCKET}/dim_product/DimProduct_batch_001.jsonl"
SALES_SOURCE = f"{BUCKET}/fact_internet_sales/FactInternetSales_batch_0*.jsonl"

OUTPUT_BASE = f"{BUCKET}/curated/adventureworks"

spark = (
    SparkSession.builder
    .appName("AdventureWorks-Curated-Transformation")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


def parse_date(df, column_name):
    """Convert date or ISO timestamp strings to Spark DateType."""
    return df.withColumn(
        column_name,
        F.coalesce(
            F.to_date(F.col(column_name)),
            F.to_date(F.to_timestamp(F.col(column_name)))
        )
    )


# --------------------------------------------------
# 1. CUSTOMER DIMENSION
# --------------------------------------------------

customers = spark.read.json(CUSTOMER_SOURCE)

customers = parse_date(customers, "BirthDate")
customers = parse_date(customers, "DateFirstPurchase")

dim_customer = customers.withColumn(
    "curated_at", F.current_timestamp()
)

dim_customer.write.mode("overwrite").parquet(
    f"{OUTPUT_BASE}/dim_customer"
)


# --------------------------------------------------
# 2. PRODUCT DIMENSION
# --------------------------------------------------

products = spark.read.json(PRODUCT_SOURCE)

# LargePhoto contains Base64 image data. Keep it in
# the source layer, not in the analytical dimension.
products = products.drop("LargePhoto")

products = parse_date(products, "StartDate")
products = parse_date(products, "EndDate")

dim_product = products.withColumn(
    "curated_at", F.current_timestamp()
)

dim_product.write.mode("overwrite").parquet(
    f"{OUTPUT_BASE}/dim_product"
)


# --------------------------------------------------
# 3. SALES FACT
# Grain: one row per sales order line
# Business key: SalesOrderNumber + SalesOrderLineNumber
# --------------------------------------------------

sales = spark.read.json(SALES_SOURCE)

# These two source columns were entirely null.
sales = sales.drop("CarrierTrackingNumber", "CustomerPONumber")

for column_name in ["OrderDate", "DueDate", "ShipDate"]:
    sales = parse_date(sales, column_name)

# Use decimal types for financial calculations.
money_columns = [
    "DiscountAmount",
    "ExtendedAmount",
    "Freight",
    "ProductStandardCost",
    "SalesAmount",
    "TaxAmt",
    "TotalProductCost",
    "UnitPrice"
]

for column_name in money_columns:
    sales = sales.withColumn(
        column_name,
        F.col(column_name).cast(DecimalType(18, 2))
    )

sales = sales.withColumn(
    "UnitPriceDiscountPct",
    F.col("UnitPriceDiscountPct").cast(DecimalType(9, 4))
)

fact_sales = (
    sales
    .withColumn(
        "sales_line_key",
        F.concat_ws(
            "|",
            F.col("SalesOrderNumber"),
            F.col("SalesOrderLineNumber").cast("string")
        )
    )
    .withColumn("curated_at", F.current_timestamp())
)

fact_sales.write.mode("overwrite").parquet(
    f"{OUTPUT_BASE}/fact_sales"
)


# --------------------------------------------------
# 4. PRODUCT SALES AGGREGATE
# Grain: one row per product with sales
# --------------------------------------------------

product_sales_agg = (
    fact_sales
    .groupBy("ProductKey")
    .agg(
        F.count("*").alias("total_order_lines"),
        F.sum("OrderQuantity").alias("total_quantity_sold"),
        F.sum("SalesAmount").alias("total_sales_amount"),
        F.sum("TotalProductCost").alias("total_product_cost"),
        F.avg("UnitPrice").alias("average_unit_price"),
        F.min("OrderDate").alias("first_sale_date"),
        F.max("OrderDate").alias("last_sale_date")
    )
)

product_sales = (
    product_sales_agg
    .join(
        dim_product.select("ProductKey", "EnglishProductName"),
        on="ProductKey",
        how="inner"
    )
    .withColumn("curated_at", F.current_timestamp())
)

product_sales.write.mode("overwrite").parquet(
    f"{OUTPUT_BASE}/product_sales"
)


# --------------------------------------------------
# 5. EXECUTION SUMMARY
# --------------------------------------------------

print("\n========== CURATED TRANSFORMATION SUMMARY ==========")

print(f"dim_customer rows : {dim_customer.count()}")
print(f"dim_product rows  : {dim_product.count()}")
print(f"fact_sales rows   : {fact_sales.count()}")
print(f"product_sales rows: {product_sales.count()}")

print("\nCURATED OUTPUT LOCATIONS:")
print(f"{OUTPUT_BASE}/dim_customer")
print(f"{OUTPUT_BASE}/dim_product")
print(f"{OUTPUT_BASE}/fact_sales")
print(f"{OUTPUT_BASE}/product_sales")

print("\nCURATED TRANSFORMATION COMPLETED SUCCESSFULLY.")

spark.stop()