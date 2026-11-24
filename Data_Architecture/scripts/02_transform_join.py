
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pathlib import Path
import re

spark = (
    SparkSession.builder
    .appName("AdventureWorks_Transform_Join")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# =====================================================
# 1. READ SOURCE DATA
# =====================================================

raw_path = Path("data/raw")

# Select only numbered full-load batches.
# Exclude incremental files from the initial full load.
customer_files = sorted([
    str(p.resolve())
    for p in raw_path.glob("DimCustomer_batch_*.jsonl")
    if re.fullmatch(r"DimCustomer_batch_\d{3}\.jsonl", p.name)
])

sales_files = sorted([
    str(p.resolve())
    for p in raw_path.glob("FactInternetSales_batch_*.jsonl")
    if re.fullmatch(r"FactInternetSales_batch_\d{3}\.jsonl", p.name)
])

product_file = str((raw_path / "products.jsonl").resolve())

print("Customer batch files:", len(customer_files))
print("Sales batch files:", len(sales_files))

if len(customer_files) != 19:
    raise ValueError(f"Expected 19 customer batches, found {len(customer_files)}")

if len(sales_files) != 61:
    raise ValueError(f"Expected 61 sales batches, found {len(sales_files)}")

customers = spark.read.json(customer_files)
products = spark.read.json(product_file)
sales = spark.read.json(sales_files)

# =====================================================
# 2. TRANSFORM CUSTOMERS
# Exclude personal contact and address information.
# =====================================================

dim_customer = (
    customers
    .select(
        F.col("CustomerKey").cast("long"),
        F.col("GeographyKey").cast("long"),
        F.col("Gender"),
        F.col("MaritalStatus"),
        F.col("EnglishEducation").alias("Education"),
        F.col("EnglishOccupation").alias("Occupation"),
        F.col("YearlyIncome").cast("decimal(18,2)"),
        F.col("NumberCarsOwned").cast("integer"),
        F.col("TotalChildren").cast("integer")
    )
    .dropDuplicates(["CustomerKey"])
)

# =====================================================
# 3. TRANSFORM PRODUCTS
# =====================================================

dim_product = (
    products
    .select(
        F.col("ProductKey").cast("long"),
        F.col("ProductAlternateKey"),
        F.col("EnglishProductName").alias("ProductName"),
        F.col("ProductSubcategoryKey").cast("long"),
        F.col("Color"),
        F.col("StandardCost").cast("decimal(18,2)"),
        F.col("ListPrice").cast("decimal(18,2)"),
        F.col("Size"),
        F.col("ProductLine"),
        F.col("Class"),
        F.col("Status"),
        F.to_date("StartDate").alias("StartDate"),
        F.to_date("EndDate").alias("EndDate")
    )
    .dropDuplicates(["ProductKey"])
)

# =====================================================
# 4. TRANSFORM INTERNET SALES
# Grain: one record per sales order line.
# =====================================================

fact_sales = (
    sales
    .select(
        F.col("SalesOrderNumber"),
        F.col("SalesOrderLineNumber").cast("integer"),
        F.col("CustomerKey").cast("long"),
        F.col("ProductKey").cast("long"),
        F.col("OrderDateKey").cast("integer"),
        F.col("OrderDate").cast("timestamp").alias("OrderDate"),
        F.col("DueDate").cast("timestamp").alias("DueDate"),
        F.col("ShipDate").cast("timestamp").alias("ShipDate"),
        F.col("OrderQuantity").cast("integer"),
        F.col("UnitPrice").cast("decimal(18,2)"),
        F.col("DiscountAmount").cast("decimal(18,2)"),
        F.col("SalesAmount").cast("decimal(18,2)"),
        F.col("TotalProductCost").cast("decimal(18,2)"),
        F.col("TaxAmt").cast("decimal(18,2)"),
        F.col("Freight").cast("decimal(18,2)")
    )
)

# =====================================================
# 5. DATA QUALITY CHECKS
# =====================================================

print("\n========== DATA QUALITY ==========")

print("Duplicate customer keys:",
      customers.count() - dim_customer.count())

print("Duplicate product keys:",
      products.count() - dim_product.count())

duplicate_sales_keys = (
    fact_sales
    .groupBy("SalesOrderNumber", "SalesOrderLineNumber")
    .count()
    .filter(F.col("count") > 1)
)

print("Duplicate sales order-line keys:", duplicate_sales_keys.count())

print("Null customer keys in sales:",
      fact_sales.filter(F.col("CustomerKey").isNull()).count())

print("Null product keys in sales:",
      fact_sales.filter(F.col("ProductKey").isNull()).count())

# =====================================================
# 6. JOIN FACT SALES WITH DIMENSIONS
# =====================================================

s = fact_sales.alias("s")
c = dim_customer.alias("c")
p = dim_product.alias("p")

joined_sales = (
    s
    .join(c, F.col("s.CustomerKey") == F.col("c.CustomerKey"), "left")
    .join(p, F.col("s.ProductKey") == F.col("p.ProductKey"), "left")
    .select(
        F.col("s.SalesOrderNumber"),
        F.col("s.SalesOrderLineNumber"),
        F.col("s.OrderDate"),
        F.col("s.CustomerKey"),
        F.col("c.Gender"),
        F.col("c.Education"),
        F.col("c.Occupation"),
        F.col("c.YearlyIncome"),
        F.col("s.ProductKey"),
        F.col("p.ProductName"),
        F.col("p.Color"),
        F.col("p.ProductLine"),
        F.col("s.OrderQuantity"),
        F.col("s.UnitPrice"),
        F.col("s.DiscountAmount"),
        F.col("s.SalesAmount"),
        F.col("s.TotalProductCost"),
        F.col("s.TaxAmt"),
        F.col("s.Freight")
    )
)

# =====================================================
# 7. CHECK UNMATCHED DIMENSION RECORDS
# =====================================================

print("\n========== JOIN VALIDATION ==========")

unmatched_customers = (
    fact_sales.alias("s")
    .join(
        dim_customer.alias("c"),
        F.col("s.CustomerKey") == F.col("c.CustomerKey"),
        "left_anti"
    )
)

unmatched_products = (
    fact_sales.alias("s")
    .join(
        dim_product.alias("p"),
        F.col("s.ProductKey") == F.col("p.ProductKey"),
        "left_anti"
    )
)

print("Sales with unmatched customers:", unmatched_customers.count())
print("Sales with unmatched products:", unmatched_products.count())

print("Original sales records:", fact_sales.count())
print("Joined sales records:", joined_sales.count())

# =====================================================
# 8. AGGREGATE SALES BY PRODUCT
# =====================================================

product_sales = (
    joined_sales
    .groupBy("ProductKey", "ProductName")
    .agg(
        F.sum("OrderQuantity").alias("TotalQuantity"),
        F.sum("SalesAmount").alias("TotalSales"),
        F.sum("TotalProductCost").alias("TotalCost"),
        F.countDistinct("SalesOrderNumber").alias("TotalOrders")
    )
    .withColumn(
        "GrossMargin",
        F.col("TotalSales") - F.col("TotalCost")
    )
)

# =====================================================
# 9. WRITE LOCAL PARQUET OUTPUT USING PYARROW
# Windows local-lab workaround for missing winutils.exe
# =====================================================



output_path = Path("data/processed")
output_path.mkdir(parents=True, exist_ok=True)

datasets = {
    "dim_customer": dim_customer,
    "dim_product": dim_product,
    "fact_sales": fact_sales,
    "joined_sales": joined_sales,
    "product_sales": product_sales
}

for name, dataframe in datasets.items():
    pandas_df = dataframe.toPandas()
    pandas_df.to_parquet(
        output_path / f"{name}.parquet",
        index=False
    )
    print(f"Written: {name}.parquet | Rows: {len(pandas_df)}")

# =====================================================
# 10. DISPLAY RESULTS
# =====================================================

print("\n========== JOINED SALES SAMPLE ==========")
joined_sales.show(10, truncate=False)

print("\n========== PRODUCT SALES SUMMARY ==========")
product_sales.orderBy(
    F.col("TotalSales").desc()
).show(20, truncate=False)

print("\n========== OUTPUT COUNTS ==========")
print("dim_customer:", dim_customer.count())
print("dim_product:", dim_product.count())
print("fact_sales:", fact_sales.count())
print("joined_sales:", joined_sales.count())
print("product_sales:", product_sales.count())

print("\nPipeline execution completed.")

spark.stop()
