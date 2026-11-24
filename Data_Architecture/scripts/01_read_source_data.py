
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Start Spark
spark = (
    SparkSession.builder
    .appName("AdventureWorks_PySpark")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 2. Read source JSONL files
customers = spark.read.json("data/raw/customers.jsonl")

products = spark.read.json("data/raw/products.jsonl")

sales = spark.read.json("data/raw/internet_sales.jsonl")

# 3. Remove large image column if present
if "LargePhoto" in products.columns:
    products = products.drop("LargePhoto")

# 4. Display source information
print("\n========== CUSTOMERS ==========")
customers.printSchema()
customers.show(5, truncate=False)

print("\n========== PRODUCTS ==========")
products.printSchema()
products.show(5, truncate=False)

print("\n========== INTERNET SALES ==========")
sales.printSchema()
sales.show(5, truncate=False)

# 5. Record counts
print("\n========== SOURCE COUNTS ==========")
print("Customers:", customers.count())
print("Products:", products.count())
print("Internet Sales:", sales.count())

spark.stop()
