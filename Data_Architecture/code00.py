import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Create Spark session
spark = (
    SparkSession.builder
    .appName("Lesson03_Transformations")
    .master("local[*]")
    .getOrCreate()
)

# 2. Create source DataFrame
data = [
    (101, "Amit", "Kolkata", 5000),
    (102, "Rahul", "Delhi", 7000),
    (103, "Priya", "Mumbai", 9000),
    (104, "Sneha", "Kolkata", 6000)
]

columns = ["customer_id", "customer_name", "city", "amount"]

df = spark.createDataFrame(data, columns)

# 3. Transformations
kolkata_customers = df.filter(F.col("city") == "Kolkata")

high_value_customers = df.filter(F.col("amount") > 6000)

city_sales = (
    df.groupBy("city")
      .agg(F.sum("amount").alias("total_amount"))
)

# 4. Actions
print("Kolkata customers:")
kolkata_customers.show()

print("High value customers:")
high_value_customers.show()

print("Sales by city:")
city_sales.show()

print("\n===== EXECUTION PLAN: FILTER =====")
kolkata_customers.explain()

print("\n===== EXECUTION PLAN: GROUP BY =====")
city_sales.explain()

# 5. Stop Spark
spark.stop()