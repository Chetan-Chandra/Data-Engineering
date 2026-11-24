import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

# 1. Create SparkSession
spark = (
    SparkSession.builder
    .appName("Lesson03_Basics")
    .master("local[*]")
    .getOrCreate()
)

# 2. Create sample data
data = [
    (101, "Amit", "Kolkata", 5000),
    (102, "Rahul", "Delhi", 7000),
    (103, "Priya", "Mumbai", 9000),
    (104, "Sneha", "Kolkata", 6000)
]

columns = [
    "customer_id",
    "customer_name",
    "city",
    "amount"
]

# 3. Create DataFrame
df = spark.createDataFrame(data, columns)

# 4. Display data
df.show()

# 5. Print schema
df.printSchema()

# 6. Count records
print("Total records:", df.count())

# 7. Stop Spark
spark.stop()