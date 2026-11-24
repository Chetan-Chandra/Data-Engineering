from pyspark.sql import SparkSession, types as T
from datetime import datetime

spark = (
    SparkSession.builder
    .appName("Create-Structured-Streaming-Input")
    .getOrCreate()
)

schema = T.StructType([
    T.StructField("customer_id", T.IntegerType(), False),
    T.StructField("event_type", T.StringType(), False),
    T.StructField("amount", T.DoubleType(), False),
    T.StructField("event_ts", T.TimestampType(), False)
])

events = [
    (101, "purchase", 250.0, datetime(2026, 11, 24, 10, 0, 0)),
    (102, "purchase", 400.0, datetime(2026, 11, 24, 10, 1, 0)),
    (101, "refund", 50.0, datetime(2026, 11, 24, 10, 2, 0)),
    (103, "purchase", 600.0, datetime(2026, 11, 24, 10, 3, 0)),
    (102, "purchase", 150.0, datetime(2026, 11, 24, 10, 4, 0)),
    (104, "purchase", 300.0, datetime(2026, 11, 24, 10, 5, 0)),
    (101, "purchase", 100.0, datetime(2026, 11, 24, 10, 6, 0)),
    (105, "purchase", 800.0, datetime(2026, 11, 24, 10, 7, 0))
]

df = spark.createDataFrame(events, schema)

input_path = (
    "gs://advworks-dev-ingestion/"
    "staging/dataproc/stream_input"
)

(
    df.write
    .mode("overwrite")
    .json(input_path)
)

print("STREAMING INPUT CREATED:", df.count())

spark.stop()