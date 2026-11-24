from pyspark.sql import SparkSession, functions as F, types as T

spark = (
    SparkSession.builder
    .appName("Structured-Streaming-GCS")
    .getOrCreate()
)

input_path = (
    "gs://advworks-dev-ingestion/"
    "staging/dataproc/stream_input"
)

output_path = (
    "gs://advworks-dev-ingestion/"
    "staging/dataproc/stream_output"
)

checkpoint_path = (
    "gs://advworks-dev-ingestion/"
    "staging/dataproc/stream_checkpoint"
)

schema = T.StructType([
    T.StructField("customer_id", T.IntegerType(), False),
    T.StructField("event_type", T.StringType(), False),
    T.StructField("amount", T.DoubleType(), False),
    T.StructField("event_ts", T.TimestampType(), False)
])

# --------------------------------------------------
# 1. READ STREAM
# --------------------------------------------------

events = (
    spark.readStream
    .schema(schema)
    .option("maxFilesPerTrigger", 2)
    .json(input_path)
)

# --------------------------------------------------
# 2. EVENT-TIME PROCESSING
# --------------------------------------------------

events_with_watermark = events.withWatermark(
    "event_ts",
    "10 minutes"
)

# --------------------------------------------------
# 3. STATEFUL WINDOW AGGREGATION
# --------------------------------------------------

aggregated = (
    events_with_watermark
    .groupBy(
        F.window("event_ts", "5 minutes"),
        F.col("customer_id")
    )
    .agg(
        F.count("*").alias("event_count"),
        F.sum(
            F.when(
                F.col("event_type") == "purchase",
                F.col("amount")
            ).otherwise(0.0)
        ).alias("purchase_amount")
    )
)

# --------------------------------------------------
# 4. WRITE STREAM
# --------------------------------------------------

query = (
    aggregated.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", output_path)
    .option("checkpointLocation", checkpoint_path)
    .trigger(once=True)
    .start()
)

query.awaitTermination()

print("STREAMING QUERY COMPLETED")
print("QUERY ID:", query.id)
print("RUN ID:", query.runId)

spark.stop()