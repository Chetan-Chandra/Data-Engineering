
import json
from pathlib import Path
from datetime import datetime, timezone

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("CDC_Current_State")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

raw_path = Path("data/raw/cdc")
files = sorted(raw_path.glob("*.jsonl"))

if not files:
    raise FileNotFoundError("No CDC JSONL files found")

events = []

for file in files:
    with file.open(encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue

            record = json.loads(line)
            event = record.get("event")

            # Kafka tombstone: no event payload
            if not isinstance(event, dict):
                continue

            payload = event.get("payload", {})
            op = payload.get("op")

            if op not in ("r", "c", "u", "d"):
                continue

            before = payload.get("before") or {}
            after = payload.get("after") or {}

            state = after if op in ("r", "c", "u") else before

            events.append({
                "customer_id": int(record["customer_id"]),
                "operation": op,
                "kafka_partition": int(record["kafka_partition"]),
                "kafka_offset": int(record["kafka_offset"]),
                "event_ts_ms": int(payload.get("ts_ms") or 0),
                "event_ts": datetime.fromtimestamp(
                    int(payload.get("ts_ms") or 0) / 1000,
                    tz=timezone.utc
                ),
                "city": state.get("City"),
                "updated_at_ns": state.get("UpdatedAt")
            })

if not events:
    raise ValueError("No valid CDC events found")

# Build Spark event DataFrame
event_df = spark.createDataFrame(events)

print("\n========== CDC EVENT SUMMARY ==========")
event_df.groupBy("operation").count().orderBy("operation").show()

print("Total CDC events:", event_df.count())

# Persist normalized event log
event_pd = event_df.orderBy(
    "kafka_partition", "kafka_offset"
).toPandas()

event_pd.to_parquet(
    "data/processed/cdc_events_normalized.parquet",
    index=False
)

# Latest event per customer
window = (
    Window.partitionBy("customer_id")
    .orderBy(
        F.col("kafka_partition").desc(),
        F.col("kafka_offset").desc()
    )
)

latest = (
    event_df
    .withColumn("rn", F.row_number().over(window))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# Delete means absent from current state
current = (
    latest
    .filter(F.col("operation") != "d")
    .select(
        "customer_id",
        "city",
        "updated_at_ns",
        "event_ts",
        "kafka_partition",
        "kafka_offset"
    )
)

print("\n========== CURRENT STATE ==========")
current.orderBy("customer_id").show(truncate=False)
print("Current customer count:", current.count())

current_pd = current.toPandas()
current_pd.to_parquet(
    "data/processed/cdc_customer_current.parquet",
    index=False
)

print("\nCDC current-state processing completed.")

spark.stop()
