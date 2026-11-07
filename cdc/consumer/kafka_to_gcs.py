
import json
import time

from confluent_kafka import Consumer, KafkaException, TopicPartition
from google.cloud import storage


# Kafka configuration
KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:29092",
    "group.id": "cdc-to-gcs-consumer",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
}

TOPIC = "sqlserver_lab.AdventureWorksDW2022.dbo.CDC_LearningCustomer"

# GCS configuration
GCP_PROJECT = "electric-tesla-507710-k2"
BUCKET_NAME = "advworks-dev-ingestion"
GCS_PREFIX = "cdc/customer_events"

BATCH_SIZE = 5
FLUSH_INTERVAL = 10


def main():

    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe([TOPIC])

    storage_client = storage.Client(project=GCP_PROJECT)
    bucket = storage_client.bucket(BUCKET_NAME)

    # Maintain separate batches for each Kafka partition
    batches = {}
    batch_start_times = {}

    print(f"Listening to Kafka topic: {TOPIC}")
    print(f"GCS destination: gs://{BUCKET_NAME}/{GCS_PREFIX}/")

    try:

        while True:

            message = consumer.poll(1.0)

            if message is not None:

                if message.error():
                    raise KafkaException(message.error())

                partition = message.partition()

                key = message.key()
                key_data = json.loads(key.decode("utf-8")) if key else None

                customer_id = (
                    key_data.get("payload", {}).get("CustomerID")
                    if key_data else None
                )

                value = message.value()

                if value is None:

                    # Kafka tombstone, not a separate database DELETE
                    record = {
                        "record_type": "tombstone",
                        "customer_id": customer_id
                    }

                else:

                    event = json.loads(value.decode("utf-8"))

                    record = {
                        "record_type": "cdc_event",
                        "customer_id": customer_id,
                        "event": event
                    }

                # Preserve Kafka metadata for traceability
                record["kafka_topic"] = message.topic()
                record["kafka_partition"] = partition
                record["kafka_offset"] = message.offset()

                if partition not in batches:
                    batches[partition] = []
                    batch_start_times[partition] = time.monotonic()

                batches[partition].append({
                    "record": record,
                    "message": message
                })

                print(
                    f"Received: Customer={customer_id}, "
                    f"Offset={message.offset()}"
                )

            # Check whether any partition batch needs uploading
            for partition in list(batches.keys()):

                batch = batches[partition]

                if not batch:
                    continue

                elapsed = time.monotonic() - batch_start_times[partition]

                if len(batch) >= BATCH_SIZE or elapsed >= FLUSH_INTERVAL:

                    first_offset = batch[0]["message"].offset()
                    last_offset = batch[-1]["message"].offset()

                    # Convert records into JSON Lines
                    jsonl_data = "\n".join(
                        json.dumps(item["record"])
                        for item in batch
                    )

                    object_name = (
                        f"{GCS_PREFIX}/"
                        f"partition={partition}/"
                        f"offsets-{first_offset:012d}-{last_offset:012d}.jsonl"
                    )

                    blob = bucket.blob(object_name)

                    # Upload to GCS
                    blob.upload_from_string(
                        jsonl_data + "\n",
                        content_type="application/x-ndjson"
                    )

                    print("\nGCS upload successful:")
                    print(f"gs://{BUCKET_NAME}/{object_name}")

                    # Commit only after successful upload
                    consumer.commit(
                        offsets=[
                            TopicPartition(
                                TOPIC,
                                partition,
                                last_offset + 1
                            )
                        ],
                        asynchronous=False
                    )

                    print(
                        f"Kafka offset committed: {last_offset + 1}\n"
                    )

                    batches[partition] = []
                    batch_start_times[partition] = time.monotonic()

    except KeyboardInterrupt:
        print("\nConsumer stopped by user.")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
