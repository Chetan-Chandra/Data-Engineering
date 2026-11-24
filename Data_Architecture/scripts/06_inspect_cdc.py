
import json
from pathlib import Path

folder = Path("data/raw/cdc")

safe_fields = {
    "CustomerID", "customer_id",
    "UpdatedAt", "updated_at",
    "City", "city",
    "op", "ts_ms"
}

for file in sorted(folder.glob("*.jsonl")):
    print(f"\n========== {file.name} ==========")

    with file.open(encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue

            record = json.loads(line)
            event = record.get("event")

            if not isinstance(event, dict):
                print(
                    "TOMBSTONE:",
                    record.get("customer_id"),
                    "offset:",
                    record.get("kafka_offset")
                )
                continue

            payload = event.get("payload", {})
            before = payload.get("before")
            after = payload.get("after")

            print(
                "\nCustomer:",
                record.get("customer_id"),
                "| Offset:",
                record.get("kafka_offset"),
                "| Operation:",
                payload.get("op")
            )

            for label, value in [("BEFORE", before), ("AFTER", after)]:
                if isinstance(value, dict):
                    print(label, "columns:", list(value.keys()))
                    print(
                        label,
                        "safe values:",
                        {k: v for k, v in value.items() if k in safe_fields}
                    )
                else:
                    print(label, ":", value)

print("\nCDC schema inspection completed.")
