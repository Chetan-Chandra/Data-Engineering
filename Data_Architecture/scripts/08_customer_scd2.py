
from pathlib import Path
import pandas as pd

source = Path("data/processed/cdc_events_normalized.parquet")
events = pd.read_parquet(source)

events = events.sort_values(
    ["kafka_partition", "kafka_offset"]
)

history = []
active = {}

for _, event in events.iterrows():
    customer_id = int(event["customer_id"])
    operation = event["operation"]
    event_time = pd.to_datetime(event["event_ts"], utc=True)

    # Expire previous version, if any
    if customer_id in active:
        previous = active[customer_id]
        previous["effective_to"] = event_time
        previous["is_current"] = False
        active.pop(customer_id)

    if operation == "d":
        # Create a current deletion marker
        row = {
            "customer_id": customer_id,
            "city": None,
            "effective_from": event_time,
            "effective_to": pd.NaT,
            "is_current": True,
            "is_deleted": True,
            "kafka_offset": int(event["kafka_offset"])
        }

        history.append(row)
        active[customer_id] = row

    elif operation in ("r", "c", "u"):
        row = {
            "customer_id": customer_id,
            "city": event["city"],
            "effective_from": event_time,
            "effective_to": pd.NaT,
            "is_current": True,
            "is_deleted": False,
            "kafka_offset": int(event["kafka_offset"])
        }

        history.append(row)
        active[customer_id] = row

history_df = pd.DataFrame(history)

output = Path("data/processed/cdc_customer_history.parquet")
history_df.to_parquet(output, index=False)

print("\n========== SCD TYPE 2 SUMMARY ==========")
print("Total historical versions:", len(history_df))
print("Current versions:", int(history_df["is_current"].sum()))
print("Deleted versions:", int(history_df["is_deleted"].sum()))

print("\n========== CUSTOMER 10 HISTORY ==========")
print(
    history_df[history_df["customer_id"] == 10]
    .sort_values("effective_from")
    .to_string(index=False)
)

print("\nSCD Type 2 history written:", output)
