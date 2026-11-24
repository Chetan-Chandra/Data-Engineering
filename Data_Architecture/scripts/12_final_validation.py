
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq

base = Path("data/processed")

expected = {
    "dim_customer.parquet": 18484,
    "dim_customer_updated.parquet": 18488,
    "dim_product.parquet": 606,
    "fact_sales.parquet": 60398,
    "joined_sales.parquet": 60398,
    "product_sales.parquet": 158,
    "cdc_events_normalized.parquet": 9,
    "cdc_customer_current.parquet": 6,
    "cdc_customer_history.parquet": 9,
    "fact_sales_incremental.parquet": 60398
}

print("\n========== FINAL FILE VALIDATION ==========")

for filename, expected_rows in expected.items():
    path = base / filename

    if not path.exists():
        raise FileNotFoundError(f"Missing output: {filename}")

    actual_rows = pq.ParquetFile(path).metadata.num_rows

    print(f"{filename}: {actual_rows:,} rows")

    if actual_rows != expected_rows:
        raise ValueError(
            f"{filename}: expected {expected_rows}, got {actual_rows}"
        )

# Sales business-key validation
sales = pd.read_parquet(base / "fact_sales.parquet")

duplicate_sales = sales.duplicated(
    ["SalesOrderNumber", "SalesOrderLineNumber"]
).sum()

assert duplicate_sales == 0, "Duplicate sales business keys found"

# Customer validation
customers = pd.read_parquet(base / "dim_customer_updated.parquet")

assert customers["CustomerKey"].is_unique

# Product validation
products = pd.read_parquet(base / "dim_product.parquet")

assert products["ProductKey"].is_unique

# CDC current-state validation
cdc = pd.read_parquet(base / "cdc_customer_current.parquet")

assert cdc["customer_id"].is_unique
assert 10 in cdc["customer_id"].values

customer_10 = cdc[cdc["customer_id"] == 10].iloc[0]
assert customer_10["city"] == "Kolkata"

# SCD2 validation
history = pd.read_parquet(base / "cdc_customer_history.parquet")

current_counts = (
    history[history["is_current"]]
    .groupby("customer_id")
    .size()
)

assert (current_counts <= 1).all()

customer_10_history = history[
    history["customer_id"] == 10
].sort_values("kafka_offset")

assert len(customer_10_history) == 4
assert customer_10_history["is_deleted"].sum() == 1
assert customer_10_history.iloc[-1]["is_current"]

print("\n========== FINAL CHECKS ==========")
print("Sales duplicate keys:", duplicate_sales)
print("Customer duplicate keys:", customers["CustomerKey"].duplicated().sum())
print("Product duplicate keys:", products["ProductKey"].duplicated().sum())
print("CDC current-state customers:", len(cdc))
print("Customer 10 historical versions:", len(customer_10_history))

print("\nALL FINAL VALIDATIONS PASSED.")
