from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq

output_path = Path("data/processed")

print("\n========== PARQUET VALIDATION ==========")

for file in sorted(output_path.glob("*.parquet")):
    parquet_file = pq.ParquetFile(file)

    print(f"\nFile: {file.name}")
    print(f"Rows: {parquet_file.metadata.num_rows:,}")
    print(f"Columns: {parquet_file.metadata.num_columns}")
    print(f"Row groups: {parquet_file.metadata.num_row_groups}")
    print(f"Size: {file.stat().st_size / 1024:.2f} KB")

    df = pd.read_parquet(file)
    print("Columns:", list(df.columns))

print("\nParquet validation completed.")