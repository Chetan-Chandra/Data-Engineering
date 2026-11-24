
import subprocess
import sys
from pathlib import Path

scripts = [
    "02_transform_join.py",
    "03_validate_parquet.py",
    "05_incremental_upsert.py",
    "07_process_cdc.py",
    "08_customer_scd2.py",
    "09_incremental_sales.py",
    "10_spark_optimization.py",
    "12_final_validation.py"
]

script_dir = Path(__file__).resolve().parent

print("\n========== STARTING END-TO-END PIPELINE ==========")

for script in scripts:
    script_path = script_dir / script

    print(f"\n\n========== RUNNING {script} ==========")

    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=script_dir.parent
    )

    if result.returncode != 0:
        print(f"\nPIPELINE FAILED AT: {script}")
        sys.exit(result.returncode)

print("\n========== PIPELINE COMPLETED SUCCESSFULLY ==========")
