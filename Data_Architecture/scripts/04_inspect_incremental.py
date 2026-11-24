import json
from pathlib import Path

folder = Path("data/raw/incremental")

safe_fields = {
    "CustomerKey",
    "GeographyKey",
    "Gender",
    "MaritalStatus",
    "EnglishEducation",
    "EnglishOccupation",
    "YearlyIncome",
    "NumberCarsOwned",
    "TotalChildren",
    "UpdatedAt",
    "Operation",
    "operation",
    "op",
}

for file in sorted(folder.glob("*.jsonl")):
    print(f"\n========== {file.name} ==========")

    with file.open(encoding="utf-8") as f:
        records = [json.loads(line) for line in f if line.strip()]

    print("Record count:", len(records))

    for record in records:
        print({key: value for key, value in record.items()
               if key in safe_fields})