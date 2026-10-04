# this is a comment
from airflow.decorators import dag, task
from datetime import datetime
import sys


# Make the DAGS directory importable
if "/opt/airflow/dags" not in sys.path:
    sys.path.insert(0, "/opt/airflow/dags")


@dag(
    dag_id="advworks_pipeline",
    start_date=datetime(2026, 9, 16),
    schedule=None,
    catchup=False,
    tags=["adventureworks", "data-engineering"],
)
def advworks_pipeline():

    @task
    def extract_data():

        from Systems.SQLServer.Adventureworks_Configuration import run_ingestion

        print("Starting AdventureWorks ingestion...")

        result = run_ingestion()

        print(f"Ingestion result: {result}")

        return result

    @task
    def validate_extraction(result):

        print("Validating Airflow extraction result...")

        if result["status"] != "success":
            raise RuntimeError("Extraction failed.")

        for table in result["tables"]:
            print(
                f"Table: {table['table']} | "
                f"Mode: {table['mode']} | "
                f"Rows: {table['rows_extracted']}"
            )

        print("Extraction validation successful.")

        return "validation_success"

    extraction_result = extract_data()

    validate_extraction(extraction_result)


advworks_pipeline()