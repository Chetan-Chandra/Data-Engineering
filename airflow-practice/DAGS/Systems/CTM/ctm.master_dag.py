# this is a comment  
import pendulum
from airflow.decorators import dag, task
from operators.extraction import ExtractionOperator
from connectors.odbc.source.mssql_source_connector import MssqlSourceConnector
from connectors.cloud.target.gcs_target_connector import GCSTargetConnector 
from Systems.CTM.configurations.ctm_config import TABLES_CONFIG_7_AM
from airflow.models import Variable
from datetime import datetime

local_tz = pendulum.timezone("America/Toronto")

def _create_dag(table_name: str, schedule: str):
    """Dynamically creates a DAG for a single CTM table configuration."""
    dag_id = f"ctm.fullload.{table_name}".lower()

    @dag(
        dag_id=dag_id,
        start_date=datetime(2023, 1, 1, tzinfo=local_tz),
        schedule=schedule,
        catchup=False,
        tags=["odbc", "mssql", "ctm", "fullload", "gcs"],
        max_active_runs=1
    )
    def generated_dag():
        @task
        def run_extraction(**context):
            """Instantiates objects and executes operator logic at runtime."""

            # 1. Instantiate the Source
            source_obj = MssqlSourceConnector(
                conn_id="ctm",
                database=Variable.get("CTM_DATABASE"),
                sql=f'SELECT * FROM [{Variable.get("CTM_DATABASE")}].[dbo].[{table_name}]',
                query_mode='default'
            )

            # 2. Instantiate the Target
            gcs_target = GCSTargetConnector(
                conn_id="gcs-bucket-project",
                gcs_path="raw/CTM",
                table_name=table_name
            )

            # 3. Instantiate and execute the ExtractionOperator
            op = ExtractionOperator(
                task_id=f"extract_{table_name.lower()}",
                source=source_obj,
                targets=[gcs_target]
            )
            return op.execute(context=context)

        run_extraction()

    return generated_dag()

# Loop through the configuration lists and create a DAG for each active table
for table in TABLES_CONFIG_7_AM:
    _create_dag(table, schedule="0 7 * * *")
