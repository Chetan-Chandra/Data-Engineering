import json
import base64
from decimal import Decimal
from datetime import date, datetime
from pathlib import Path

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook


# ============================================================
# AIRFLOW CONNECTIONS
# ============================================================

SQLSERVER_CONN_ID = "advworks_sqlserver"
GCP_CONN_ID = "advworks_gcp"


# ============================================================
# PIPELINE CONFIGURATION
# ============================================================

BATCH_SIZE = 1000

GCS_BUCKET_NAME = "advworks-dev-ingestion"

WATERMARK_TABLE = "dbo.ETL_Watermark"

RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")


INCREMENTAL_TABLES = {

    "DimCustomer": {

        "pipeline_name": "DIM_CUSTOMER_INCREMENTAL",

        "watermark_column": "CustomerKey"
    }
}


# ============================================================
# TABLE CONFIGURATION
# ============================================================

TABLES = {

    "DimCustomer": {

        "source_table": "dbo.DimCustomer",

        "gcs_folder": "dim_customer",

        "local_folder": Path(
            "output/dim_customer"
        ),

        "file_prefix": "DimCustomer_batch",

        "order_by": [
            "CustomerKey"
        ]
    },

    "DimProduct": {

        "source_table": "dbo.DimProduct",

        "gcs_folder": "dim_product",

        "local_folder": Path(
            "output/dim_product"
        ),

        "file_prefix": "DimProduct_batch",

        "order_by": [
            "ProductKey"
        ]
    }
}


# ============================================================
# SQL SERVER HOOK
# ============================================================

def create_sqlserver_hook():

    print(
        f"Creating SQL Server hook using Airflow connection: "
        f"{SQLSERVER_CONN_ID}"
    )

    return MsSqlHook(
        mssql_conn_id=SQLSERVER_CONN_ID
    )


# ============================================================
# GCS HOOK
# ============================================================

def create_gcs_hook():

    print(
        f"Creating GCS hook using Airflow connection: "
        f"{GCP_CONN_ID}"
    )

    return GCSHook(
        gcp_conn_id=GCP_CONN_ID
    )


# ============================================================
# VALIDATE GCS BUCKET
# ============================================================

def validate_gcs_bucket(
    gcs_hook
):

    bucket = gcs_hook.get_conn().bucket(
        GCS_BUCKET_NAME
    )

    if not bucket.exists():

        raise RuntimeError(
            f"GCS bucket does not exist: "
            f"gs://{GCS_BUCKET_NAME}"
        )

    print(
        f"GCS bucket validated: "
        f"gs://{GCS_BUCKET_NAME}"
    )

    return GCS_BUCKET_NAME


# ============================================================
# JSON SERIALIZER
# ============================================================

def json_serializer(value):

    if isinstance(value, (datetime, date)):

        return value.isoformat()

    if isinstance(value, Decimal):

        return float(value)

    if isinstance(value, bytes):

        return base64.b64encode(
            value
        ).decode("utf-8")

    raise TypeError(
        f"Type {type(value)} is not JSON serializable"
    )


# ============================================================
# SOURCE COUNT
# ============================================================

def get_source_count(
    connection,
    source_table
):

    query = f"""
        SELECT COUNT(*)
        FROM {source_table}
    """

    cursor = connection.cursor()

    cursor.execute(query)

    count = cursor.fetchone()[0]

    cursor.close()

    return count


# ============================================================
# WATERMARK
# ============================================================

def get_watermark(
    connection,
    pipeline_name,
    source_table
):

    query = f"""
        SELECT WATERMARK_VALUE
        FROM {WATERMARK_TABLE}
        WHERE PIPELINE_NAME = %s
          AND SOURCE_OBJECT = %s
    """

    cursor = connection.cursor()

    cursor.execute(
        query,
        (pipeline_name, source_table)
    )

    row = cursor.fetchone()

    cursor.close()

    if row is None:

        raise RuntimeError(
            f"No watermark found for "
            f"{pipeline_name} / {source_table}"
        )

    return int(row[0])


# ============================================================
# UPDATE WATERMARK
# ============================================================

def update_watermark(
    connection,
    pipeline_name,
    source_table,
    new_watermark
):

    query = f"""
        UPDATE {WATERMARK_TABLE}
        SET
            WATERMARK_VALUE = %s,
            UPDATED_AT = SYSDATETIME()
        WHERE PIPELINE_NAME = %s
          AND SOURCE_OBJECT = %s
    """

    cursor = connection.cursor()

    cursor.execute(
        query,
        (
            str(new_watermark),
            pipeline_name,
            source_table
        )
    )

    if cursor.rowcount != 1:

        cursor.close()

        raise RuntimeError(
            f"Watermark update failed for "
            f"{pipeline_name} / {source_table}"
        )

    connection.commit()

    cursor.close()

    print(
        f"Watermark updated successfully: "
        f"{new_watermark}"
    )


# ============================================================
# GET SOURCE COLUMNS
# ============================================================

def get_columns(
    connection,
    source_table
):

    schema_name, table_name = (
        source_table.split(".")
    )

    query = """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s
          AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
    """

    cursor = connection.cursor()

    cursor.execute(
        query,
        (schema_name, table_name)
    )

    columns = [
        row[0]
        for row in cursor.fetchall()
    ]

    cursor.close()

    return columns


# ============================================================
# VALIDATE ORDER COLUMNS
# ============================================================

def validate_order_columns(
    columns,
    order_by_columns
):

    missing_columns = [

        column
        for column in order_by_columns

        if column not in columns
    ]

    if missing_columns:

        raise ValueError(
            "ORDER_BY_COLUMNS contains columns "
            f"not found in source table: "
            f"{missing_columns}"
        )


# ============================================================
# EXTRACT FULL LOAD BATCH
# ============================================================

def extract_batch(
    connection,
    source_table,
    columns,
    order_by_columns,
    offset,
    batch_size
):

    column_list = ", ".join(
        f"[{column}]"
        for column in columns
    )

    order_by = ", ".join(
        f"[{column}]"
        for column in order_by_columns
    )

    query = f"""
    SELECT {column_list}
    FROM {source_table}
    ORDER BY {order_by}
    OFFSET %s ROWS
    FETCH NEXT %s ROWS ONLY
    """

    cursor = connection.cursor()

    cursor.execute(
        query,
        (offset, batch_size)
    )

    rows = cursor.fetchall()

    cursor.close()

    records = []

    for row in rows:

        record = dict(
            zip(columns, row)
        )

        records.append(record)

    return records


# ============================================================
# EXTRACT INCREMENTAL BATCH
# ============================================================

def extract_incremental_batch(
    connection,
    source_table,
    columns,
    watermark_column,
    last_key,
    batch_size
):

    column_list = ", ".join(
        f"[{column}]"
        for column in columns
    )

    query = f"""
    SELECT TOP (%s)
        {column_list}
    FROM {source_table}
    WHERE [{watermark_column}] > %s
    ORDER BY [{watermark_column}]
    """

    cursor = connection.cursor()

    cursor.execute(
        query,
        (batch_size, last_key)
    )

    rows = cursor.fetchall()

    cursor.close()

    records = []

    for row in rows:

        record = dict(
            zip(columns, row)
        )

        records.append(record)

    return records


# ============================================================
# CONVERT RECORDS TO JSONL
# ============================================================

def records_to_jsonl(records):

    lines = []

    for record in records:

        lines.append(
            json.dumps(
                record,
                default=json_serializer
            )
        )

    return "\n".join(lines) + "\n"


# ============================================================
# WRITE LOCAL JSONL
# ============================================================

def write_local_jsonl(
    records,
    output_file
):

    with open(
        output_file,
        "w",
        encoding="utf-8"
    ) as file:

        file.write(
            records_to_jsonl(records)
        )


# ============================================================
# UPLOAD JSONL TO GCS
# ============================================================

def upload_to_gcs(
    gcs_hook,
    bucket_name,
    records,
    gcs_folder,
    filename
):

    blob_name = (
        f"{gcs_folder}/"
        f"{filename}"
    )

    jsonl_data = records_to_jsonl(
        records
    )

    gcs_hook.upload(
        bucket_name=bucket_name,
        object_name=blob_name,
        data=jsonl_data,
        mime_type="application/json"
    )

    print(
        f"Uploaded to GCS: "
        f"gs://{bucket_name}/"
        f"{blob_name}"
    )

    return blob_name


# ============================================================
# INGEST ONE TABLE
# ============================================================

def ingest_table(
    connection,
    gcs_hook,
    bucket_name,
    table_name,
    config
):

    print()
    print("=" * 70)
    print(
        f"INGESTING {table_name}"
    )
    print("=" * 70)

    source_table = config["source_table"]

    gcs_folder = config["gcs_folder"]

    local_folder = config["local_folder"]

    file_prefix = config["file_prefix"]

    order_by_columns = config["order_by"]

    source_count = get_source_count(
        connection,
        source_table
    )

    print(
        f"Source table: {source_table}"
    )

    print(
        f"Source record count: "
        f"{source_count}"
    )

    columns = get_columns(
        connection,
        source_table
    )

    print(
        f"Source columns discovered: "
        f"{len(columns)}"
    )

    validate_order_columns(
        columns,
        order_by_columns
    )

    print(
        "ORDER BY: "
        + ", ".join(order_by_columns)
    )

    local_folder.mkdir(
        parents=True,
        exist_ok=True
    )

    extracted_count = 0

    batch_number = 1

    offset = 0

    while offset < source_count:

        records = extract_batch(
            connection,
            source_table,
            columns,
            order_by_columns,
            offset,
            BATCH_SIZE
        )

        if not records:
            break

        filename = (
            f"{file_prefix}_"
            f"{batch_number:03d}.jsonl"
        )

        output_file = (
            local_folder /
            filename
        )

        write_local_jsonl(
            records,
            output_file
        )

        upload_to_gcs(
            gcs_hook,
            bucket_name,
            records,
            gcs_folder,
            filename
        )

        record_count = len(records)

        extracted_count += record_count

        print(
            f"Batch {batch_number:03d}: "
            f"{record_count} records"
        )

        offset += BATCH_SIZE

        batch_number += 1

    print()
    print("-" * 70)
    print(
        f"{table_name} INGESTION VALIDATION"
    )
    print("-" * 70)

    print(
        f"Source count:    {source_count}"
    )

    print(
        f"Extracted count: {extracted_count}"
    )

    difference = (
        source_count -
        extracted_count
    )

    print(
        f"Difference:      {difference}"
    )

    if difference != 0:

        raise Exception(
            f"{table_name}: "
            "Source and extracted "
            "counts do not match."
        )

    print(
        f"SUCCESS: {table_name} "
        "ingestion completed."
    )

    return {
        "table": table_name,
        "mode": "FULL",
        "source_count": source_count,
        "rows_extracted": extracted_count,
        "difference": difference,
    }


# ============================================================
# INGEST TABLE INCREMENTALLY
# ============================================================

def ingest_table_incremental(
    connection,
    gcs_hook,
    bucket_name,
    table_name,
    config
):

    print()
    print("=" * 70)
    print(
        f"INCREMENTAL INGESTING {table_name}"
    )
    print("=" * 70)

    source_table = config["source_table"]

    gcs_folder = config["gcs_folder"]

    local_folder = config["local_folder"]

    file_prefix = config["file_prefix"]

    pipeline_config = INCREMENTAL_TABLES[
        table_name
    ]

    pipeline_name = pipeline_config[
        "pipeline_name"
    ]

    watermark_column = pipeline_config[
        "watermark_column"
    ]

    columns = get_columns(
        connection,
        source_table
    )

    validate_order_columns(
        columns,
        [watermark_column]
    )

    current_watermark = get_watermark(
        connection,
        pipeline_name,
        source_table
    )

    print(
        f"Source table: {source_table}"
    )

    print(
        f"Watermark column: "
        f"{watermark_column}"
    )

    print(
        f"Current watermark: "
        f"{current_watermark}"
    )

    local_folder.mkdir(
        parents=True,
        exist_ok=True
    )

    extracted_count = 0

    batch_number = 1

    last_key = current_watermark

    max_extracted_key = current_watermark

    while True:

        records = extract_incremental_batch(
            connection,
            source_table,
            columns,
            watermark_column,
            last_key,
            BATCH_SIZE
        )

        if not records:
            break

        batch_max_key = max(
            record[watermark_column]
            for record in records
        )

        filename = (
            f"{file_prefix}_"
            f"incremental_"
            f"{RUN_ID}_"
            f"{batch_number:03d}.jsonl"
        )

        output_file = (
            local_folder /
            filename
        )

        write_local_jsonl(
            records,
            output_file
        )

        upload_to_gcs(
            gcs_hook,
            bucket_name,
            records,
            gcs_folder,
            filename
        )

        record_count = len(records)

        extracted_count += record_count

        max_extracted_key = batch_max_key

        print(
            f"Batch {batch_number:03d}: "
            f"{record_count} records | "
            f"Key range: "
            f"{last_key + 1} -> "
            f"{batch_max_key}"
        )

        last_key = batch_max_key

        batch_number += 1

    print()
    print("-" * 70)
    print(
        f"{table_name} INCREMENTAL VALIDATION"
    )
    print("-" * 70)

    print(
        f"Previous watermark: "
        f"{current_watermark}"
    )

    print(
        f"Rows extracted:     "
        f"{extracted_count}"
    )

    print(
        f"New watermark:      "
        f"{max_extracted_key}"
    )

    if extracted_count > 0:

        update_watermark(
            connection,
            pipeline_name,
            source_table,
            max_extracted_key
        )

    else:

        print(
            "No new records found. "
            "Watermark unchanged."
        )

    print(
        f"SUCCESS: {table_name} "
        "incremental ingestion completed."
    )

    return {
        "table": table_name,
        "mode": "INCREMENTAL",
        "previous_watermark": current_watermark,
        "rows_extracted": extracted_count,
        "new_watermark": max_extracted_key,
    }


# ============================================================
# MAIN INGESTION
# ============================================================

def run_ingestion():

    print("=" * 70)
    print(
        "STAGE 8 - GCS ingestion using Airflow"
    )
    print("=" * 70)

    results = []

    connection = None

    try:

        sqlserver_hook = create_sqlserver_hook()

        connection = sqlserver_hook.get_conn()

        gcs_hook = create_gcs_hook()

        bucket_name = validate_gcs_bucket(
            gcs_hook
        )

        for table_name, config in TABLES.items():

            if table_name in INCREMENTAL_TABLES:

                result = (
                    ingest_table_incremental(
                        connection,
                        gcs_hook,
                        bucket_name,
                        table_name,
                        config
                    )
                )

            else:

                result = ingest_table(
                    connection,
                    gcs_hook,
                    bucket_name,
                    table_name,
                    config
                )

            results.append(result)

    finally:

        if connection:

            connection.close()

    print()
    print("=" * 70)
    print(
        "ALL DIMENSION INGESTION COMPLETED"
    )
    print("=" * 70)

    return {
        "status": "success",
        "tables": results,
    }


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    run_ingestion()