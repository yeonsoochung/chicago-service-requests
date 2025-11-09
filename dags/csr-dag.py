# Imports
from airflow import DAG
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta
import time
from requests.exceptions import ReadTimeout
from google.cloud import storage
import pandas as pd
from sodapy import Socrata
# import csr_bq_transform_data
import os

# Define API input parameters and file path variables
app_token = os.getenv("API_KEY")
client = Socrata("data.cityofchicago.org", app_token) 
csv_name = "chicago-service-requests-recent.csv" # recent incremental batch
csv_path = f"/opt/airflow/data/{csv_name}"
gcs_bucket = "csr-project-bucket"
gcs_path = f"csv-data/{csv_name}"
gcp_project_id = "csr-project-453302"

def safe_get(client, dataset, limit, offset, select, where, max_retries=5):
    """
    Wrapper around client.get() that retries with exponential backoff on ReadTimeout.
    """
    for attempt in range(max_retries):
        try:
            return client.get(
                dataset,
                limit=limit,
                offset=offset,
                select=select,
                where=where)
        except ReadTimeout:
            wait = 2 ** attempt
            print(f"[Retry {attempt+1}/{max_retries}] Timeout at offset={offset}. Retrying in {wait} sec...")
            time.sleep(wait)
    raise Exception(f"Max retries reached for offset={offset}")

def create_csv(app_token, client):
    data = []
    chunk_size = 2000
    offset = 0
    client.timeout = 180

    start_date = '2025-01-01T00:00:00' # get recent batch of data

    while True:
        # Retrieve a chunk of data
        chunk = safe_get(client, dataset="v6vf-nfxy", limit=chunk_size, offset=offset,
                            select = " sr_number, sr_type, sr_short_code, owner_department, status, " 
                                "origin, created_date, last_modified_date, closed_date, street_address, "
                                "street_direction, street_name, street_type, community_area, "
                                "created_hour, created_day_of_week, created_month, latitude, longitude " ,
                            where = f"CREATED_DATE >= '{start_date}'")
        
        # Break the loop if no data is returned
        if not chunk:
            break
        
        # Append the chunk to the data list
        data.extend(chunk)
        
        # Update offset for the next iteration
        offset += chunk_size

    csr_data_df = pd.DataFrame.from_records(data)
    csr_data_df.to_csv(csv_path, index=False)

# Define DAG
dag = DAG(
    'csr_dag',
    start_date=datetime(2025, 3, 1),
    end_date=datetime(2025, 12, 31),
    schedule_interval="0 19 * * 0",  # Runs every Sunday at 19:00 UTC
    catchup=False,  # Prevents backfilling past missed runs
)

# Only execute DAG once if multiple scheduled runs are missed
latest_only = LatestOnlyOperator(
    task_id='latest_only',
    dag=dag,
)

# Instantiate task to download data from API
download_task = PythonOperator(
    task_id = 'download_data',
    python_callable = create_csv,
    op_kwargs = {
        "app_token": app_token,
        "client": client
    },
    dag = dag
)

def upload_to_gcs():
    client = storage.Client()
    bucket = client.bucket(gcs_bucket)
    blob = bucket.blob(gcs_path)
    
    # Optional: increase chunk size for performance on large files
    blob.chunk_size = 5 * 1024 * 1024  # 5 MB

    # Upload with a longer timeout
    blob.upload_from_filename(csv_path, content_type="text/csv", timeout=300)

upload_to_gcs_task = PythonOperator(
    task_id="upload_to_gcs",
    python_callable=upload_to_gcs,
    dag=dag,
)

bq_dataset = "chicago_service_requests"
bq_raw_table = "csr_raw"

# Task to load raw SR data from GCS to BQ (BigQuery)
load_gcs_to_bq_task_1 = GCSToBigQueryOperator(
    task_id="load_gcs_to_bq_csr",
    bucket=gcs_bucket,
    source_objects=gcs_path,  # Path in GCS
    destination_project_dataset_table=f"{gcp_project_id}.{bq_dataset}.{bq_raw_table}",
    source_format="CSV",
    skip_leading_rows=1,
    write_disposition="WRITE_TRUNCATE",  # Overwrites existing data
    autodetect=True,
    dag=dag,
)

# Task to load community areas table from GCS to BQ
load_gcs_to_bq_task_2 = GCSToBigQueryOperator(
    task_id="load_gcs_to_bq_ca",
    bucket=gcs_bucket,
    source_objects="csv-data/community-areas.csv",  # Path in GCS
    destination_project_dataset_table=f"{gcp_project_id}.{bq_dataset}.community_areas",
    source_format="CSV",
    skip_leading_rows=1,
    write_disposition="WRITE_TRUNCATE",  # Overwrites existing data
    autodetect=True,
    dag=dag,
)

# Task to load SR categories table from GCS to BQ
load_gcs_to_bq_task_3 = GCSToBigQueryOperator(
    task_id="load_gcs_to_bq_categories",
    bucket=gcs_bucket,
    source_objects="csv-data/chicago-sr-categories-types.csv",  # Path in GCS
    destination_project_dataset_table=f"{gcp_project_id}.{bq_dataset}.sr_categories",
    source_format="CSV",
    skip_leading_rows=1,
    write_disposition="WRITE_TRUNCATE",  # Overwrites existing data
    schema_fields=[
        {"name": "sr_category", "type": "STRING", "mode": "NULLABLE"},
        {"name": "sr_subcategory", "type": "STRING", "mode": "NULLABLE"},
        {"name": "sr_type", "type": "STRING", "mode": "NULLABLE"},
    ],
    dag=dag,
)

# Read sql files
sql_staging_path = "dags/csr_staging.sql"
with open(sql_staging_path, "r") as staging_file:
    sql_staging_query = staging_file.read()

sql_processing_path = "dags/csr_processing.sql"
with open(sql_processing_path, "r") as processing_file:
    sql_processing_query = processing_file.read()

# Backup existing csr_processed table
bq_data_backup = BigQueryInsertJobOperator(
    task_id = "backup_csr_processed",
    configuration = {
        "query": {
            "query": """
            CREATE OR REPLACE TABLE `chicago_service_requests.csr_processed_backup` AS
            SELECT * FROM `chicago_service_requests.csr_processed`;
            """,
            "useLegacySql": False,
        }
    }
)

# Task to stage and transform csr_raw data table and create dates and community_areas_processed tables
staging_query = BigQueryInsertJobOperator(
    task_id="run_staging_query",
    configuration={
        "query": {
            "query": sql_staging_query,
            "useLegacySql": False
        }
    },
    dag=dag,
)

processing_query = BigQueryInsertJobOperator(
    task_id="run_processing_query",
    configuration={
        "query": {
            "query": sql_processing_query,
            "useLegacySql": False
        }
    },
    dag=dag,
)

# Define task dependencies
latest_only >> download_task >> upload_to_gcs_task >> load_gcs_to_bq_task_1 >> load_gcs_to_bq_task_2 >> load_gcs_to_bq_task_3 >> bq_data_backup >> \
    staging_query >> processing_query

