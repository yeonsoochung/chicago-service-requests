# Imports
from airflow import DAG
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta
from google.cloud import storage
import pandas as pd
from sodapy import Socrata
import csr_bq_transform_data

# Define API input parameters and file path variables
app_token = "<My App Token>" # This is hidden for security reasons
client = Socrata("data.cityofchicago.org", app_token) 
csv_name = "chicago-service-requests.csv"
csv_path = f"/opt/airflow/data/{csv_name}"
gcs_bucket = "csr-project-bucket"
gcs_path = f"csv-data/{csv_name}"
gcp_project_id = "csr-project-453302"

def create_csv(app_token, client):
    data = []
    chunk_size = 2000
    offset = 0
    client.timeout = 60

    # today = datetime.now()
    # start_date = today - timedelta(days = 7)
    # start_date = start_date.strftime("%Y-%m-%dT%H:%M:%S")  # Format as ISO 8601
    start_date = '2023-01-01T00:00:00'

    while True:
        # Retrieve a chunk of data
        chunk = client.get("v6vf-nfxy", limit=chunk_size, offset=offset, 
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

# Task to upload data to GCS bucket
upload_to_gcs_task = LocalFilesystemToGCSOperator(
    task_id="upload_to_gcs",
    src=csv_path,  # Local file path
    dst=gcs_path,  # Destination path in GCS bucket
    bucket=gcs_bucket,
    mime_type="text/csv",
    dag=dag
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

# Task to transform csr_raw data table and create dates table
transform_sr = PythonOperator(
    task_id="run_bq_transform_sr_data",
    python_callable=csr_bq_transform_data.transform_sr_data,
    dag=dag
)

transform_ca = PythonOperator(
    task_id="run_bq_transform_community_areas_data",
    python_callable=csr_bq_transform_data.community_areas,
    dag=dag
)

# Define task dependencies
latest_only >> download_task >> upload_to_gcs_task >> \
    load_gcs_to_bq_task_1 >> load_gcs_to_bq_task_2 >> load_gcs_to_bq_task_3 >> transform_sr >> transform_ca

