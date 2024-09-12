from pendulum import DateTime
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'Richard Omega',
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

def get_project_id(gcp_creds_path):
    import json

    with open(gcp_creds_path) as f:
        gcp_creds = json.load(f)

    return gcp_creds['project_id']

def get_date(logical_date) -> tuple[str,str]:
    year, month = logical_date.strftime("%Y"), logical_date.strftime("%m")
    return year, month

def get_partition_dates(input_df, date_column_name: str):
    input_df['year'] = input_df[date_column_name].dt.year
    input_df['month'] = input_df[date_column_name].dt.month
    input_df['day'] = input_df[date_column_name].dt.day

    return input_df

def ingest_green_to_gcs(logical_date: DateTime):
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    import os

    year, month = get_date(logical_date)
    project_id = get_project_id(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
    
    BUCKET_NAME = f"{project_id}-bucket"
    DATA_GROUP_NAME = os.environ['DATA_GROUP_NAME']
    CAB_DATA_BASE_URL = os.environ['CAB_DATA_BASE_URL']
    
    df = pd.read_parquet(f"{CAB_DATA_BASE_URL}/green_tripdata_{year}-{month}.parquet")
    df = get_partition_dates(df, 'lpep_pickup_datetime')

    table = pa.Table.from_pandas(df)
    gcs = pa.fs.GcsFileSystem()
    
    root_path = f"{BUCKET_NAME}/{DATA_GROUP_NAME}/green_cab_data/raw"

    pq.write_to_dataset(
        table,
        root_path,
        partition_cols=['year', 'month'],
        filesystem=gcs
    )


def ingest_yellow_to_gcs(logical_date):
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    import os

    year, month = get_date(logical_date)
    project_id = get_project_id(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
    
    BUCKET_NAME = f"{project_id}-bucket"
    DATA_GROUP_NAME = os.environ['DATA_GROUP_NAME']
    CAB_DATA_BASE_URL = os.environ['CAB_DATA_BASE_URL']

    df = pd.read_parquet(f"{CAB_DATA_BASE_URL}/yellow_tripdata_{year}-{month}.parquet")
    df = get_partition_dates(df, 'tpep_pickup_datetime')

    table = pa.Table.from_pandas(df)
    gcs = pa.fs.GcsFileSystem()

    root_path = f"{BUCKET_NAME}/{DATA_GROUP_NAME}/yellow_cab_data/raw"

    pq.write_to_dataset(
        table,
        root_path,
        partition_cols=['year', 'month'],
        filesystem=gcs
    )


with DAG(
    dag_id = 'ingest_cab_data_to_gcs_v01',
    default_args = default_args,
    start_date=datetime(2020,1,1),
    schedule_interval='@monthly',
    max_active_runs=5
) as dag:
    from dotenv import load_dotenv
    load_dotenv()

    task1 = PythonOperator(
        task_id = "ingest_green_to_gcs",
        python_callable = ingest_green_to_gcs
    )

    task2 = PythonOperator(
        task_id = "ingest_yellow_to_gcs",
        python_callable = ingest_yellow_to_gcs
    )

    [task1, task2]