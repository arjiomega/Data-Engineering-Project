from pendulum import DateTime

from airflow.decorators import task

from utils import get_project_id, get_date

def get_partition_dates(input_df, date_column_name: str):
    input_df['year'] = input_df[date_column_name].dt.year
    input_df['month'] = input_df[date_column_name].dt.month
    input_df['day'] = input_df[date_column_name].dt.day

    return input_df

@task
def extract_green_to_gcs(logical_date: DateTime):
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

@task
def extract_yellow_to_gcs(logical_date: DateTime):
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

class ExtractTasks:
    def extract_green(*args, **kwargs):
        return extract_green_to_gcs(*args, **kwargs)
    def extract_yellow(*args, **kwargs):
        return extract_yellow_to_gcs(*args, **kwargs)