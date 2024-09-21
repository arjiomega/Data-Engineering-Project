from airflow.operators.python import PythonOperator

from utils import get_project_id, get_date


class ExtractTasks:
    def __init__(self) -> None:
        pass

    def _get_partition_dates(self, input_df, date_column_name: str):
        input_df["year"] = input_df[date_column_name].dt.year
        input_df["month"] = input_df[date_column_name].dt.month
        input_df["day"] = input_df[date_column_name].dt.day

        return input_df

    def _extract_data_to_gcs(self, data_name, logical_date):
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq
        import os

        year, month = get_date(logical_date)
        project_id = get_project_id(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])

        BUCKET_NAME = f"{project_id}-bucket"
        DATA_GROUP_NAME = os.environ["DBT_GOOGLE_BIGQUERY_DATASET_DEV"]
        CAB_DATA_BASE_URL = os.environ["CAB_DATA_BASE_URL"]

        df = pd.read_parquet(
            f"{CAB_DATA_BASE_URL}/{data_name}_tripdata_{year}-{month}.parquet"
        )
        date_column_name = (
            "tpep_pickup_datetime" if data_name == "yellow" else "lpep_pickup_datetime"
        )
        df = self._get_partition_dates(df, date_column_name)

        table = pa.Table.from_pandas(df)
        gcs = pa.fs.GcsFileSystem()

        root_path = f"{BUCKET_NAME}/{DATA_GROUP_NAME}/{data_name}_cab_data/raw"

        pq.write_to_dataset(
            table, root_path, partition_cols=["year", "month"], filesystem=gcs
        )

    def extract_green(self):
        return PythonOperator(
            task_id="extract_green_cab_to_gcs",
            python_callable=self._extract_data_to_gcs,
            op_kwargs={"data_name": "green"},
        )

    def extract_yellow(self):
        return PythonOperator(
            task_id="extract_yellow_cab_to_gcs",
            python_callable=self._extract_data_to_gcs,
            op_kwargs={"data_name": "yellow"},
        )
