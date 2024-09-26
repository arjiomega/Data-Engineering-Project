import os

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
)

from utils import get_project_id

yellow_cab_schema = [
    {"name": "VendorID", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "tpep_pickup_datetime", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "tpep_dropoff_datetime", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "passenger_count", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "trip_distance", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "RatecodeID", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "store_and_fwd_flag", "type": "STRING", "mode": "NULLABLE"},
    {"name": "PULocationID", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "DOLocationID", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "payment_type", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "fare_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "extra", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "mta_tax", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "tip_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "tolls_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "improvement_surcharge", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "total_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "congestion_surcharge", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "airport_fee", "type": "FLOAT", "mode": "NULLABLE"},
]

green_cab_schema = [
    {"name": "VendorID", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "lpep_pickup_datetime", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "lpep_dropoff_datetime", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "store_and_fwd_flag", "type": "STRING", "mode": "NULLABLE"},
    {"name": "RatecodeID", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "PULocationID", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "DOLocationID", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "passenger_count", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "trip_distance", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "fare_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "extra", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "mta_tax", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "tip_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "tolls_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "ehail_fee", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "improvement_surcharge", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "total_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "payment_type", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "trip_type", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "congestion_surcharge", "type": "FLOAT", "mode": "NULLABLE"},
]


class LoadTasks:
    def __init__(
        self, green_cab_schema=green_cab_schema, yellow_cab_schema=yellow_cab_schema
    ) -> None:
        """Initializes the LoadTasks class with the given schemas.

        Args:
            green_cab_schema (list, optional): Schema for green cab data. Defaults to the global green_cab_schema.
            yellow_cab_schema (list, optional): Schema for yellow cab data. Defaults to the global yellow_cab_schema.
        """
        project_id = get_project_id(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
        self.BUCKET_NAME = f"{project_id}-bucket"
        self.DATASET_NAME = "nyc_taxi_data"
        self.FILE_FORMAT = "parquet"
        self.WAREHOUSE_LAYER = "raw"

        self.green_cab_schema = green_cab_schema
        self.yellow_cab_schema = yellow_cab_schema

    def create_empty_dataset(self):
        """Creates an empty dataset in BigQuery.

        Returns:
            BigQueryCreateEmptyDatasetOperator: An operator for creating an empty BigQuery dataset.
        """
        return BigQueryCreateEmptyDatasetOperator(
            task_id=f"create_bigquery_{self.DATASET_NAME}_dataset",
            gcp_conn_id="google_cloud_default",
            dataset_id=self.DATASET_NAME,
            if_exists="log",
        )

    def _get_file_uri(self, table_name) -> str:
        """Generates the file URI for a given table.

        Args:
            table_name (str): The name of the table to generate the URI for.

        Returns:
            str: The URI of the file in Google Cloud Storage.
        """
        year, month = "{{ logical_date.year }}", "{{ logical_date.month }}"
        return f"{self.DATASET_NAME}/{table_name}/{self.WAREHOUSE_LAYER}/year={year}/month={month}/*.{self.FILE_FORMAT}"

    def _load_raw_cab_data_to_bigquery(self, table_name, schema):
        """Creates an Airflow task to load data from Google Cloud Storage into BigQuery.

        Args:
            table_name (str): The name of the table to load data into.
            schema (list): The schema definition for the data to be loaded.

        Returns:
            GCSToBigQueryOperator: An operator for loading data from Google Cloud Storage into BigQuery.
        """
        file_uri = self._get_file_uri(table_name)

        return GCSToBigQueryOperator(
            task_id=f"load_{self.WAREHOUSE_LAYER}_{table_name}_to_biquery",
            bucket=self.BUCKET_NAME,
            source_objects=[file_uri],
            destination_project_dataset_table=f"{self.DATASET_NAME}.{self.WAREHOUSE_LAYER}_{table_name}",
            schema_fields=schema,
            write_disposition="WRITE_APPEND",
            source_format=self.FILE_FORMAT,
        )

    def load_yellow(self):
        """Creates an Airflow task to load yellow cab data from Google Cloud Storage into BigQuery.

        Returns:
            GCSToBigQueryOperator: An operator for loading yellow cab data into BigQuery.
        """
        return self._load_raw_cab_data_to_bigquery(
            table_name="yellow_cab_data", schema=self.yellow_cab_schema
        )

    def load_green(self):
        """Creates an Airflow task to load green cab data from Google Cloud Storage into BigQuery.

        Returns:
            GCSToBigQueryOperator: An operator for loading green cab data into BigQuery.
        """
        return self._load_raw_cab_data_to_bigquery(
            table_name="green_cab_data", schema=self.green_cab_schema
        )
