def get_project_id(gcp_creds_path):
    """
    Retrieves the GCP project ID from a provided service account credentials file.

    Args:
        gcp_creds_path (str): The file path to the Google Cloud service account JSON credentials.

    Returns:
        str: The Google Cloud project ID extracted from the credentials file.
    """
    import json

    with open(gcp_creds_path) as f:
        gcp_creds = json.load(f)

    return gcp_creds['project_id']

def get_date(logical_date) -> tuple[str,str]:
    """
    Extracts the year and month from a logical date object and returns them as strings.

    Args:
        logical_date (datetime): The logical date from which to extract the year and month.

    Returns:
        tuple[str, str]: A tuple containing the year and month as strings, in the format ('YYYY', 'MM').
    """
    year, month = logical_date.strftime("%Y"), logical_date.strftime("%m")
    return year, month

def parquet_reader(logical_date):
    import os

    import pyarrow as pa
    import pyarrow.parquet as pq

    year, month = get_date(logical_date)

    gs = pa.fs.GcsFileSystem()
    project_id = get_project_id(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
    
    BUCKET_NAME = f"{project_id}-bucket"
    DATA_GROUP_NAME = os.environ['DATA_GROUP_NAME']

    root_path = f"{BUCKET_NAME}/{DATA_GROUP_NAME}/yellow_cab_data/raw/year={year}/month={int(month)}"

    parquet_dataset = pq.ParquetDataset(root_path, filesystem=gs)
    print("parquet dataset loaded.")

    pyarrow_table = parquet_dataset.read_pandas()
    print("pyarrow table loaded.")

    df = pyarrow_table.to_pandas()
    print("df loaded.")

    print(df)
