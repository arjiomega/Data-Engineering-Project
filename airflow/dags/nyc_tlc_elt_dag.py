from datetime import datetime, timedelta

from airflow import DAG

import extract_data, load_data


default_args = {
    'owner': 'Richard Omega',
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}


with DAG(
    dag_id = 'ingest_cab_data_to_gcs_v04',
    default_args = default_args,
    start_date=datetime(2024,1,1),
    schedule_interval='@monthly',
    max_active_runs=1
) as dag:
    from dotenv import load_dotenv
    load_dotenv()

    extract_tasks = extract_data.ExtractTasks()
    load_tasks = load_data.LoadTasks()

    load_tasks.create_empty_dataset()
    extract_tasks.extract_green() >> load_tasks.load_green()
    extract_tasks.extract_yellow() >> load_tasks.load_yellow()
