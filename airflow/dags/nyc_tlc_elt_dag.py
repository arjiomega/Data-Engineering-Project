from datetime import datetime, timedelta

from airflow import DAG


import extract_data, load_data, transform_data


default_args = {
    "owner": "Richard Omega",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="nyc_tlc_elt_dag_v10",
    default_args=default_args,
    start_date=datetime(2015, 1, 1),
    schedule_interval="@monthly",
    max_active_runs=1,
) as dag:
    from dotenv import load_dotenv

    load_dotenv()

    transform_data.load_seeds()

    extract_tasks = extract_data.ExtractTasks()
    load_tasks = load_data.LoadTasks()

    # transform
    staging_tasks = transform_data.StagingTasks()
    data_warehouse_tasks = transform_data.DataWarehouseTasks()

    load_tasks.create_empty_dataset()
    extract_tasks.create_bucket()

    green_flow = (
        extract_tasks.extract_green()
        >> load_tasks.load_green()
        >> staging_tasks.run_staging_green()
        >> staging_tasks.test_staging_green()
    )

    yellow_flow = (
        extract_tasks.extract_yellow()
        >> load_tasks.load_yellow()
        >> staging_tasks.run_staging_yellow()
        >> staging_tasks.test_staging_yellow()
    )
    
    [green_flow, yellow_flow] >> data_warehouse_tasks.build_datawarehouse()
