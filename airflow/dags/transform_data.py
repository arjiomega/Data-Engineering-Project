from typing import Literal
from airflow.operators.bash import BashOperator


class StagingTasks:
    def __init__(self) -> None:
        pass

    def _staging_task(
        self, task: Literal["run", "test"], data_name: Literal["green", "yellow"]
    ):
        return BashOperator(
            task_id=f"{task}_staging_{data_name}",
            bash_command=f"dbt {task} --select stg_bigquery__{data_name}_cab",
            cwd='/opt/airflow'
        )

    def run_staging_green(self):
        return self._staging_task(task="run", data_name="green")

    def run_staging_yellow(self):
        return self._staging_task(task="run", data_name="yellow")

    def test_staging_green(self):
        return self._staging_task(task="test", data_name="green")

    def test_staging_yellow(self):
        return self._staging_task(task="test", data_name="yellow")
