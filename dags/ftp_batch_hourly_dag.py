from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow_modules.ftp_batch_jobs import run_combined


LOCAL_TZ = pendulum.timezone("Asia/Seoul")


with DAG(
    dag_id="ftp_batch_hourly",
    description="Run COMBINED FTP batch every hour",
    schedule="0 * * * *",
    start_date=pendulum.datetime(2026, 4, 11, tz=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["ftp", "batch"],
) as dag:
    run_combined_task = PythonOperator(
        task_id="run_combined",
        python_callable=run_combined,
        op_kwargs={"input_date": "{{ ds }}"},
    )
