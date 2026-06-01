from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow_modules.er_dose_jobs import run_er_dose_window


LOCAL_TZ = pendulum.timezone("Asia/Seoul")


with DAG(
    dag_id="er_dose_near_realtime",
    description="Parse ER Dose logs every 10 minutes with a 10 minute processing delay",
    schedule="*/10 * * * *",
    start_date=pendulum.datetime(2026, 6, 1, tz=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["er_dose", "batch"],
) as dag:
    run_er_dose_task = PythonOperator(
        task_id="run_er_dose_window",
        python_callable=run_er_dose_window,
    )
