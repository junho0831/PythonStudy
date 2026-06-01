from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow_modules.er_dose_jobs import run_er_dose_backfill_daily


LOCAL_TZ = pendulum.timezone("Asia/Seoul")


with DAG(
    dag_id="er_dose_backfill_daily",
    description="Reprocess the latest 3 days of ER Dose logs in 10 minute chunks once a day",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2026, 6, 1, tz=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["er_dose", "backfill"],
) as dag:
    run_er_dose_task = PythonOperator(
        task_id="run_er_dose_backfill_daily",
        python_callable=run_er_dose_backfill_daily,
    )
