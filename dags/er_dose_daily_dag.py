from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from airflow_modules.er_dose_dates import prepare_run
from airflow_modules.er_dose_process import process_date


with DAG(
    'er_dose_daily',
    description='One data date per run: EUV then RAW and summaries',
    schedule=None,
    # Manual date runs may predate deployment; no scheduled catchup is enabled.
    start_date=pendulum.datetime(1970, 1, 1, tz='UTC'),
    catchup=False,
    max_active_runs=1,
    default_args={'retries': 3, 'retry_delay': timedelta(minutes=5)},
    tags=['er_dose'],
) as dag:
    start = PythonOperator(task_id='start', python_callable=prepare_run, do_xcom_push=False)
    ssh_operators = {
        'parse_er_data_euv': PythonOperator(
            task_id='parse_er_data_euv', python_callable=process_date,
            op_kwargs={'parser': 'euv'}, do_xcom_push=False,
        ),
        'parse_er_data_raw': PythonOperator(
            task_id='parse_er_data_raw', python_callable=process_date,
            op_kwargs={'parser': 'raw'}, do_xcom_push=False,
        ),
    }
    end = EmptyOperator(task_id='end')

    start >> ssh_operators['parse_er_data_euv'] >> ssh_operators['parse_er_data_raw'] >> end
