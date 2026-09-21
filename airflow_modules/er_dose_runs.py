"""Run a date range on the Airflow host, without creating a controller DAG."""
import argparse
import time
from datetime import date, timedelta

import pendulum
from airflow.api.common.trigger_dag import trigger_dag
from airflow.exceptions import DagRunAlreadyExists
from airflow.models import DagBag, DagRun

DAG_ID = 'er_dose_daily'


def target_dates(reference, start=None, end=None):
    first = date.fromisoformat(start) if start else reference - timedelta(days=10)
    last = date.fromisoformat(end) if end else (first if start else reference - timedelta(days=1))
    if (start and first.isoformat() != start) or (end and last.isoformat() != end):
        raise ValueError('dates must be YYYY-MM-DD')
    if not 0 <= (last - first).days < 10:
        raise ValueError('date range must contain between 1 and 10 days')
    return [(first + timedelta(days=i)).isoformat() for i in range((last - first).days + 1)]


def ensure_run(target):
    logical_date = pendulum.parse(target, tz='UTC')
    existing = DagRun.find(dag_id=DAG_ID, execution_date=logical_date)
    if existing:
        run = existing[0]
    else:
        try:
            run = trigger_dag(
                dag_id=DAG_ID, run_id=f'manual__{target}', execution_date=logical_date,
                conf={'target_date': target}, replace_microseconds=False,
            )
        except DagRunAlreadyExists as error:
            run = error.dag_run
    if run is None or run.execution_date != logical_date or run.conf.get('target_date', target) != target:
        raise ValueError('existing run does not match the requested data date')
    return run


def recheck_run(run):
    # Only a successful run may be restarted. Failed runs retain their progress.
    dag = DagBag(read_dags_from_db=True).get_dag(DAG_ID)
    if dag is None:
        raise ValueError('DAG has not been loaded by the scheduler')
    dag.clear(start_date=run.execution_date, end_date=run.execution_date)


def run_dates(dates, recheck_successful=False, poll_seconds=30, timeout_seconds=86400):
    for target in dates:
        run = ensure_run(target)
        if run.state == 'success' and recheck_successful:
            recheck_run(run)
        deadline = time.monotonic() + timeout_seconds
        while True:
            current = DagRun.find(dag_id=DAG_ID, run_id=run.run_id)[0]
            if current.state == 'success':
                break
            if current.state == 'failed':
                raise RuntimeError(f'{target} failed; clear failed/downstream tasks in Airflow, then resume')
            if time.monotonic() >= deadline:
                raise TimeoutError(f'{target} has not completed; no later date was submitted')
            time.sleep(poll_seconds)
        print(f'{target}: success', flush=True)


def main():
    import fcntl
    from pathlib import Path
    from airflow.configuration import AIRFLOW_HOME

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--start-date')
    parser.add_argument('--end-date')
    parser.add_argument('--reference-date', default=pendulum.now('Asia/Seoul').date().isoformat())
    parser.add_argument('--recheck-successful', action='store_true',
                        help='clear successful date runs to compare source counts again')
    args = parser.parse_args()
    dates = target_dates(date.fromisoformat(args.reference_date), args.start_date, args.end_date)
    # Run this entry point on one Airflow host; reject overlapping invocations there.
    with (Path(AIRFLOW_HOME) / 'er_dose_runs.lock').open('w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        run_dates(dates, recheck_successful=args.recheck_successful)


if __name__ == '__main__':
    main()
