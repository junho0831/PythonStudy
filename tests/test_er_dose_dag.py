"""Check the deployed four-task DAG rather than the retired builder."""
from pathlib import Path

import pytest
pytest.importorskip('airflow')
pytest.importorskip('airflow.providers.ssh')

from airflow.models import DagBag
from airflow.utils.dag_cycle_tester import check_cycle


def test_daily_dag_preserves_order_and_retry_settings():
    path = Path(__file__).resolve().parents[1] / 'dags' / 'er_dose_daily_dag.py'
    bag = DagBag(dag_folder=str(path), include_examples=False)
    assert not bag.import_errors
    assert set(bag.dags) == {'er_dose_daily'}
    dag = bag.dags['er_dose_daily']
    check_cycle(dag)
    expected = ['start', 'parse_er_data_euv', 'parse_er_data_raw', 'end']
    assert dag.task_ids == expected
    for index, name in enumerate(expected):
        task = dag.get_task(name)
        assert task.upstream_task_ids == (set() if index == 0 else {expected[index - 1]})
        assert task.downstream_task_ids == (set() if index == 3 else {expected[index + 1]})
        assert task.trigger_rule == 'all_success'
        assert task.retries == 3
        assert task.retry_delay.total_seconds() == 300
    assert dag.get_task('parse_er_data_euv').op_kwargs == {'parser': 'euv'}
    assert dag.get_task('parse_er_data_raw').op_kwargs == {'parser': 'raw'}
    assert dag.max_active_runs == 1
    assert dag.schedule_interval is None
