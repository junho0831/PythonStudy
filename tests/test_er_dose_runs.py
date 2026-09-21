"""Date-run metadata is real; scheduler completion and SSH/DB are simulated."""
import os
from datetime import date
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
pytest.importorskip('airflow')
import pendulum
from airflow.models import DagRun, Variable
from airflow.models.serialized_dag import SerializedDagModel
from airflow.utils.session import create_session
from airflow_modules import er_dose_runs as runs
from airflow_modules.er_dose_dates import prepare_run
from tests.test_er_dose_single_task import load_dag


def test_ten_day_range_crosses_year_boundary():
    values = runs.target_dates(date(2026, 1, 5))
    assert len(values) == 10
    assert values[0] == '2025-12-26'
    assert values[-1] == '2026-01-04'
    assert runs.target_dates(date(2026, 1, 5), '2026-01-02') == ['2026-01-02']


@pytest.mark.parametrize('first,last', [('2026-01-02','2026-01-01'), ('2026-01-01','2026-01-11'), ('2026-1-2',None)])
def test_invalid_ranges(first, last):
    with pytest.raises(ValueError):
        runs.target_dates(date(2026, 1, 12), first, last)


@pytest.mark.parametrize('conf,logical', [
    ({'target_date': '2026-05-13'}, pendulum.datetime(2026,5,12,tz='UTC')),
    ({'end_date': '2026-05-13'}, pendulum.datetime(2026,5,12,tz='UTC')),
    ({}, pendulum.datetime(2026,5,12,12,tz='UTC')),
])
def test_ui_date_cannot_differ_from_processed_date(conf, logical):
    with pytest.raises(ValueError):
        prepare_run(ti=Mock(), logical_date=logical, dag_run=SimpleNamespace(conf=conf))


def test_start_publishes_ui_logical_date(monkeypatch):
    monkeypatch.setattr(Variable, 'get', lambda key: key + ' {target_date}')
    ti = Mock()
    prepare_run(ti=ti, logical_date=pendulum.datetime(2026,5,12,tz='UTC'), dag_run=SimpleNamespace(conf={}))
    assert ti.xcom_push.call_args_list[0].kwargs == {'key': 'date', 'value': '2026-05-12'}


integration = pytest.mark.skipif(os.environ.get('ER_DOSE_DAG_INTEGRATION') != '1', reason='isolated metadata required')


def register_and_clean(dates):
    dag = load_dag()
    dag.sync_to_db()
    SerializedDagModel.write_dag(dag, min_update_interval=0)
    with create_session() as session:
        for target in dates:
            for run in session.query(DagRun).filter_by(dag_id=runs.DAG_ID, execution_date=pendulum.parse(target, tz='UTC')):
                session.delete(run)


def set_state(run_id, state):
    with create_session() as session:
        run = session.query(DagRun).filter_by(dag_id=runs.DAG_ID, run_id=run_id).one()
        run.set_state(state)


@integration
@pytest.mark.parametrize('failure', [False, True])
def test_real_runs_have_exact_dates_and_wait_before_next(monkeypatch, failure):
    dates = runs.target_dates(date(2026, 1, 12))
    register_and_clean(dates)
    real_trigger = runs.trigger_dag
    created = []
    def trigger(**kwargs):
        if created:
            assert DagRun.find(dag_id=runs.DAG_ID, run_id=created[-1].run_id)[0].state == 'success'
        run = real_trigger(**kwargs)
        created.append(run)
        set_state(run.run_id, 'failed' if failure and len(created) == 2 else 'success')
        return run
    monkeypatch.setattr(runs, 'trigger_dag', trigger)
    if failure:
        with pytest.raises(RuntimeError, match='2026-01-03 failed'):
            runs.run_dates(dates, poll_seconds=0)
        assert len(created) == 2
    else:
        runs.run_dates(dates, poll_seconds=0)
        assert len(created) == 10
        # Resuming does not create duplicate runs or clear successful tasks.
        runs.run_dates(dates, poll_seconds=0)
        assert len(created) == 10
    for run, target in zip(created, dates):
        assert run.execution_date == pendulum.parse(target, tz='UTC')
        assert run.conf['target_date'] == target
        assert run.run_id == f'manual__{target}'


@integration
def test_recheck_clears_only_the_selected_successful_run():
    target = '2026-08-01'
    register_and_clean([target])
    run = runs.ensure_run(target)
    set_state(run.run_id, 'success')
    runs.recheck_run(run)
    assert DagRun.find(dag_id=runs.DAG_ID, run_id=run.run_id)[0].state == 'queued'


def test_timeout_does_not_submit_next_date(monkeypatch):
    run = SimpleNamespace(run_id='manual__2026-01-02', state='queued')
    ensure = Mock(return_value=run)
    monkeypatch.setattr(runs, 'ensure_run', ensure)
    monkeypatch.setattr(runs.DagRun, 'find', Mock(return_value=[run]))
    with pytest.raises(TimeoutError):
        runs.run_dates(['2026-01-02', '2026-01-03'], timeout_seconds=0)
    ensure.assert_called_once_with('2026-01-02')
