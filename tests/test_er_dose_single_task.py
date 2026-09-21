"""One task, with progress surviving actual Airflow XCom clearing on retry."""
import os
from pathlib import Path
from unittest.mock import MagicMock, Mock

import pytest

pytest.importorskip("airflow")
pytest.importorskip("airflow.providers.ssh")

from airflow.exceptions import AirflowException
from airflow.models import DagBag, Variable
from airflow_modules import er_dose_process as process
from airflow_modules import er_dose_jobs as jobs


def load_dag():
    path = Path(__file__).resolve().parents[1] / "dags" / "er_dose_daily_dag.py"
    bag = DagBag(dag_folder=str(path), include_examples=False)
    assert not bag.import_errors
    dag = bag.dags["er_dose_daily"]
    assert len(dag.tasks) == 4
    return dag


@pytest.mark.parametrize("status", [0, 1])
def test_ssh_command_is_unchanged_and_failure_propagates(monkeypatch, status):
    hook = MagicMock()
    hook.exec_ssh_client_command.return_value = (status, b"done", b"failed")
    factory = Mock(return_value=hook)
    monkeypatch.setattr(process, "SSHHook", factory)
    command = 'cd "/existing path" && python parser.py --parser EXISTING --date 2026-05-12\n'
    if status:
        with pytest.raises(AirflowException, match="status 1"):
            process.run_parser(command)
    else:
        process.run_parser(command)
    factory.assert_called_once_with(ssh_conn_id="er_dose_parser", cmd_timeout=None)
    assert hook.exec_ssh_client_command.call_args.kwargs["command"] == command
    hook.get_conn.return_value.__exit__.assert_called_once()


@pytest.mark.skipif(os.environ.get("ER_DOSE_DAG_INTEGRATION") != "1", reason="isolated metadata DB required")
@pytest.mark.parametrize("raw,euv,failure", [
    (False, False, None), (True, False, None), (False, True, None), (True, True, None),
    (True, True, "euv"), (True, True, "raw"),
    (True, True, "die_yield"), (True, True, "root_cause"),
    (True, True, "raw_count"), (True, True, "euv_count"),
])
def test_single_task_resumes_completed_stages_without_replanning(monkeypatch, raw, euv, failure):
    import pendulum

    monkeypatch.setattr(jobs, "PostgresDB", Mock())
    compare = Mock(side_effect=[raw, euv])
    monkeypatch.setattr(jobs, "needs_reload", compare)
    events = []
    failed = False
    def stage(name):
        nonlocal failed
        events.append(name)
        if name == failure and not failed:
            failed = True
            raise RuntimeError("connection terminated")
    def parse(command):
        assert command.endswith("--date 2026-05-12")
        stage("euv" if "EUV" in command else "raw")
    def report(job, day_index, ti, plan_task_id):
        assert plan_task_id == "parse_er_data_raw"
        saved = ti.xcom_pull(task_ids=plan_task_id, key=jobs.PLAN_XCOM_KEY)[day_index]
        assert saved["target_date"] == "2026-05-12"
        assert saved["end_time"] == "2026-05-13T00:00:00"
        stage(job)
    monkeypatch.setattr(process, "run_parser", parse)
    monkeypatch.setattr(process, "report_day", report)
    dag = load_dag()
    for task in dag.tasks:
        task.retries = 0
    monkeypatch.setenv("AIRFLOW_VAR_ER_DOSE_EUV_COMMAND", "existing --parser EUV --date {target_date}")
    monkeypatch.setenv("AIRFLOW_VAR_ER_DOSE_RAW_COMMAND", "existing --parser RAW --date {target_date}")
    execution_date = pendulum.datetime(2026, 5, 12, tz="UTC")
    conf = {"target_date": "2026-05-12", "commands": {
        "ER_DOSE_EUV": "existing --parser EUV --date {target_date}",
        "ER_DOSE_RAW": "existing --parser RAW --date {target_date}",
    }}
    dag.test(execution_date=execution_date, run_conf=conf)
    run = dag.get_dagrun(execution_date=execution_date)
    ti = run.get_task_instance("parse_er_data_euv" if failure == "euv" else "parse_er_data_raw")
    key = process.progress_key(ti)
    expected = (["euv"] if euv else []) + (["raw"] if raw else [])
    if raw or euv:
        expected += list(process.REPORTS)
    if failure:
        assert run.state == "failed"
        saved = Variable.get(key, deserialize_json=True)
        assert saved["completed"] == expected[:expected.index(failure)]
        ti.clear_xcom_data()
        assert ti.xcom_pull(task_ids=ti.task_id, key=jobs.PLAN_XCOM_KEY) is None
        dag.test(execution_date=execution_date, run_conf=conf)
        run = dag.get_dagrun(execution_date=execution_date)
        index = expected.index(failure)
        assert events == expected[:index] + [failure] + expected[index:]
    else:
        assert events == expected
    assert run.state == "success"
    assert len(run.get_task_instances()) == 4
    assert compare.call_count == 2
    assert Variable.get(key, default_var=None) is None
