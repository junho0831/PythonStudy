from datetime import date, datetime
from unittest.mock import Mock

import pandas as pd
import pytest

from airflow_modules import er_dose_jobs as jobs
from er_dose.euv.euv_processor import ERDoseEUVProcessor
from er_dose.euv.euv_repository import ERDoseEUVRepository
from er_dose.raw.raw_processor import ERDoseProcessor, ERDoseEUVProcessor as LegacyEUVProcessor
from er_dose.raw.raw_repository import ERDoseRepository
from tests.test_er_dose_processor import FakeDB
from tests.test_er_dose_euv_processor import FakeDB as EUVFakeDB


def saved_plan(raw=True, euv=True):
    return [{"target_date": "2026-05-12", "start_time": "2026-05-12T00:00:00",
             "end_time": "2026-05-13T00:00:00", "raw": raw, "euv": euv}]


@pytest.mark.parametrize("source,target,distinct,expected", [
    (0, 0, None, False), (2, 2, None, False), (2, 1, 1, False),
    (3, 1, 2, True), (1, 0, None, True), (0, 1, 0, True),
])
def test_planner_preserves_count_and_distinct_rules(source, target, distinct, expected):
    repo = Mock()
    repo.fetch_source_count.side_effect = [source, distinct]
    repo.fetch_target_count.return_value = target
    assert jobs.needs_reload(repo, date(2026, 5, 12)) is expected
    assert repo.fetch_source_count.call_count == (2 if source != target and target > 0 else 1)
    if repo.fetch_source_count.call_count == 2:
        assert repo.fetch_source_count.call_args.kwargs == {"distinct": True}


def test_existing_date_xcom_is_used_without_another_lookback(monkeypatch):
    monkeypatch.setattr(jobs, "PostgresDB", Mock())
    raw, euv = Mock(), Mock()
    monkeypatch.setattr(jobs, "ERDoseRepository", Mock(return_value=raw))
    monkeypatch.setattr(jobs, "ERDoseEUVRepository", Mock(return_value=euv))
    raw.fetch_source_count.side_effect = lambda day, **kw: int(day == date(2026, 5, 12))
    raw.fetch_target_count.return_value = 0
    euv.fetch_source_count.return_value = euv.fetch_target_count.return_value = 0
    ti = Mock()
    ti.xcom_pull.return_value = "2026-05-12"
    assert jobs.plan_dates(ti, "existing_date") is None
    ti.xcom_pull.assert_called_once_with(task_ids="existing_date", key="date")
    stored = {call.kwargs["key"]: call.kwargs["value"] for call in ti.xcom_push.call_args_list}
    assert set(stored) == {"ER_DOSE_PLAN"}
    plan = stored["ER_DOSE_PLAN"]
    assert len(plan) == 1
    assert plan[0]["target_date"] == "2026-05-12"
    assert plan[-1]["target_date"] == "2026-05-12"
    assert [day for day in plan if day["raw"]] == saved_plan(raw=True, euv=False)
    import json
    assert json.loads(json.dumps(plan)) == plan
    raw.truncate_target_partition.assert_not_called()


@pytest.mark.parametrize("processor_type,repository_type,db_type", [
    (ERDoseProcessor, ERDoseRepository, FakeDB),
    (ERDoseEUVProcessor, ERDoseEUVRepository, EUVFakeDB),
    (LegacyEUVProcessor, ERDoseEUVRepository, EUVFakeDB),
])
def test_parser_requires_explicit_period_and_date_retry_does_not_recheck_counts(processor_type, repository_type, db_type):
    db = db_type(pd.DataFrame())
    repo = repository_type(db)
    repo.fetch_source_count = Mock(side_effect=AssertionError("planner only"))
    repo.fetch_target_count = Mock(side_effect=AssertionError("planner only"))
    processor = processor_type(repo)
    with pytest.raises(ValueError, match="start_time and end_time"):
        processor.run()
    for _ in range(2):
        processor.run(target_date=date(2026, 5, 12))
    assert len([q for q, _, _ in db.executed if q.strip().lower().startswith("truncate")]) == 2
    assert not any("de_trend_" in q or "equipment_count_log" in q for q, _, _ in db.executed)


@pytest.mark.parametrize("job_type,method", [("die_yield", "die_yield_daily_summary"), ("root_cause", "root_cause_daily_summary")])
def test_report_retry_restarts_delete_without_reparsing(monkeypatch, job_type, method):
    db_factory = Mock()
    monkeypatch.setattr(jobs, "PostgresDB", db_factory)
    repo = Mock()
    monkeypatch.setattr(jobs, "ERDoseRepository", Mock(return_value=repo))
    events = []
    getattr(repo, "delete_" + method).side_effect = lambda *a: events.append(("delete", a))
    def insert(*args):
        events.append(("insert", args))
        if len(events) == 2:
            raise RuntimeError("admin terminated connection")
    getattr(repo, "insert_" + method).side_effect = insert
    ti = Mock()
    ti.xcom_pull.return_value = saved_plan()
    with pytest.raises(RuntimeError):
        jobs.report_day(job_type, 0, ti, plan_task_id="parse_er_data_raw")
    jobs.report_day(job_type, 0, ti, plan_task_id="parse_er_data_raw")
    assert [action for action, _ in events] == ["delete", "insert", "delete", "insert"]
    assert all(bounds == (datetime(2026, 5, 12), datetime(2026, 5, 13)) for _, bounds in events)
    assert db_factory.call_count == 2
    repo.transaction.assert_not_called()
    repo.fetch_source_count.assert_not_called()


def test_failed_delete_does_not_insert(monkeypatch):
    monkeypatch.setattr(jobs, "PostgresDB", Mock())
    repo = Mock()
    repo.delete_die_yield_daily_summary.side_effect = RuntimeError("delete failed")
    monkeypatch.setattr(jobs, "ERDoseRepository", Mock(return_value=repo))
    ti = Mock()
    ti.xcom_pull.return_value = saved_plan()
    with pytest.raises(RuntimeError):
        jobs.report_day("die_yield", 0, ti, plan_task_id="parse_er_data_raw")
    repo.insert_die_yield_daily_summary.assert_not_called()


def test_called_report_executes_without_checking_selection_flags(monkeypatch):
    monkeypatch.setattr(jobs, "PostgresDB", Mock())
    repo = Mock()
    monkeypatch.setattr(jobs, "ERDoseRepository", Mock(return_value=repo))
    ti = Mock()
    # Execution functions need only the saved period, not selection flags.
    window = saved_plan()[0]
    del window["raw"], window["euv"]
    ti.xcom_pull.return_value = [window]
    jobs.report_day("die_yield", 0, ti, plan_task_id="parse_er_data_raw")
    repo.insert_die_yield_daily_summary.assert_called_once_with(datetime(2026, 5, 12), datetime(2026, 5, 13))
    ti.xcom_pull.assert_called_once_with(task_ids="parse_er_data_raw", key="ER_DOSE_PLAN")


def test_missing_plan_fails_instead_of_recomputing(monkeypatch):
    monkeypatch.setattr(jobs, "PostgresDB", Mock(side_effect=AssertionError("do not replan")))
    ti = Mock()
    ti.xcom_pull.return_value = None
    with pytest.raises(ValueError, match="plan is missing"):
        jobs.report_day("raw_count", 0, ti, plan_task_id="parse_er_data_raw")


def test_statistics_query_failure_keeps_existing_snapshot(monkeypatch):
    repo = Mock()
    repo.fetch_equipment_counts.side_effect = RuntimeError("select failed")
    with pytest.raises(RuntimeError):
        jobs.write_equipment_count_log(repo, datetime(2026, 5, 12), datetime(2026, 5, 13))
    repo.delete_equipment_count.assert_not_called()
    repo.insert_equipment_count.assert_not_called()


@pytest.mark.parametrize("job,table", [
    ("raw_count", "mbeat.er_dose_raw_equipment_count_log"),
    ("euv_count", "mbeat.er_dose_euv_equipment_count_log"),
])
def test_statistics_retry_replaces_only_selected_date_snapshot(monkeypatch, job, table):
    day = date(2026, 5, 12)
    other_day = date(2026, 5, 11)
    records = {(table, other_day): [{"eq_name": "KEEP"}]}
    db = Mock()
    db.select.return_value = pd.DataFrame([
        {"eq_name": "EQ1", "source_count": 3, "target_count": 2},
        {"eq_name": "EQ2", "source_count": 5, "target_count": 5},
    ])
    events = []
    def execute(query, params):
        assert f"delete from {table}" in query
        assert params == {"target_date": day}
        events.append("delete")
        records.pop((table, day), None)
    def bulk_insert(table_name, df):
        assert table_name == table
        events.append("insert")
        records.setdefault((table, day), []).extend(df.to_dict("records"))
        if bulk_insert.fail:
            bulk_insert.fail = False
            raise RuntimeError("worker lost after commit")
    bulk_insert.fail = True
    db.execute.side_effect = execute
    db.bulk_insert_df.side_effect = bulk_insert
    monkeypatch.setattr(jobs, "PostgresDB", Mock(return_value=db))
    ti = Mock()
    ti.xcom_pull.return_value = saved_plan()
    with pytest.raises(RuntimeError, match="worker lost"):
        jobs.report_day(job, 0, ti, plan_task_id="parse_er_data_raw")
    jobs.report_day(job, 0, ti, plan_task_id="parse_er_data_raw")
    assert events == ["delete", "insert", "delete", "insert"]
    assert len(records[(table, day)]) == 2
    assert records[(table, other_day)] == [{"eq_name": "KEEP"}]
    db.transaction.assert_not_called()
    db.select.return_value = pd.DataFrame()
    jobs.report_day(job, 0, ti, plan_task_id="parse_er_data_raw")
    assert (table, day) not in records
    assert events[-1] == "delete"


def test_statistics_delete_failure_prevents_insert():
    repo = Mock()
    repo.fetch_equipment_counts.return_value = [{"eq_name": "EQ1", "source_count": 1, "target_count": 1}]
    repo.delete_equipment_count.side_effect = RuntimeError("delete failed")
    with pytest.raises(RuntimeError, match="delete failed"):
        jobs.write_equipment_count_log(repo, datetime(2026, 5, 12), datetime(2026, 5, 13))
    repo.insert_equipment_count.assert_not_called()


@pytest.mark.parametrize("value", [None, [], "2026-5-12", "2026-02-30", "2026-05-12T00:00:00"])
def test_invalid_date_xcom_fails_before_db_access(monkeypatch, value):
    db = Mock()
    monkeypatch.setattr(jobs, "PostgresDB", db)
    ti = Mock()
    ti.xcom_pull.return_value = value
    with pytest.raises(ValueError):
        jobs.plan_dates(ti, "existing_date")
    db.assert_not_called()
    ti.xcom_push.assert_not_called()
