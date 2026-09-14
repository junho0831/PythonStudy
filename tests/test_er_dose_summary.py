from datetime import date, datetime
from unittest.mock import Mock

import pandas as pd
import pytest

from er_dose.euv.euv_processor import ERDoseEUVProcessor
from er_dose.euv.euv_repository import ERDoseEUVRepository
from er_dose.raw.raw_processor import ERDoseProcessor, ERDoseEUVProcessor as LegacyEUVProcessor
from er_dose.raw.raw_repository import ERDoseRepository
from tests.test_er_dose_processor import FakeDB
from tests.test_er_dose_euv_processor import FakeDB as EUVFakeDB


@pytest.mark.parametrize('processor_type,repository_type,tables', [
    (ERDoseProcessor, ERDoseRepository, ['de_trend_die_yield_daily', 'de_trend_root_cause_daily']),
])
def test_empty_reload_rebuilds_summary(processor_type, repository_type, tables):
    target_date = date(2026, 5, 1)
    db_type = FakeDB if repository_type is ERDoseRepository else EUVFakeDB
    db = db_type(pd.DataFrame(), source_counts={target_date: 0}, target_counts={target_date: 1})
    processor_type(repository_type(db)).run_recent_days(lookback_days=1, reference_date=target_date)
    for table in tables:
        statements = [(q, p) for q, p, _ in db.executed if table in q]
        assert len(statements) == 2
        assert statements[0][0].strip().lower().startswith('delete')
        assert statements[0][1] == {'start_time': datetime(2026, 5, 1), 'end_time': datetime(2026, 5, 2)}
        assert statements[1][0].strip().lower().startswith('insert')


def test_empty_window_includes_each_day_but_excludes_end_midnight():
    db = FakeDB(pd.DataFrame())
    ERDoseProcessor(ERDoseRepository(db)).run(
        start_time=datetime(2026, 5, 1, 12), end_time=datetime(2026, 5, 3),
    )
    windows = [p for q, p, _ in db.executed
             if q.strip().lower().startswith('delete') and 'de_trend_die_yield_daily' in q]
    assert windows == [{'start_time': datetime(2026, 5, 1, 12), 'end_time': datetime(2026, 5, 3)}]


def test_summary_failure_still_allows_statistics_but_not_raw_success(capsys):
    import json

    db = FakeDB(pd.DataFrame())
    repo = ERDoseRepository(db)
    repo.insert_die_yield_daily_summary = Mock(side_effect=RuntimeError('summary failed'))
    repo.insert_root_cause_daily_summary = Mock()
    with pytest.raises(RuntimeError, match='summary failed'):
        ERDoseProcessor(repo).run(start_time=datetime(2026, 5, 1), end_time=datetime(2026, 5, 2))
    logs = [params for query, params, _ in db.executed if 'batch_event_log' in query]
    assert len(logs) == 2
    assert all(json.loads(log['data'])['action'] == 'STATISTICS_RECORDED' for log in logs)
    assert '[ER_DOSE] done' not in capsys.readouterr().out


@pytest.mark.parametrize('processor_type,repository_type', [
    (ERDoseProcessor, ERDoseRepository),
    (ERDoseEUVProcessor, ERDoseEUVRepository),
])
def test_logging_counts_are_separate_from_existing_reload_decision(processor_type, repository_type):
    repo = repository_type(FakeDB(pd.DataFrame()))
    events = []
    repo.fetch_source_count = Mock(side_effect=lambda *a, **k: events.append('source') or 2)
    repo.fetch_target_count = Mock(side_effect=lambda *a: events.append('target') or 1)
    repo.fetch_equipment_counts = Mock(side_effect=lambda *a, **k: events.append('log_counts') or [])
    repo.insert_batch_log = Mock(side_effect=lambda **k: events.append('log_insert'))
    processor = processor_type(repo)
    processor._reload_target_date = Mock(side_effect=lambda **k: events.append('reload_and_summary') or 2)
    processor.run(lookback_days=1, reference_date=date(2026, 5, 1))
    assert events == ['source', 'target', 'source', 'reload_and_summary']
    repo.fetch_equipment_counts.assert_not_called()
    repo.insert_batch_log.assert_not_called()
    assert repo.fetch_source_count.call_args_list[1].kwargs == {'distinct': True}


def test_reload_processor_does_not_query_or_write_reporting_counts():
    repo = ERDoseRepository(FakeDB(pd.DataFrame()))
    repo.fetch_source_count = Mock(return_value=1)
    repo.fetch_target_count = Mock(return_value=0)
    repo.fetch_equipment_counts = Mock()
    repo.insert_batch_log = Mock()
    processor = ERDoseProcessor(repo)
    processor._reload_target_date = Mock(return_value=1)
    processor.run_recent_days(lookback_days=1, reference_date=date(2026, 5, 1))
    processor._reload_target_date.assert_called_once()
    repo.fetch_equipment_counts.assert_not_called()
    repo.insert_batch_log.assert_not_called()


def test_raw_logs_both_batches_with_unchanged_time_bounds():
    import json

    db = FakeDB(pd.DataFrame())
    start = datetime(2026, 5, 1, 12, 34, 56)
    end = datetime(2026, 5, 3, 15, 16, 17)
    ERDoseProcessor(ERDoseRepository(db)).run(start_time=start, end_time=end)
    summaries = [(q, p) for q, p, _ in db.executed if 'de_trend_' in q]
    assert len(summaries) == 4
    assert all(p == {'start_time': start, 'end_time': end} for _, p in summaries)
    logs = [p for q, p, _ in db.executed if 'batch_event_log' in q]
    assert sorted(p['batch_name'] for p in logs) == ['ER_DOSE_EUV', 'ER_DOSE_RAW']
    for log in logs:
        data = json.loads(log['data'])
        assert data['start_time'] == str(start)
        assert data['end_time'] == str(end)


def test_log_insert_has_no_return_value():
    from er_dose.common.batch_log_repository import insert_batch_log

    db = Mock()
    db.execute.return_value = 7
    assert insert_batch_log(db, 'ER_DOSE_RAW', date(2026, 5, 1), 'EQUIPMENT_COUNT', '', {}) is None


def test_all_four_final_tasks_start_before_waiting(monkeypatch):
    from threading import Barrier

    repo = ERDoseRepository(FakeDB(pd.DataFrame()))
    rendezvous = Barrier(4, timeout=3)
    completed = []
    start = datetime(2026, 5, 1)
    end = datetime(2026, 5, 2)

    def run_task(name, start_time, end_time):
        assert (start_time, end_time) == (start, end)
        rendezvous.wait()
        completed.append(name)

    repo.insert_die_yield_daily_summary = lambda *args: run_task('yield', *args)
    repo.insert_root_cause_daily_summary = lambda *args: run_task('root', *args)

    def write_log(repository, batch_name, start_time, end_time):
        run_task(batch_name, start_time, end_time)

    monkeypatch.setattr('er_dose.raw.raw_processor.write_equipment_count_log', write_log)
    ERDoseProcessor(repo).run(start_time=start, end_time=end)
    assert sorted(completed) == ['ER_DOSE_EUV', 'ER_DOSE_RAW', 'root', 'yield']


def test_statistics_failure_prevents_successful_raw_completion(monkeypatch, capsys):
    repo = ERDoseRepository(FakeDB(pd.DataFrame()))
    repo.insert_die_yield_daily_summary = Mock()
    repo.insert_root_cause_daily_summary = Mock()

    def write_log(repository, batch_name, start_time, end_time):
        if batch_name == 'ER_DOSE_EUV':
            raise RuntimeError('statistics failed')

    monkeypatch.setattr('er_dose.raw.raw_processor.write_equipment_count_log', write_log)
    with pytest.raises(RuntimeError, match='statistics failed'):
        ERDoseProcessor(repo).run(start_time=datetime(2026, 5, 1), end_time=datetime(2026, 5, 2))
    assert '[ER_DOSE] done' not in capsys.readouterr().out


def test_raw_deletes_before_submitting_final_inserts(monkeypatch):
    events = []
    repo = ERDoseRepository(FakeDB(pd.DataFrame()))
    repo.delete_die_yield_daily_summary = lambda *args: events.append('delete_yield')
    repo.delete_root_cause_daily_summary = lambda *args: events.append('delete_root')

    def insert(*args):
        assert events == ['delete_yield', 'delete_root']

    repo.insert_die_yield_daily_summary = insert
    repo.insert_root_cause_daily_summary = insert
    monkeypatch.setattr('er_dose.raw.raw_processor.write_equipment_count_log', lambda *args: None)
    ERDoseProcessor(repo).run(start_time=datetime(2026, 5, 1), end_time=datetime(2026, 5, 2))


def test_failed_delete_does_not_start_insert():
    repo = ERDoseRepository(FakeDB(pd.DataFrame()))
    repo.delete_die_yield_daily_summary = Mock(side_effect=RuntimeError('delete failed'))
    repo.insert_die_yield_daily_summary = Mock()
    repo.insert_root_cause_daily_summary = Mock()
    with pytest.raises(RuntimeError, match='delete failed'):
        ERDoseProcessor(repo).run(start_time=datetime(2026, 5, 1), end_time=datetime(2026, 5, 2))
    repo.insert_die_yield_daily_summary.assert_not_called()
    repo.insert_root_cause_daily_summary.assert_not_called()


@pytest.mark.parametrize('repository_type', [ERDoseRepository, ERDoseEUVRepository])
def test_equipment_counts_without_database_json_functions(repository_type):
    db = Mock()
    db.select.return_value = pd.DataFrame([
        {'eq_name': 'A', 'source_count': 3, 'target_count': 2, 'total_source_count': 5, 'total_target_count': 5, 'matched': 0},
        {'eq_name': 'B', 'source_count': 2, 'target_count': 3, 'total_source_count': 5, 'total_target_count': 5, 'matched': 0},
    ])
    result = repository_type(db).fetch_equipment_counts(datetime(2026, 5, 1), datetime(2026, 5, 2))
    query = db.select.call_args.args[0].lower()
    assert 'jsonb_build_object' not in query and 'jsonb_agg' not in query
    assert 'over ()' in query
    assert result['source_count'] == result['target_count'] == 5
    assert result['matched'] is False
    assert len(result['equipment_counts']) == 2
