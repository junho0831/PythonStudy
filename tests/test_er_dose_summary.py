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


def test_summary_failure_stops_following_summary_and_statistics(capsys):
    db = FakeDB(pd.DataFrame(), equipment_counts={date(2026, 5, 1): [{"eq_name": "EQ1", "source_count": 3, "target_count": 2}]})
    repo = ERDoseRepository(db)
    repo.insert_die_yield_daily_summary = Mock(side_effect=RuntimeError('summary failed'))
    repo.insert_root_cause_daily_summary = Mock()
    with pytest.raises(RuntimeError, match='summary failed'):
        ERDoseProcessor(repo).run(start_time=datetime(2026, 5, 1), end_time=datetime(2026, 5, 2))
    logs = db.bulk_inserted
    assert logs == []
    repo.insert_root_cause_daily_summary.assert_not_called()
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
    repo.insert_equipment_count = Mock(side_effect=lambda **k: events.append('log_insert'))
    processor = processor_type(repo)
    processor._reload_target_date = Mock(side_effect=lambda **k: events.append('reload_and_summary') or 2)
    processor.run(lookback_days=1, reference_date=date(2026, 5, 1))
    assert events == ['source', 'target', 'source', 'reload_and_summary']
    repo.fetch_equipment_counts.assert_not_called()
    repo.insert_equipment_count.assert_not_called()
    assert repo.fetch_source_count.call_args_list[1].kwargs == {'distinct': True}


def test_reload_processor_does_not_query_or_write_reporting_counts():
    repo = ERDoseRepository(FakeDB(pd.DataFrame()))
    repo.fetch_source_count = Mock(return_value=1)
    repo.fetch_target_count = Mock(return_value=0)
    repo.fetch_equipment_counts = Mock()
    repo.insert_equipment_count = Mock()
    processor = ERDoseProcessor(repo)
    processor._reload_target_date = Mock(return_value=1)
    processor.run_recent_days(lookback_days=1, reference_date=date(2026, 5, 1))
    processor._reload_target_date.assert_called_once()
    repo.fetch_equipment_counts.assert_not_called()
    repo.insert_equipment_count.assert_not_called()


def test_raw_logs_both_batches_with_unchanged_time_bounds():
    db = FakeDB(pd.DataFrame(), equipment_counts={date(2026, 5, 1): [{"eq_name": "EQ1", "source_count": 3, "target_count": 2}]})
    start = datetime(2026, 5, 1, 12, 34, 56)
    end = datetime(2026, 5, 3, 15, 16, 17)
    ERDoseProcessor(ERDoseRepository(db)).run(start_time=start, end_time=end)
    summaries = [(q, p) for q, p, _ in db.executed if 'de_trend_' in q]
    assert len(summaries) == 4
    assert all(p == {'start_time': start, 'end_time': end} for _, p in summaries)
    assert {table for table, _ in db.bulk_inserted} == {
        'mbeat.er_dose_raw_equipment_count_log',
        'mbeat.er_dose_euv_equipment_count_log',
    }
    assert len(db.bulk_inserted) == 2
    for _, df in db.bulk_inserted:
        assert df.to_dict('records') == [{
            'target_date': start.date(), 'eq_name': 'EQ1',
            'source_count': 3, 'target_count': 2,
        }]


def test_log_insert_has_no_return_value():
    from er_dose.common.equipment_count_repository import insert_equipment_count

    db = Mock()
    db.bulk_insert_df.return_value = 7
    assert insert_equipment_count(db, 'mbeat.er_dose_raw_equipment_count_log', date(2026, 5, 1), [{'eq_name': 'EQ1', 'source_count': 3, 'target_count': 2}]) is None


@pytest.mark.parametrize('repository_type,table', [
    (ERDoseRepository, 'mbeat.er_dose_raw_equipment_count_log'),
    (ERDoseEUVRepository, 'mbeat.er_dose_euv_equipment_count_log'),
])
def test_eighty_equipment_rows_use_one_parameterized_insert(repository_type, table):
    db = Mock()
    repo = repository_type(db)
    rows = [{'eq_name': f"EQ'{i}%", 'source_count': 2**40 + i, 'target_count': i} for i in range(80)]
    assert repo.insert_equipment_count(date(2026, 5, 1), rows) is None
    db.bulk_insert_df.assert_called_once()
    actual_table, df = db.bulk_insert_df.call_args.args
    assert actual_table == table
    assert df.to_dict('records') == [
        {'target_date': date(2026, 5, 1), **row} for row in rows
    ]
    db.execute.assert_not_called()
    db.reset_mock()
    repo.insert_equipment_count(date(2026, 5, 1), [])
    db.bulk_insert_df.assert_not_called()


@pytest.mark.parametrize('repository_type', [ERDoseRepository, ERDoseEUVRepository])
def test_equipment_counts_return_rows_without_json_functions(repository_type):
    db = Mock()
    db.select.return_value = pd.DataFrame([
        {'eq_name': 'RAW_ONLY', 'source_count': 5, 'target_count': 0},
        {'eq_name': 'TARGET_ONLY', 'source_count': 0, 'target_count': 7},
    ])
    repo = repository_type(db)
    rows = repo.fetch_equipment_counts(datetime(2026, 5, 1), datetime(2026, 5, 2))
    assert rows == db.select.return_value.to_dict('records')
    query = db.select.call_args.args[0].lower()
    assert 'jsonb_build_object' not in query and 'jsonb_agg' not in query
    assert 'group by r.eq_name' in query and 'group by p.eq_name' in query
    assert 'full outer join' in query
    db.select.return_value = pd.DataFrame()
    assert repo.fetch_equipment_counts(datetime(2026, 5, 1), datetime(2026, 5, 2)) == []


def test_empty_equipment_counts_do_not_insert_anonymous_log():
    from er_dose.common.equipment_count_repository import write_equipment_count_log

    repo = Mock()
    repo.fetch_equipment_counts.return_value = []
    write_equipment_count_log(repo, datetime(2026, 5, 1), datetime(2026, 5, 2))
    repo.insert_equipment_count.assert_not_called()


def test_final_tasks_run_sequentially_on_calling_thread(monkeypatch):
    from threading import get_ident

    repo = ERDoseRepository(FakeDB(pd.DataFrame()))
    calling_thread = get_ident()
    completed = []
    start = datetime(2026, 5, 1)
    end = datetime(2026, 5, 2)

    def run_task(name, start_time, end_time):
        assert (start_time, end_time) == (start, end)
        assert get_ident() == calling_thread
        completed.append(name)

    repo.insert_die_yield_daily_summary = lambda *args: run_task('yield', *args)
    repo.insert_root_cause_daily_summary = lambda *args: run_task('root', *args)

    def write_log(repository, start_time, end_time):
        run_task(type(repository).__name__, start_time, end_time)

    monkeypatch.setattr('er_dose.raw.raw_processor.write_equipment_count_log', write_log)
    ERDoseProcessor(repo).run(start_time=start, end_time=end)
    assert completed == ['yield', 'root', 'ERDoseRepository', 'ERDoseEUVRepository']


def test_statistics_failure_prevents_successful_raw_completion(monkeypatch, capsys):
    repo = ERDoseRepository(FakeDB(pd.DataFrame()))
    repo.insert_die_yield_daily_summary = Mock()
    repo.insert_root_cause_daily_summary = Mock()

    def write_log(repository, start_time, end_time):
        if isinstance(repository, ERDoseEUVRepository):
            raise RuntimeError('statistics failed')

    monkeypatch.setattr('er_dose.raw.raw_processor.write_equipment_count_log', write_log)
    with pytest.raises(RuntimeError, match='statistics failed'):
        ERDoseProcessor(repo).run(start_time=datetime(2026, 5, 1), end_time=datetime(2026, 5, 2))
    assert '[ER_DOSE] done' not in capsys.readouterr().out


def test_raw_deletes_before_final_inserts(monkeypatch):
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
