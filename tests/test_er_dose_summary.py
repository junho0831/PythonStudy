from contextlib import contextmanager
from datetime import date, datetime
from unittest.mock import Mock

import pandas as pd
import pytest

from er_dose.euv.euv_processor import ERDoseEUVProcessor
from er_dose.euv.euv_repository import ERDoseEUVRepository
from er_dose.raw.raw_processor import ERDoseProcessor, ERDoseEUVProcessor as LegacyEUVProcessor
from er_dose.raw.raw_repository import ERDoseRepository
from tests.test_er_dose_processor import FakeDB


@pytest.mark.parametrize('processor_type,repository_type,tables', [
    (ERDoseProcessor, ERDoseRepository, ['de_trend_die_yield_daily', 'de_trend_root_cause_daily']),
    (ERDoseEUVProcessor, ERDoseEUVRepository, ['de_trend_root_cause_daily']),
    (LegacyEUVProcessor, ERDoseEUVRepository, ['de_trend_root_cause_daily']),
])
def test_empty_reload_rebuilds_summary(processor_type, repository_type, tables):
    target_date = date(2026, 5, 1)
    db = FakeDB(pd.DataFrame(), source_counts={target_date: 0}, target_counts={target_date: 1})
    processor_type(repository_type(db)).run_recent_days(lookback_days=1, reference_date=target_date)
    for table in tables:
        statements = [(q, p) for q, p, _ in db.executed if table in q]
        assert len(statements) == 2
        assert statements[0][0].strip().lower().startswith('delete')
        assert statements[0][1]['target_date'] == target_date
        assert statements[1][0].strip().lower().startswith('insert')


def test_empty_window_includes_each_day_but_excludes_end_midnight():
    db = FakeDB(pd.DataFrame())
    ERDoseProcessor(ERDoseRepository(db)).run(
        start_time=datetime(2026, 5, 1, 12), end_time=datetime(2026, 5, 3),
    )
    dates = [p['target_date'] for q, p, _ in db.executed
             if q.strip().lower().startswith('delete') and 'de_trend_die_yield_daily' in q]
    assert dates == [date(2026, 5, 1), date(2026, 5, 2)]


@pytest.mark.parametrize('repository_type,method', [
    (ERDoseRepository, 'replace_die_yield_daily_summary'),
    (ERDoseRepository, 'replace_root_cause_daily_summary'),
    (ERDoseEUVRepository, 'replace_root_cause_daily_summary'),
])
def test_summary_binds_parameters_and_rolls_back_delete_on_insert_failure(repository_type, method):
    db = Mock()
    connection = object()
    events = []

    @contextmanager
    def transaction():
        try:
            yield connection
        except RuntimeError:
            events.append('rollback')
            raise
        else:
            events.append('commit')

    def execute(query, params, connection):
        # Exercise pyformat binding, including literal percent signs in LIKE.
        rendered = query % params
        assert ':target_date' not in rendered
        assert ':start_time' not in rendered
        assert ':end_time' not in rendered
        events.append(query.strip().split()[0].lower())
        if events[-1] == 'insert':
            raise RuntimeError('insert failed')

    db.transaction.side_effect = transaction
    db.execute.side_effect = execute
    with pytest.raises(RuntimeError, match='insert failed'):
        getattr(repository_type(db), method)('2026-05-01')
    assert events == ['delete', 'insert', 'rollback']
    assert all(call.kwargs['connection'] is connection for call in db.execute.call_args_list)


def test_raw_and_euv_use_identical_root_cause_aggregation():
    raw_db, euv_db = FakeDB(pd.DataFrame()), FakeDB(pd.DataFrame())
    ERDoseRepository(raw_db).replace_root_cause_daily_summary('2026-05-01')
    ERDoseEUVRepository(euv_db).replace_root_cause_daily_summary('2026-05-01')
    assert [(q, p) for q, p, _ in raw_db.executed] == [(q, p) for q, p, _ in euv_db.executed]
