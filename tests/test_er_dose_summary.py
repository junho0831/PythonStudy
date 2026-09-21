from datetime import date, datetime
from unittest.mock import Mock

import pandas as pd
import pytest

from er_dose.euv.euv_repository import ERDoseEUVRepository
from er_dose.raw.raw_repository import ERDoseRepository


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
    repo.delete_equipment_count.assert_called_once_with(target_date=date(2026, 5, 1))
    repo.insert_equipment_count.assert_not_called()
