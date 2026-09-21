from unittest.mock import Mock
import pytest
from airflow_modules.er_dose_dates import prepare_single_date


def task(target):
    ti = Mock()
    saved = {'date': target, 'ER_DOSE_COMMANDS': {
        'ER_DOSE_EUV': 'euv --date {target_date}', 'ER_DOSE_RAW': 'raw --date {target_date}'}}
    ti.xcom_pull.side_effect = lambda task_ids, key: saved[key]
    return ti


def test_date_and_commands_come_from_start_xcom():
    ti = task('2026-05-13')
    for _ in range(2):
        assert prepare_single_date(ti=ti) == '2026-05-13'
    assert [call.kwargs['value'] for call in ti.xcom_push.call_args_list] == [
        '2026-05-13', 'euv --date 2026-05-13', 'raw --date 2026-05-13'] * 2


def test_missing_date_fails_without_recomputing():
    with pytest.raises(ValueError, match='missing'):
        prepare_single_date(ti=task(None))


@pytest.mark.parametrize('target', ['2026-5-12', '2026-02-30'])
def test_invalid_date(target):
    with pytest.raises(ValueError):
        prepare_single_date(ti=task(target))
