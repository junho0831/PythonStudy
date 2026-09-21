from datetime import date, datetime
from unittest.mock import MagicMock, Mock

import pandas as pd
import pytest

from airflow_modules.er_dose_db import PostgresDB
from airflow_modules.er_dose_repository import ERDoseRepository, ERDoseEUVRepository
from er_dose.raw.raw_repository import ERDoseRepository as ParserRawRepository
from er_dose.euv.euv_repository import ERDoseEUVRepository as ParserEuvRepository


@pytest.mark.parametrize("local,remote", [
    (ERDoseRepository, ParserRawRepository), (ERDoseEUVRepository, ParserEuvRepository),
])
@pytest.mark.parametrize("method,args,kwargs", [
    ("fetch_source_count", (date(2026, 5, 12),), {}),
    ("fetch_source_count", (date(2026, 5, 12),), {"distinct": True}),
    ("fetch_target_count", (date(2026, 5, 12),), {}),
    ("fetch_equipment_counts", (datetime(2026, 5, 12), datetime(2026, 5, 13)), {}),
    ("delete_equipment_count", (date(2026, 5, 12),), {}),
])
def test_dag_queries_preserve_parser_filters_and_bindings(local, remote, method, args, kwargs):
    db = Mock()
    db.select.return_value = pd.DataFrame()
    getattr(local(db), method)(*args, **kwargs)
    expected = db.mock_calls[:]
    db.reset_mock()
    getattr(remote(db), method)(*args, **kwargs)
    assert db.mock_calls == expected


@pytest.mark.parametrize("method", [
    "delete_die_yield_daily_summary", "insert_die_yield_daily_summary",
    "delete_root_cause_daily_summary", "insert_root_cause_daily_summary",
])
def test_dag_summary_queries_are_unchanged(method):
    db = Mock()
    getattr(ERDoseRepository(db), method)(datetime(2026, 5, 12), datetime(2026, 5, 13))
    expected = db.mock_calls[:]
    db.reset_mock()
    getattr(ParserRawRepository(db), method)(datetime(2026, 5, 12), datetime(2026, 5, 13))
    assert db.mock_calls == expected


def test_dag_select_binds_values_and_closes_connection(monkeypatch):
    db = PostgresDB()
    conn = MagicMock()
    cur = conn.cursor.return_value.__enter__.return_value
    cur.description = [("row_count",)]
    cur.fetchall.return_value = [(3,)]
    monkeypatch.setattr(db, "_connect_raw", lambda: conn)
    assert db.select("select :value as row_count where 'NXE1' like 'NXE%'", {"value": 3}).iloc[0]["row_count"] == 3
    cur.execute.assert_called_once_with(
        "select %(value)s as row_count where 'NXE1' like 'NXE%%'", {"value": 3},
    )
    conn.close.assert_called_once()


@pytest.mark.parametrize("fails", [False, True])
def test_dag_execute_commit_or_rollback_and_close(monkeypatch, fails):
    db = PostgresDB()
    conn = MagicMock()
    cur = conn.cursor.return_value.__enter__.return_value
    cur.rowcount = 2
    monkeypatch.setattr(db, "_connect_raw", lambda: conn)
    if fails:
        cur.execute.side_effect = RuntimeError("terminated")
        with pytest.raises(RuntimeError, match="terminated"):
            db.execute("delete from example where day=:day", {"day": date(2026, 5, 12)})
        conn.rollback.assert_called_once()
        conn.commit.assert_not_called()
    else:
        assert db.execute("delete from example where day=:day", {"day": date(2026, 5, 12)}) == 2
        conn.commit.assert_called_once()
        conn.rollback.assert_not_called()
    conn.close.assert_called_once()


def test_empty_bulk_insert_does_not_connect(monkeypatch):
    db = PostgresDB()
    connect = Mock()
    monkeypatch.setattr(db, "_connect_raw", connect)
    assert db.bulk_insert_df("mbeat.example", pd.DataFrame()) == 0
    connect.assert_not_called()
