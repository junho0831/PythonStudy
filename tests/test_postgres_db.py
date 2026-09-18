from __future__ import annotations

import unittest

import pandas as pd

from er_dose.infra.postgres_db import PostgresDB


class FakeCursor:
    def __init__(self):
        self.executed = []
        self.copy_query = None
        self.copy_payload = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def copy_expert(self, query, file):
        self.copy_query = query
        self.copy_payload = file.getvalue()

    def close(self):
        pass


class FakeConnection:
    def __init__(self):
        self.cursor_obj = FakeCursor()
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


class PostgresDBTest(unittest.TestCase):
    def test_copy_insert_to_partition_table_keeps_duplicate_rows(self):
        db = PostgresDB(dsn="postgresql://user:password@localhost:5432/db")
        connection = FakeConnection()
        db._PostgresDB__engine = type("FakeEngine", (), {"raw_connection": lambda _: connection})()
        df = pd.DataFrame(
            [
                {"eq_name": "EQ1", "use_yn": "Y"},
                {"eq_name": "EQ1", "use_yn": "Y"},
            ]
        )

        db.copy_insert_to_partition_table(
            schema="prism_common",
            table_name="er_dose_raw_parsed",
            target_date="2026-07-20",
            df=df,
        )

        self.assertEqual(
            connection.cursor_obj.copy_query,
            "COPY prism_common.er_dose_raw_parsed_1_prt_p20260720 FROM STDIN WITH CSV HEADER",
        )
        self.assertEqual(connection.cursor_obj.copy_payload.count("EQ1,Y"), 2)
        self.assertIn(
            ("ANALYZE prism_common.er_dose_raw_parsed_1_prt_p20260720", None),
            connection.cursor_obj.executed,
        )
        self.assertTrue(connection.committed)

    def test_copy_insert_to_partition_table_can_skip_analyze(self):
        db = PostgresDB(dsn="postgresql://user:password@localhost:5432/db")
        connection = FakeConnection()
        db._PostgresDB__engine = type("FakeEngine", (), {"raw_connection": lambda _: connection})()
        df = pd.DataFrame([{"eq_name": "EQ1"}])

        db.copy_insert_to_partition_table(
            schema="prism_common",
            table_name="er_dose_raw_parsed",
            target_date="2026-07-20",
            df=df,
            analyze=False,
        )

        self.assertNotIn(
            ("ANALYZE prism_common.er_dose_raw_parsed_1_prt_p20260720", None),
            connection.cursor_obj.executed,
        )


if __name__ == "__main__":
    unittest.main()


def test_execute_adapts_named_parameters_without_interpolating_values():
    from datetime import date

    db = PostgresDB(dsn='postgresql://unused')
    connection = FakeConnection()
    connection.cursor_obj.rowcount = 2
    value = "x'; DROP TABLE example; --"
    result = db.execute(
        "update example set value = :value where day::date = :day and label like '%test%'",
        {'value': value, 'day': date(2026, 5, 1)}, connection=connection,
    )
    query, params = connection.cursor_obj.executed[0]
    assert query == "update example set value = %(value)s where day::date = %(day)s and label like '%%test%%'"
    assert params == {'value': value, 'day': date(2026, 5, 1)}
    assert value not in query
    assert result == 2
    assert not connection.committed
    assert not connection.closed


def test_execute_preserves_native_driver_parameters():
    db = PostgresDB(dsn='postgresql://unused')
    connection = FakeConnection()
    connection.cursor_obj.rowcount = 1
    query = 'delete from example where id = %(id)s'
    db.execute(query, {'id': 7}, connection=connection)
    assert connection.cursor_obj.executed == [(query, {'id': 7})]


def test_bulk_insert_df_binds_rows_and_commits_once(monkeypatch):
    from unittest.mock import Mock

    db = PostgresDB(dsn='postgresql://unused')
    connection = FakeConnection()
    monkeypatch.setattr(db, '_connect_raw', lambda: connection)
    execute_values = Mock()
    monkeypatch.setattr('psycopg2.extras.execute_values', execute_values)
    df = pd.DataFrame([{'eq_name': "EQ'한글%", 'count': 2**40}, {'eq_name': None, 'count': None}])
    assert db.bulk_insert_df('mbeat.example', df) == 2
    execute_values.assert_called_once()
    cursor, query, rows = execute_values.call_args.args
    assert cursor is connection.cursor_obj
    assert query == 'insert into "mbeat"."example" ("eq_name", "count") values %s'
    assert rows == [("EQ'한글%", 2**40), (None, None)]
    assert connection.committed and connection.closed
    assert not connection.rolled_back


def test_bulk_insert_df_rolls_back_owned_connection_on_error(monkeypatch):
    from unittest.mock import Mock
    import pytest

    db = PostgresDB(dsn='postgresql://unused')
    connection = FakeConnection()
    monkeypatch.setattr(db, '_connect_raw', lambda: connection)
    monkeypatch.setattr('psycopg2.extras.execute_values', Mock(side_effect=RuntimeError('insert failed')))
    with pytest.raises(RuntimeError, match='insert failed'):
        db.bulk_insert_df('example', pd.DataFrame([{'value': 1}]))
    assert connection.rolled_back and connection.closed
    assert not connection.committed


def test_bulk_insert_df_empty_and_caller_connection(monkeypatch):
    from unittest.mock import Mock

    db = PostgresDB(dsn='postgresql://unused')
    connect = Mock()
    monkeypatch.setattr(db, '_connect_raw', connect)
    execute_values = Mock()
    monkeypatch.setattr('psycopg2.extras.execute_values', execute_values)
    assert db.bulk_insert_df('example', pd.DataFrame()) == 0
    connect.assert_not_called()
    execute_values.assert_not_called()
    connection = FakeConnection()
    assert db.bulk_insert_df('example', pd.DataFrame([{'value': 1}]), connection=connection) == 1
    connect.assert_not_called()
    assert not connection.committed
    assert not connection.rolled_back
    assert not connection.closed
