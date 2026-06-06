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
    def test_copy_insert_to_partition_table_truncates_copies_and_analyzes(self):
        conn = FakeConnection()
        db = PostgresDB(dsn="postgresql://example")
        db._connect = lambda: conn
        df = pd.DataFrame(
            [
                {"code_occur_time": "2026-06-01 10:00:00.000000", "eq_name": "EQ1"},
                {"code_occur_time": "2026-06-01 10:00:00.000000", "eq_name": "EQ1"},
                {"code_occur_time": "2026-06-01 10:00:01.000000", "eq_name": None},
            ]
        )

        db.copy_insert_to_partition_table(
            schema="mbeat",
            table_name="er_dose_error_parsed",
            target_date="2026-06-01",
            df=df,
            is_truncate=True,
        )

        partition_sql = '"mbeat"."er_dose_error_parsed_1_prt_p20260601"'
        self.assertEqual(conn.cursor_obj.executed[0], (f"truncate table {partition_sql}", None))
        self.assertEqual(conn.cursor_obj.executed[1], (f"analyze {partition_sql}", None))
        self.assertEqual(
            conn.cursor_obj.copy_query,
            f"copy {partition_sql} from stdin with csv header null ''",
        )
        self.assertEqual(
            conn.cursor_obj.copy_payload,
            "code_occur_time,eq_name\n"
            "2026-06-01 10:00:00.000000,EQ1\n"
            "2026-06-01 10:00:01.000000,\n",
        )
        self.assertTrue(conn.committed)
        self.assertFalse(conn.rolled_back)
        self.assertTrue(conn.closed)

    def test_copy_insert_to_partition_table_skips_empty_dataframe(self):
        conn = FakeConnection()
        db = PostgresDB(dsn="postgresql://example")
        db._connect = lambda: conn

        db.copy_insert_to_partition_table(
            schema="mbeat",
            table_name="er_dose_error_parsed",
            target_date="2026-06-01",
            df=pd.DataFrame(),
            is_truncate=True,
        )

        self.assertEqual(conn.cursor_obj.executed, [])
        self.assertFalse(conn.committed)
        self.assertFalse(conn.closed)


if __name__ == "__main__":
    unittest.main()
