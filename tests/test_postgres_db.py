from __future__ import annotations

import unittest

import pandas as pd

from er_dose.infra.postgres_db import PostgresDB


class FakeCursor:
    def __init__(self):
        self.executed = []
        self.copy_query = None
        self.copy_payload = None
        self.rowcount = 0

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
    def test_copy_insert_to_partition_table_keeps_header_copy_and_analyze(self):
        db = PostgresDB(dsn="postgresql://user:password@localhost:5432/db")
        connection = FakeConnection()
        db._PostgresDB__engine = type("FakeEngine", (), {"raw_connection": lambda _: connection})()
        df = pd.DataFrame([{"eq_name": "EQ1", "use_yn": "Y"}])

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
        self.assertIn("eq_name,use_yn", connection.cursor_obj.copy_payload)
        self.assertIn("EQ1,Y", connection.cursor_obj.copy_payload)
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

    def test_analyze_partition_table_runs_analyze_only(self):
        db = PostgresDB(dsn="postgresql://user:password@localhost:5432/db")
        connection = FakeConnection()

        db.analyze_partition_table(
            schema="prism_common",
            table_name="er_dose_raw_parsed",
            target_date="2026-07-20",
            connection=connection,
        )

        self.assertEqual(
            connection.cursor_obj.executed,
            [("ANALYZE prism_common.er_dose_raw_parsed_1_prt_p20260720", None)],
        )


if __name__ == "__main__":
    unittest.main()
