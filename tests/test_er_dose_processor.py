from __future__ import annotations

import unittest
from contextlib import redirect_stdout
from datetime import datetime
from io import StringIO

import pandas as pd

from er_dose.processor import ERDoseProcessor
from er_dose.repository import ERDoseRepository


SAMPLE_CONTENTS = """system warning: dw-3411 skip the dose evaluation 0.0461075 [%]
exceeds the dose evaluation warning level 0 [%]
de_err=0.0461075 [%]
de_warn_lvl=0 [%]
eset:89898 [bits]
freq=50000 [hz]
n_slit=44
mb_enabled=t
action_handle=2625
exposure_handle:2631
[dwdc_eval_determine_dose_performance_result:dwdc_warn_total_dose]"""


class FakeTransaction:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, exc, traceback):
        return False


class FakeDB:
    def __init__(self, raw_df):
        self.raw_df = raw_df
        self.executed = []
        self.inserted = []
        self.connection = object()

    def fetch_df(self, query, params=None):
        self.fetch_query = query
        self.fetch_params = params
        return self.raw_df

    def fetch_df_in_chunks(self, query, params=None, chunk_size=10000):
        self.fetch_query = query
        self.fetch_params = params
        for start in range(0, len(self.raw_df), chunk_size):
            yield self.raw_df.iloc[start : start + chunk_size].copy()

    def transaction(self):
        return FakeTransaction(self.connection)

    def execute(self, query, params=None, connection=None):
        self.executed.append((query, params, connection))
        return 0

    def copy_insert_df(self, table_name, df, connection=None):
        self.insert_table_name = table_name
        self.inserted.append((table_name, df))
        self.insert_connection = connection
        return len(df)

    def copy_insert_to_partition_table(self, schema, table_name, target_date, df, is_truncate=False):
        full_table_name = f"{schema}.{table_name}"
        self.inserted.append((full_table_name, df))
        return len(df)


class ERDoseProcessorTest(unittest.TestCase):
    def test_fetch_raw_logs_uses_general_raw_table_and_code_occur_time_range(self):
        db = FakeDB(pd.DataFrame())
        repo = ERDoseRepository(db)
        start_time = datetime(2026, 5, 1)
        end_time = datetime(2026, 5, 2)

        list(repo.fetch_raw_logs_in_chunks(start_time=start_time, end_time=end_time, limit=10, chunk_size=100))

        self.assertIn("from mbeat.er_data_raw r", db.fetch_query)
        self.assertIn("r.er_date", db.fetch_query)
        self.assertIn("r.er_index", db.fetch_query)
        self.assertIn('r."type" as type', db.fetch_query)
        self.assertIn("r.title", db.fetch_query)
        self.assertIn("r.code_occur_time >= %(start_time)s", db.fetch_query)
        self.assertIn("r.code_occur_time < %(end_time)s", db.fetch_query)
        self.assertIn("r.code in", db.fetch_query)
        self.assertIn("'DW-3411'", db.fetch_query)
        self.assertIn("'DW-3425'", db.fetch_query)
        self.assertIn("'DW-343A'", db.fetch_query)
        self.assertIn("'DW-343B'", db.fetch_query)
        self.assertIn("'LO-0061'", db.fetch_query)
        self.assertIn("'LO-8166'", db.fetch_query)
        self.assertIn("'LO-8167'", db.fetch_query)
        self.assertIn("'KE-9103'", db.fetch_query)
        self.assertIn("'KE-9104'", db.fetch_query)
        self.assertEqual(db.fetch_params["start_time"], start_time)
        self.assertEqual(db.fetch_params["end_time"], end_time)
        self.assertEqual(db.fetch_params["limit"], 10)

    def test_run_inserts_rows_without_deleting_existing_history(self):
        raw_df = pd.DataFrame(
            [
                self._row(1, "dw-3411", SAMPLE_CONTENTS),
                self._row(2, "dw-3411", "system info: dw-3411 normal message"),
                self._row(3, "dw-3411", None),
            ]
        )
        db = FakeDB(raw_df)
        repo = ERDoseRepository(db)
        processor = ERDoseProcessor(repo)

        with redirect_stdout(StringIO()):
            processor.run(start_time=datetime(2026, 5, 1), end_time=datetime(2026, 5, 2))

        delete_queries = [query for query, _, _ in db.executed if query.strip().lower().startswith("delete")]
        self.assertEqual(delete_queries, [])
        parsed_insert = self._inserted_df(db, "mbeat.er_dose_error_parsed")
        self.assertNotIn("parser_version", parsed_insert.columns)
        self.assertNotIn("parsing_status", parsed_insert.columns)
        self.assertNotIn("parsing_error", parsed_insert.columns)
        self.assertEqual(parsed_insert.loc[0, "code_occur_time"], datetime(2026, 5, 1, 10, 0, 0, 123456))
        self.assertEqual(parsed_insert.loc[0, "er_date"], 20260501)
        self.assertEqual(parsed_insert.loc[0, "er_index"], 1)
        self.assertEqual(parsed_insert.loc[0, "belong"], "SCANNER")
        self.assertEqual(parsed_insert.loc[0, "type"], "ER")
        self.assertEqual(parsed_insert.loc[0, "title"], "Dose warning")
        self.assertEqual(parsed_insert.loc[0, "contents"], SAMPLE_CONTENTS)
        inserted_tables = [table_name for table_name, _ in db.inserted]
        self.assertEqual(inserted_tables, ["mbeat.er_dose_error_parsed"])

    def test_run_processes_multiple_chunks(self):
        raw_df = pd.DataFrame(
            [
                self._row(1, "dw-3411", SAMPLE_CONTENTS),
                self._row(2, "dw-3411", SAMPLE_CONTENTS),
                self._row(3, "dw-3411", SAMPLE_CONTENTS),
            ]
        )
        db = FakeDB(raw_df)
        repo = ERDoseRepository(db)
        processor = ERDoseProcessor(repo)

        with redirect_stdout(StringIO()):
            processor.run(
                start_time=datetime(2026, 5, 1),
                end_time=datetime(2026, 5, 2),
                chunk_size=2,
            )

        self.assertEqual(len(db.inserted), 2)
        self.assertEqual(len(db.inserted[0][1]), 2)
        self.assertEqual(len(db.inserted[1][1]), 1)

    def test_partition_creation_covers_each_day_in_range(self):
        db = FakeDB(pd.DataFrame())
        repo = ERDoseRepository(db)

        repo.ensure_partitions(start_time=datetime(2026, 5, 31, 12), end_time=datetime(2026, 6, 2, 1))

        create_queries = [query for query, _, _ in db.executed if "partition of mbeat.er_dose_error_parsed" in query]
        self.assertEqual(len(create_queries), 3)
        self.assertIn("er_dose_error_parsed_1_prt_p20260531", create_queries[0])
        self.assertIn("er_dose_error_parsed_1_prt_p20260601", create_queries[1])
        self.assertIn("er_dose_error_parsed_1_prt_p20260602", create_queries[2])

    def _row(self, row_no, code, contents, code_occur_time=None, belong="SCANNER", eq_name="EQ1"):
        return {
            "er_date": 20260501,
            "er_index": row_no,
            "er_line": "L1",
            "eq_name": eq_name,
            "er_type": "EUV",
            "code": code,
            "code_occur_time": code_occur_time or datetime(2026, 5, 1, 10, 0, 0, 123456),
            "belong": belong,
            "type": "ER",
            "title": "Dose warning",
            "contents": contents,
        }

    def _inserted_df(self, db, table_name):
        for inserted_table_name, inserted_df in db.inserted:
            if inserted_table_name == table_name:
                return inserted_df
        raise AssertionError(f"{table_name} was not inserted")


if __name__ == "__main__":
    unittest.main()
