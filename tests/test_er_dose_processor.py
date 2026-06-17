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
    def __init__(self, raw_df, fetch_df_result=None):
        self.raw_df = raw_df
        self.fetch_df_result = raw_df if fetch_df_result is None else fetch_df_result
        self.executed = []
        self.inserted = []
        self.connection = object()
        self.partition_inserts = []

    def fetch_df(self, query, params=None):
        self.fetch_query = query
        self.fetch_params = params
        return self.fetch_df_result

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
        self.partition_inserts.append((full_table_name, target_date, df.copy()))
        return len(df)


class ERDoseProcessorTest(unittest.TestCase):
    def test_fetch_raw_logs_uses_general_raw_table_and_code_occur_time_range(self):
        db = FakeDB(pd.DataFrame())
        repo = ERDoseRepository(db)
        start_time = datetime(2026, 5, 1)
        end_time = datetime(2026, 5, 2)

        list(repo.fetch_raw_logs_in_chunks(start_time=start_time, end_time=end_time, chunk_size=100))

        self.assertIn("from mbeat.er_data_raw r", db.fetch_query)
        self.assertIn("r.er_date", db.fetch_query)
        self.assertIn("r.er_index", db.fetch_query)
        self.assertIn('r."type" as type', db.fetch_query)
        self.assertIn("r.title", db.fetch_query)
        self.assertIn("r.code_occur_time >= :start_time", db.fetch_query)
        self.assertIn("r.code_occur_time < :end_time", db.fetch_query)
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

    def test_fetch_latest_wafer_states_returns_latest_state_per_eq_name(self):
        history_df = pd.DataFrame(
            [
                {"eq_name": "EQ1", "wafer_id": 1001, "wafer_seq": 21},
                {"eq_name": "EQ2", "wafer_id": None, "wafer_seq": 7},
            ]
        )
        db = FakeDB(pd.DataFrame(), fetch_df_result=history_df)
        repo = ERDoseRepository(db)
        start_time = datetime(2026, 5, 2)

        wafer_states = repo.fetch_latest_wafer_states(start_time)

        self.assertIn("from prism_common.er_dose_error_parsed p", db.fetch_query)
        self.assertIn("p.code_occur_time < :start_time", db.fetch_query)
        self.assertEqual(db.fetch_params["start_time"], start_time)
        self.assertEqual(
            wafer_states,
            {
                "EQ1": {"wafer_id": 1001, "wafer_seq": 21},
                "EQ2": {"wafer_id": None, "wafer_seq": 7},
            },
        )

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
        parsed_insert = self._inserted_df(db, "prism_common.er_dose_error_parsed")
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
        self.assertIn("wafer_seq", parsed_insert.columns)
        self.assertTrue(pd.isna(parsed_insert.loc[0, "wafer_seq"]))
        inserted_tables = [table_name for table_name, _ in db.inserted]
        self.assertEqual(inserted_tables, ["prism_common.er_dose_error_parsed"])

    def test_run_uses_preloaded_wafer_state_when_chunk_starts_without_wafer_info(self):
        raw_df = pd.DataFrame([
            self._row(1, "lo-0061", "system info: lo-0061 normal message", eq_name="EQ1")
        ])
        history_df = pd.DataFrame([
            {"eq_name": "EQ1", "wafer_id": 2111, "wafer_seq": 23}
        ])
        db = FakeDB(raw_df, fetch_df_result=history_df)
        repo = ERDoseRepository(db)
        processor = ERDoseProcessor(repo)

        with redirect_stdout(StringIO()):
            processor.run(start_time=datetime(2026, 5, 2), end_time=datetime(2026, 5, 3))

        parsed_insert = self._inserted_df(db, "prism_common.er_dose_error_parsed")
        self.assertEqual(parsed_insert.loc[0, "wafer_id"], 2111)
        self.assertEqual(parsed_insert.loc[0, "wafer_seq"], 23)

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

    def test_insert_parsed_df_keeps_integer_columns_as_nullable_int(self):
        db = FakeDB(pd.DataFrame())
        repo = ERDoseRepository(db)
        df = pd.DataFrame(
            [
                {
                    "er_date": 20260615,
                    "er_index": 1,
                    "er_line": "L1",
                    "eq_name": "EQ1",
                    "code": "DW-3411",
                    "code_occur_time": datetime(2026, 6, 15, 10, 0, 0),
                    "belong": "SCANNER",
                    "type": "ER",
                    "title": "Dose warning",
                    "contents": SAMPLE_CONTENTS,
                    "exposure_handle": 11388,
                    "action_handle": None,
                    "wafer_id": None,
                    "wafer_seq": 44,
                    "de_err": "0.0461075",
                    "n_slit": 44,
                },
                {
                    "er_date": 20260615,
                    "er_index": 2,
                    "er_line": "L1",
                    "eq_name": "EQ1",
                    "code": "DW-3411",
                    "code_occur_time": datetime(2026, 6, 15, 10, 1, 0),
                    "belong": "SCANNER",
                    "type": "ER",
                    "title": "Dose warning",
                    "contents": SAMPLE_CONTENTS,
                    "exposure_handle": None,
                    "action_handle": 2625,
                    "wafer_id": 2111,
                    "wafer_seq": None,
                    "de_err": "0.0461075",
                    "n_slit": None,
                },
            ]
        )

        repo.insert_parsed_df(df)

        inserted_df = db.partition_inserts[0][2]
        self.assertEqual(str(inserted_df["exposure_handle"].dtype), "Int64")
        self.assertEqual(str(inserted_df["action_handle"].dtype), "Int64")
        self.assertEqual(str(inserted_df["wafer_id"].dtype), "Int64")
        self.assertEqual(str(inserted_df["wafer_seq"].dtype), "Int64")
        self.assertEqual(str(inserted_df["n_slit"].dtype), "Int64")
        self.assertEqual(inserted_df.loc[0, "exposure_handle"], 11388)
        self.assertTrue(pd.isna(inserted_df.loc[1, "exposure_handle"]))

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
