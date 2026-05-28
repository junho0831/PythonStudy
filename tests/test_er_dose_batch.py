from __future__ import annotations

import unittest
from contextlib import redirect_stdout
from datetime import datetime
from io import StringIO

import pandas as pd

from er_dose.batch import ERDoseBatch


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
        self.inserted_df = None
        self.connection = object()

    def fetch_df(self, query, params=None):
        self.fetch_query = query
        self.fetch_params = params
        return self.raw_df

    def transaction(self):
        return FakeTransaction(self.connection)

    def execute(self, query, params=None, connection=None):
        self.executed.append((query, params, connection))
        if query.strip().lower().startswith("delete"):
            return 7
        return 0

    def bulk_insert_df(self, table_name, df, connection=None):
        self.insert_table_name = table_name
        self.inserted_df = df
        self.insert_connection = connection
        return len(df)


class ERDoseBatchTest(unittest.TestCase):
    def test_fetch_raw_logs_uses_code_occur_time_range(self):
        db = FakeDB(pd.DataFrame())
        batch = ERDoseBatch(db)
        start_time = datetime(2026, 5, 1)
        end_time = datetime(2026, 5, 2)

        batch.fetch_raw_logs(start_time=start_time, end_time=end_time, limit=10)

        self.assertIn("r.code_occur_time >= %(start_time)s", db.fetch_query)
        self.assertIn("r.code_occur_time < %(end_time)s", db.fetch_query)
        self.assertIn("r.er_date", db.fetch_query)
        self.assertIn("r.er_index", db.fetch_query)
        self.assertIn("((r.er_date::bigint * 1000000000) + r.er_index::bigint) as raw_id", db.fetch_query)
        self.assertEqual(db.fetch_params["start_time"], start_time)
        self.assertEqual(db.fetch_params["end_time"], end_time)
        self.assertEqual(db.fetch_params["limit"], 10)

    def test_run_deletes_range_and_inserts_status_rows_in_one_transaction(self):
        raw_df = pd.DataFrame(
            [
                self._row(1, "dw-3411", SAMPLE_CONTENTS),
                self._row(2, "dw-3411", "system info: dw-3411 normal message"),
                self._row(3, "dw-3411", None),
            ]
        )
        db = FakeDB(raw_df)
        batch = ERDoseBatch(db)

        with redirect_stdout(StringIO()):
            summary = batch.run(start_time=datetime(2026, 5, 1), end_time=datetime(2026, 5, 2))

        self.assertEqual(summary["fetched"], 3)
        self.assertEqual(summary["success"], 1)
        self.assertEqual(summary["regex_fail"], 1)
        self.assertEqual(summary["parser_error"], 1)
        self.assertEqual(summary["inserted"], 3)
        self.assertEqual(summary["deleted"], 7)
        self.assertIs(db.insert_connection, db.connection)
        self.assertEqual(list(db.inserted_df["parsing_status"]), ["SUCCESS", "REGEX_FAIL", "PARSER_ERROR"])
        self.assertEqual(db.inserted_df.loc[0, "code_occur_time_raw"], "2026-05-01 10:00:00.123456")
        self.assertEqual(db.inserted_df.loc[0, "er_date"], 20260501)
        self.assertEqual(db.inserted_df.loc[0, "er_index"], 1)
        self.assertEqual(db.inserted_df.loc[0, "raw_id"], 20260501000000001)
        self.assertEqual(db.inserted_df.loc[0, "log_source"], "SCANNER:ER")

    def test_partition_creation_covers_each_month_in_range(self):
        db = FakeDB(pd.DataFrame())
        batch = ERDoseBatch(db)

        batch.ensure_partitions(start_time=datetime(2026, 5, 31), end_time=datetime(2026, 7, 1))

        create_queries = [query for query, _, _ in db.executed if "partition of mbeat.er_dose_error_parsed" in query]
        self.assertEqual(len(create_queries), 2)
        self.assertIn("er_dose_error_parsed_202605", create_queries[0])
        self.assertIn("er_dose_error_parsed_202606", create_queries[1])

    def _row(self, raw_id, code, contents):
        return {
            "er_date": 20260501,
            "er_index": raw_id,
            "raw_id": 20260501000000000 + raw_id,
            "er_line": "L1",
            "eq_name": "EQ1",
            "code": code,
            "code_occur_time": datetime(2026, 5, 1, 10, 0, 0, 123456),
            "belong": "SCANNER",
            "type": "ER",
            "contents": contents,
        }


if __name__ == "__main__":
    unittest.main()
