from __future__ import annotations

import unittest
from contextlib import redirect_stdout
from datetime import date, datetime
from io import StringIO

import pandas as pd

from er_dose.euv_processor import ERDoseEUVProcessor
from er_dose.euv_repository import ERDoseEUVRepository
from tests.test_er_dose_root_cause import SAMPLE_EUV_CONTENTS


class FakeDB:
    def __init__(self, raw_df):
        self.raw_df = raw_df
        self.fetch_query = None
        self.fetch_params = None
        self.inserted = []
        self.partition_inserts = []

    def select_in_chunks(self, query, params=None, chunk_size=10000):
        self.fetch_query = query
        self.fetch_params = params
        for start in range(0, len(self.raw_df), chunk_size):
            yield self.raw_df.iloc[start : start + chunk_size].copy()

    def copy_insert_to_partition_table(self, schema, table_name, target_date, df, is_truncate=False):
        full_table_name = f"{schema}.{table_name}"
        self.inserted.append((full_table_name, df.copy()))
        self.partition_inserts.append((full_table_name, target_date, df.copy()))
        return len(df)


class ERDoseEUVProcessorTest(unittest.TestCase):
    def test_fetch_raw_logs_uses_euv_raw_table_and_time_range(self):
        db = FakeDB(pd.DataFrame())
        repo = ERDoseEUVRepository(db)
        start_time = datetime(2026, 5, 1)
        end_time = datetime(2026, 5, 2)

        list(repo.fetch_raw_logs_in_chunks(start_time=start_time, end_time=end_time, chunk_size=100))

        self.assertIn("from mbeat.er_data_raw_euv r", db.fetch_query)
        self.assertIn("r.er_type", db.fetch_query)
        self.assertIn("r.reason_code", db.fetch_query)
        self.assertIn("r.compile_script", db.fetch_query)
        self.assertIn("r.code_occur_time >= :start_time", db.fetch_query)
        self.assertIn("r.code_occur_time < :end_time", db.fetch_query)
        self.assertEqual(db.fetch_params["start_time"], start_time)
        self.assertEqual(db.fetch_params["end_time"], end_time)

    def test_run_inserts_parsed_root_cause_rows(self):
        raw_df = pd.DataFrame(
            [
                {
                    "er_line": "L1",
                    "eq_name": "EQ1",
                    "er_type": "EUV",
                    "code": "CODE1",
                    "code_occur_time": datetime(2026, 5, 4, 18, 5, 29),
                    "belong": "SCANNER",
                    "type": "ER",
                    "title": "Dose Error Root Cause",
                    "contents": SAMPLE_EUV_CONTENTS,
                    "reason_code": "R1",
                    "task": "TASK1",
                    "compile_script": "SCRIPT1",
                },
                {
                    "er_line": "L1",
                    "eq_name": "EQ1",
                    "er_type": "EUV",
                    "code": "CODE2",
                    "code_occur_time": datetime(2026, 5, 4, 19, 0, 0),
                    "belong": "SCANNER",
                    "type": "ER",
                    "title": "Normal",
                    "contents": "system info: normal message",
                    "reason_code": "R2",
                    "task": "TASK2",
                    "compile_script": "SCRIPT2",
                },
            ]
        )
        db = FakeDB(raw_df)
        repo = ERDoseEUVRepository(db)
        processor = ERDoseEUVProcessor(repo)

        with redirect_stdout(StringIO()):
            processor.run(target_date=date(2026, 5, 4))

        self.assertEqual(db.fetch_params["start_time"], datetime(2026, 5, 4, 0, 0, 0))
        self.assertEqual(db.fetch_params["end_time"], datetime(2026, 5, 5, 0, 0, 0))
        self.assertEqual(len(db.inserted), 1)
        table_name, inserted_df = db.inserted[0]
        self.assertEqual(table_name, "prism_common.er_dose_error_root_cause")
        self.assertEqual(len(inserted_df), 1)
        self.assertEqual(inserted_df.loc[0, "source_exposure_id"], 25415)
        self.assertEqual(inserted_df.loc[0, "root_cause_code"], "plasma_oscillations")
        self.assertEqual(inserted_df.loc[0, "source_file_name"], "adecetdcdata_fdd_lc_eei_scanner_dose_error_event_20260504_180529_3502+0900.zip")
        self.assertEqual(db.partition_inserts[0][1], "2026-05-04")


if __name__ == "__main__":
    unittest.main()
