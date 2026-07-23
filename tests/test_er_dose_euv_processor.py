from __future__ import annotations

import unittest
from contextlib import redirect_stdout
from datetime import date, datetime
from io import StringIO

import pandas as pd

from er_dose.euv.euv_processor import ERDoseEUVProcessor
from er_dose.euv.euv_repository import ERDoseEUVRepository
from tests.test_er_dose_root_cause import SAMPLE_EUV_CONTENTS


class FakeTransaction:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, exc, traceback):
        return False


class FakeDB:
    def __init__(self, raw_df, source_counts=None, target_counts=None):
        self.raw_df = raw_df
        self.source_counts = source_counts or {}
        self.target_counts = target_counts or {}
        self.fetch_query = None
        self.fetch_params = None
        self.executed = []
        self.inserted = []
        self.copy_options = []
        self.connection = object()
        self.partition_inserts = []

    def select(self, query, params=None):
        self.fetch_query = query
        self.fetch_params = params
        lowered = query.lower()
        if "count(*) as row_count" in lowered:
            target_date = params["start_time"].date()
            if "from mbeat.er_data_raw_euv r" in lowered:
                return pd.DataFrame([{"row_count": self.source_counts.get(target_date, 0)}])
            if "from prism_common.er_dose_euv_parsed p" in lowered:
                return pd.DataFrame([{"row_count": self.target_counts.get(target_date, 0)}])
        return pd.DataFrame()

    def select_in_chunks(self, query, params=None, chunk_size=10000):
        self.fetch_query = query
        self.fetch_params = params
        for start in range(0, len(self.raw_df), chunk_size):
            yield self.raw_df.iloc[start : start + chunk_size].copy()

    def transaction(self):
        return FakeTransaction(self.connection)

    def execute(self, query, params=None, connection=None):
        self.executed.append((query, params, connection))
        return 0

    def copy_insert_to_partition_table(
        self,
        schema,
        table_name,
        target_date,
        df,
        is_truncate=False,
        connection=None,
        analyze=True,
    ):
        full_table_name = f"{schema}.{table_name}"
        self.inserted.append((full_table_name, df.copy()))
        self.partition_inserts.append((full_table_name, target_date, df.copy()))
        self.copy_options.append({"target_date": target_date, "analyze": analyze, "connection": connection})
        return len(df)

    def analyze_partition_table(self, schema, table_name, target_date, connection=None):
        query = f"ANALYZE {schema}.{table_name}_1_prt_p{target_date.replace('-', '')}"
        self.executed.append((query, None, connection))
        return 0


class ERDoseEUVProcessorTest(unittest.TestCase):
    def test_fetch_raw_logs_uses_euv_raw_table_and_time_range(self):
        db = FakeDB(pd.DataFrame())
        repo = ERDoseEUVRepository(db)
        start_time = datetime(2026, 5, 1)
        end_time = datetime(2026, 5, 2)

        list(
            repo.fetch_raw_logs_in_chunks(
                start_time=start_time,
                end_time=end_time,
                chunk_size=100,
            )
        )

        self.assertIn("from mbeat.er_data_raw_euv r", db.fetch_query)
        self.assertNotIn("r.er_line,", db.fetch_query)
        self.assertIn("r.er_type", db.fetch_query)
        self.assertNotIn("r.belong", db.fetch_query)
        self.assertNotIn('r."type" as type', db.fetch_query)
        self.assertIn("r.reason_code", db.fetch_query)
        self.assertIn("r.compile_script", db.fetch_query)
        self.assertIn("r.code_occur_time >= :start_time", db.fetch_query)
        self.assertIn("r.code_occur_time < :end_time", db.fetch_query)
        self.assertEqual(db.fetch_params["start_time"], start_time)
        self.assertEqual(db.fetch_params["end_time"], end_time)

    def test_fetch_raw_logs_filters_active_nxe_eq_names(self):
        db = FakeDB(pd.DataFrame())
        repo = ERDoseEUVRepository(db)
        start_time = datetime(2026, 5, 1)
        end_time = datetime(2026, 5, 2)

        list(
            repo.fetch_raw_logs_in_chunks(
                start_time=start_time,
                end_time=end_time,
                chunk_size=100,
            )
        )

        self.assertIn("r.eq_name in", db.fetch_query)
        self.assertIn("from prism_dev.photo_eqp_info eqp", db.fetch_query)
        self.assertIn("eqp.use_yn = 'Y'", db.fetch_query)
        self.assertIn("eqp.eqp_model_name like 'NXE%'", db.fetch_query)

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
        db = FakeDB(
            raw_df,
            source_counts={date(2026, 5, 4): 1},
            target_counts={date(2026, 5, 4): 0},
        )
        repo = ERDoseEUVRepository(db)
        processor = ERDoseEUVProcessor(repo)

        with redirect_stdout(StringIO()):
            processor.run(target_date=date(2026, 5, 4))

        self.assertEqual(db.fetch_params["start_time"], datetime(2026, 5, 4, 0, 0, 0))
        self.assertEqual(db.fetch_params["end_time"], datetime(2026, 5, 5, 0, 0, 0))
        self.assertEqual(len(db.inserted), 1)
        table_name, inserted_df = db.inserted[0]
        self.assertEqual(table_name, "prism_common.er_dose_euv_parsed")
        self.assertEqual(len(inserted_df), 1)
        self.assertEqual([option["analyze"] for option in db.copy_options], [False])
        analyze_queries = [query for query, _, _ in db.executed if query == "ANALYZE prism_common.er_dose_euv_parsed_1_prt_p20260504"]
        self.assertEqual(len(analyze_queries), 1)
        self.assertNotIn("er_line", inserted_df.columns)
        self.assertNotIn("belong", inserted_df.columns)
        self.assertNotIn("type", inserted_df.columns)
        self.assertEqual(inserted_df.loc[0, "er_type"], "EUV")
        self.assertEqual(inserted_df.loc[0, "exposure_id"], 25415)
        self.assertEqual(inserted_df.loc[0, "root_cause_code"], "plasma_oscillations")
        self.assertEqual(inserted_df.loc[0, "dose_error_detected_in_file"], "adecetdcdata_fdd_lc_eei_scanner_dose_error_event_20260504_180529_3502+0900.zip")
        self.assertEqual(db.partition_inserts[0][1], "2026-05-04")

    def test_fetch_counts_filter_active_nxe_eq_names_and_root_cause_source(self):
        target_date = date(2026, 5, 4)
        db = FakeDB(
            pd.DataFrame(),
            source_counts={target_date: 1},
            target_counts={target_date: 1},
        )
        repo = ERDoseEUVRepository(db)

        self.assertEqual(repo.fetch_source_count(target_date), 1)
        self.assertIn("from mbeat.er_data_raw_euv r", db.fetch_query)
        self.assertIn("lower(r.contents) like '%dose error detected in file:%'", db.fetch_query)
        self.assertIn("lower(r.contents) like '%root cause%'", db.fetch_query)
        self.assertIn("from prism_dev.photo_eqp_info eqp", db.fetch_query)

        self.assertEqual(repo.fetch_target_count(target_date), 1)
        self.assertIn("from prism_common.er_dose_euv_parsed p", db.fetch_query)
        self.assertIn("from prism_dev.photo_eqp_info eqp", db.fetch_query)

    def test_run_recent_days_skips_when_counts_match(self):
        target_date = date(2026, 5, 4)
        raw_df = pd.DataFrame(
            [
                {
                    "eq_name": "EQ1",
                    "er_type": "EUV",
                    "code": "CODE1",
                    "code_occur_time": datetime(2026, 5, 4, 18, 5, 29),
                    "title": "Dose Error Root Cause",
                    "contents": SAMPLE_EUV_CONTENTS,
                    "reason_code": "R1",
                    "task": "TASK1",
                    "compile_script": "SCRIPT1",
                },
            ]
        )
        db = FakeDB(
            raw_df,
            source_counts={target_date: 1},
            target_counts={target_date: 1},
        )
        repo = ERDoseEUVRepository(db)
        processor = ERDoseEUVProcessor(repo)

        with redirect_stdout(StringIO()) as stdout:
            processor.run_recent_days(
                lookback_days=1,
                reference_date=target_date,
                chunk_size=100,
            )

        self.assertEqual(db.executed, [])
        self.assertEqual(len(db.partition_inserts), 0)
        self.assertIn("lookback_done start_date=2026-05-04 end_date=2026-05-04", stdout.getvalue())
        self.assertIn("checked_dates=1 reloaded_dates=0 source_rows=0 inserted=0", stdout.getvalue())

    def test_run_recent_days_truncates_and_reloads_when_counts_differ(self):
        target_date = date(2026, 5, 4)
        raw_df = pd.DataFrame(
            [
                {
                    "eq_name": "EQ1",
                    "er_type": "EUV",
                    "code": "CODE1",
                    "code_occur_time": datetime(2026, 5, 4, 18, 5, 29),
                    "title": "Dose Error Root Cause",
                    "contents": SAMPLE_EUV_CONTENTS,
                    "reason_code": "R1",
                    "task": "TASK1",
                    "compile_script": "SCRIPT1",
                },
            ]
        )
        db = FakeDB(
            raw_df,
            source_counts={target_date: 1},
            target_counts={target_date: 0},
        )
        repo = ERDoseEUVRepository(db)
        processor = ERDoseEUVProcessor(repo)

        with redirect_stdout(StringIO()) as stdout:
            processor.run_recent_days(
                lookback_days=1,
                reference_date=target_date,
                chunk_size=100,
            )

        truncate_queries = [query for query, _, _ in db.executed if query.strip().upper().startswith("TRUNCATE")]
        self.assertEqual(truncate_queries, ["truncate table prism_common.er_dose_euv_parsed_1_prt_p20260504"])
        self.assertEqual(len(db.partition_inserts), 1)
        self.assertIs(db.executed[0][2], db.connection)
        analyze_queries = [item for item in db.executed if item[0] == "ANALYZE prism_common.er_dose_euv_parsed_1_prt_p20260504"]
        self.assertEqual(len(analyze_queries), 1)
        self.assertIs(analyze_queries[0][2], db.connection)
        self.assertIn("lookback_done start_date=2026-05-04 end_date=2026-05-04", stdout.getvalue())
        self.assertIn("checked_dates=1 reloaded_dates=1 source_rows=1 inserted=1", stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
