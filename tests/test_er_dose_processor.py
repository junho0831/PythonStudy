from __future__ import annotations

import unittest
from contextlib import redirect_stdout
from datetime import datetime
from io import StringIO

import pandas as pd

from er_dose.raw.raw_processor import ERDoseProcessor
from er_dose.raw.raw_repository import ERDoseRepository


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
    def __init__(self, raw_df, fetch_df_result=None, source_counts=None, target_counts=None):
        self.raw_df = raw_df
        self.fetch_df_result = raw_df if fetch_df_result is None else fetch_df_result
        self.source_counts = source_counts or {}
        self.target_counts = target_counts or {}
        self.executed = []
        self.inserted = []
        self.copy_options = []
        self.connection = object()
        self.partition_inserts = []
        self.queries_called = []

    def select(self, query, params=None):
        self.fetch_query = query
        self.fetch_params = params
        lowered = query.lower()
        if "count(*) as row_count" in lowered:
            target_date = params["start_time"].date()
            if "from mbeat.er_data_raw r" in lowered:
                return pd.DataFrame([{"row_count": self.source_counts.get(target_date, 0)}])
            if "from prism_common.er_dose_raw_parsed p" in lowered:
                return pd.DataFrame([{"row_count": self.target_counts.get(target_date, 0)}])
        return self.fetch_df_result

    def select_in_chunks(self, query, params=None, chunk_size=10000):
        self.fetch_query = query
        self.fetch_params = params
        self.queries_called.append((query, params))
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
        self.inserted.append((full_table_name, df))
        self.partition_inserts.append((full_table_name, target_date, df.copy()))
        self.copy_options.append({"target_date": target_date, "analyze": analyze, "connection": connection})
        return len(df)


class ERDoseProcessorTest(unittest.TestCase):
    def test_fetch_raw_logs_uses_general_raw_table_and_code_occur_time_range(self):
        db = FakeDB(pd.DataFrame())
        repo = ERDoseRepository(db)
        start_time = datetime(2026, 5, 1)
        end_time = datetime(2026, 5, 2)

        list(
            repo.fetch_raw_logs_in_chunks(
                start_time=start_time,
                end_time=end_time,
                chunk_size=100,
            )
        )

        self.assertIn("from mbeat.er_data_raw_1_prt_p20260501 r", db.fetch_query)
        self.assertNotIn("r.er_line", db.fetch_query)
        self.assertNotIn('r."type" as type', db.fetch_query)
        self.assertNotIn("r.belong", db.fetch_query)
        self.assertIn("r.title", db.fetch_query)
        self.assertIn("r.code_occur_time >= :start_time", db.fetch_query)
        self.assertIn("r.code_occur_time < :end_time", db.fetch_query)
        self.assertIn("r.code in", db.fetch_query)
        self.assertIn("'DW-3411'", db.fetch_query)
        self.assertIn("'DW-3425'", db.fetch_query)
        self.assertIn("'DW-343A'", db.fetch_query)
        self.assertIn("'DW-343B'", db.fetch_query)
        self.assertIn("'LO-0050'", db.fetch_query)
        self.assertIn("'LO-0061'", db.fetch_query)
        self.assertIn("'LO-8166'", db.fetch_query)
        self.assertIn("'LO-8167'", db.fetch_query)
        self.assertIn("'KE-9103'", db.fetch_query)
        self.assertIn("'KE-9104'", db.fetch_query)
        self.assertEqual(db.fetch_params["start_time"], start_time)
        self.assertEqual(db.fetch_params["end_time"], end_time)

    def test_fetch_raw_logs_filters_active_nxe_eq_names(self):
        db = FakeDB(pd.DataFrame())
        repo = ERDoseRepository(db)
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

    def test_fetch_counts_filter_active_nxe_eq_names(self):
        target_date = datetime(2026, 5, 1).date()
        db = FakeDB(
            pd.DataFrame(),
            source_counts={target_date: 1},
            target_counts={target_date: 1},
        )
        repo = ERDoseRepository(db)

        repo.fetch_source_count(target_date)
        self.assertIn("r.eq_name in", db.fetch_query)
        self.assertIn("from prism_dev.photo_eqp_info eqp", db.fetch_query)

        repo.fetch_target_count(target_date)
        self.assertIn("p.eq_name in", db.fetch_query)
        self.assertIn("from prism_dev.photo_eqp_info eqp", db.fetch_query)

    def test_fetch_latest_lot_states_filters_active_nxe_eq_names(self):
        db = FakeDB(
            pd.DataFrame(),
            fetch_df_result=pd.DataFrame(
                [{"eq_name": "EQ1", "lot_seq": 1, "wafer_seq": 2}]
            ),
        )
        repo = ERDoseRepository(db)

        states = repo.fetch_latest_lot_states(
            datetime(2026, 5, 2),
        )

        self.assertEqual(states["EQ1"], {"lot_seq": 1, "wafer_seq": 2})
        self.assertIn("p.eq_name in", db.fetch_query)
        self.assertIn("from prism_dev.photo_eqp_info eqp", db.fetch_query)

    def test_fetch_raw_logs_across_multiple_days(self):
        db = FakeDB(pd.DataFrame())
        repo = ERDoseRepository(db)
        start_time = datetime(2026, 5, 1, 12, 0, 0)
        end_time = datetime(2026, 5, 3, 14, 0, 0)

        list(
            repo.fetch_raw_logs_in_chunks(
                start_time=start_time,
                end_time=end_time,
                chunk_size=100,
            )
        )

        # Should make 3 queries (Day 1: 5/1 12:00 to 5/2 0:00, Day 2: 5/2 0:00 to 5/3 0:00, Day 3: 5/3 0:00 to 5/3 14:00)
        self.assertEqual(len(db.queries_called), 3)

        q1, p1 = db.queries_called[0]
        q2, p2 = db.queries_called[1]
        q3, p3 = db.queries_called[2]

        self.assertIn("from mbeat.er_data_raw_1_prt_p20260501 r", q1)
        self.assertEqual(p1["start_time"], datetime(2026, 5, 1, 12, 0, 0))
        self.assertEqual(p1["end_time"], datetime(2026, 5, 2, 0, 0, 0))

        self.assertIn("from mbeat.er_data_raw_1_prt_p20260502 r", q2)
        self.assertEqual(p2["start_time"], datetime(2026, 5, 2, 0, 0, 0))
        self.assertEqual(p2["end_time"], datetime(2026, 5, 3, 0, 0, 0))

        self.assertIn("from mbeat.er_data_raw_1_prt_p20260503 r", q3)
        self.assertEqual(p3["start_time"], datetime(2026, 5, 3, 0, 0, 0))
        self.assertEqual(p3["end_time"], datetime(2026, 5, 3, 14, 0, 0))

    def test_fetch_latest_lot_states_returns_latest_state_per_eq_name(self):
        history_df = pd.DataFrame(
            [
                {"eq_name": "EQ1", "lot_seq": 1001, "wafer_seq": 21},
                {"eq_name": "EQ2", "lot_seq": None, "wafer_seq": 7},
            ]
        )
        db = FakeDB(pd.DataFrame(), fetch_df_result=history_df)
        repo = ERDoseRepository(db)
        start_time = datetime(2026, 5, 2)

        lot_states = repo.fetch_latest_lot_states(start_time)

        self.assertIn("from prism_common.er_dose_raw_parsed p", db.fetch_query)
        self.assertIn("p.lot_seq", db.fetch_query)
        self.assertNotIn("p.wafer_id", db.fetch_query)
        self.assertIn("p.code_occur_time >= :previous_day_start", db.fetch_query)
        self.assertIn("p.code_occur_time < :start_time", db.fetch_query)
        self.assertEqual(db.fetch_params["start_time"], start_time)
        self.assertEqual(db.fetch_params["previous_day_start"], datetime(2026, 5, 1, 0, 0, 0))
        self.assertEqual(
            lot_states,
            {
                "EQ1": {"lot_seq": 1001, "wafer_seq": 21},
                "EQ2": {"lot_seq": None, "wafer_seq": 7},
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
        parsed_insert = self._inserted_df(db, "prism_common.er_dose_raw_parsed")
        self.assertNotIn("parser_version", parsed_insert.columns)
        self.assertNotIn("parsing_status", parsed_insert.columns)
        self.assertNotIn("parsing_error", parsed_insert.columns)
        self.assertNotIn("er_date", parsed_insert.columns)
        self.assertNotIn("er_index", parsed_insert.columns)
        self.assertNotIn("er_line", parsed_insert.columns)
        self.assertNotIn("belong", parsed_insert.columns)
        self.assertNotIn("type", parsed_insert.columns)
        self.assertEqual(parsed_insert.loc[0, "code_occur_time"], datetime(2026, 5, 1, 10, 0, 0, 123456))
        self.assertEqual(parsed_insert.loc[0, "title"], "Dose warning")
        self.assertEqual(parsed_insert.loc[0, "contents"], SAMPLE_CONTENTS)
        self.assertIn("lot_id", parsed_insert.columns)
        self.assertIn("lot_name", parsed_insert.columns)
        self.assertTrue(pd.isna(parsed_insert.loc[0, "lot_id"]))
        self.assertTrue(pd.isna(parsed_insert.loc[0, "lot_name"]))
        self.assertIn("lot_seq", parsed_insert.columns)
        self.assertTrue(pd.isna(parsed_insert.loc[0, "lot_seq"]))
        self.assertIn("wafer_seq", parsed_insert.columns)
        self.assertTrue(pd.isna(parsed_insert.loc[0, "wafer_seq"]))
        self.assertIn("use_yn", parsed_insert.columns)
        self.assertEqual(parsed_insert.loc[0, "use_yn"], "Y")
        inserted_tables = [table_name for table_name, _ in db.inserted]
        self.assertEqual(inserted_tables, ["prism_common.er_dose_raw_parsed"])

    def test_run_uses_preloaded_lot_state_when_chunk_starts_without_lot_info(self):
        raw_df = pd.DataFrame([
            self._row(1, "lo-0061", "system info: lo-0061 normal message", eq_name="EQ1")
        ])
        history_df = pd.DataFrame([
            {"eq_name": "EQ1", "lot_seq": 2111, "wafer_seq": 23}
        ])
        db = FakeDB(raw_df, fetch_df_result=history_df)
        repo = ERDoseRepository(db)
        processor = ERDoseProcessor(repo)

        with redirect_stdout(StringIO()):
            processor.run(start_time=datetime(2026, 5, 2), end_time=datetime(2026, 5, 3))

        parsed_insert = self._inserted_df(db, "prism_common.er_dose_raw_parsed")
        self.assertEqual(parsed_insert.loc[0, "lot_seq"], 2111)
        self.assertEqual(parsed_insert.loc[0, "wafer_seq"], 23)

    def test_run_marks_dw_jump_rows_unused_without_skipping_insert(self):
        jump_contents = SAMPLE_CONTENTS.replace("exposure_handle:2631", "exposure_handle:3631")
        next_contents = SAMPLE_CONTENTS.replace("exposure_handle:2631", "exposure_handle:3632")
        raw_df = pd.DataFrame(
            [
                self._row(1, "DW-3411", SAMPLE_CONTENTS, eq_name="EQ1"),
                self._row(2, "DW-3411", jump_contents, eq_name="EQ1"),
                self._row(3, "DW-3411", next_contents, eq_name="EQ1"),
            ]
        )
        db = FakeDB(raw_df)
        repo = ERDoseRepository(db)
        processor = ERDoseProcessor(repo)

        with redirect_stdout(StringIO()) as stdout:
            processor.run(start_time=datetime(2026, 5, 1), end_time=datetime(2026, 5, 2))

        parsed_insert = self._inserted_df(db, "prism_common.er_dose_raw_parsed")
        self.assertEqual(len(parsed_insert), 3)
        self.assertEqual(parsed_insert.loc[0, "exposure_handle"], 2631)
        self.assertEqual(parsed_insert.loc[0, "use_yn"], "Y")
        self.assertEqual(parsed_insert.loc[1, "exposure_handle"], 3631)
        self.assertEqual(parsed_insert.loc[1, "use_yn"], "N")
        self.assertEqual(parsed_insert.loc[2, "exposure_handle"], 3632)
        self.assertEqual(parsed_insert.loc[2, "use_yn"], "N")
        self.assertIn("mark_unused_test_shot", stdout.getvalue())

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
        self.assertEqual([option["analyze"] for option in db.copy_options], [False, False])
        analyze_queries = [query for query, _, _ in db.executed if query == "ANALYZE prism_common.er_dose_raw_parsed_1_prt_p20260501"]
        self.assertEqual(len(analyze_queries), 1)

    def test_run_accepts_target_date_and_builds_daily_window(self):
        raw_df = pd.DataFrame([
            self._row(1, "dw-3411", SAMPLE_CONTENTS)
        ])
        db = FakeDB(raw_df)
        repo = ERDoseRepository(db)
        processor = ERDoseProcessor(repo)

        with redirect_stdout(StringIO()):
            processor.run(target_date=datetime(2026, 5, 1).date())

        self.assertEqual(db.fetch_params["start_time"], datetime(2026, 5, 1, 0, 0, 0))
        self.assertEqual(db.fetch_params["end_time"], datetime(2026, 5, 2, 0, 0, 0))

    def test_run_without_window_uses_recent_days_lookback(self):
        raw_df = pd.DataFrame([
            self._row(1, "dw-3411", SAMPLE_CONTENTS, code_occur_time=datetime(2026, 5, 2, 10, 0, 0))
        ])
        db = FakeDB(
            raw_df,
            source_counts={
                datetime(2026, 5, 1).date(): 0,
                datetime(2026, 5, 2).date(): 1,
            },
            target_counts={
                datetime(2026, 5, 1).date(): 0,
                datetime(2026, 5, 2).date(): 1,
            },
        )
        repo = ERDoseRepository(db)
        processor = ERDoseProcessor(repo)

        with redirect_stdout(StringIO()) as stdout:
            processor.run(
                lookback_days=2,
                reference_date=datetime(2026, 5, 2).date(),
                chunk_size=100,
            )

        self.assertEqual(db.executed, [])
        self.assertEqual(len(db.partition_inserts), 0)
        self.assertIn("lookback_done start_date=2026-05-01 end_date=2026-05-02", stdout.getvalue())
        self.assertIn("checked_dates=2 reloaded_dates=0 source_rows=0 inserted=0", stdout.getvalue())

    def test_run_recent_days_skips_when_counts_match(self):
        raw_df = pd.DataFrame([
            self._row(1, "dw-3411", SAMPLE_CONTENTS, code_occur_time=datetime(2026, 5, 2, 10, 0, 0))
        ])
        db = FakeDB(
            raw_df,
            source_counts={
                datetime(2026, 5, 1).date(): 0,
                datetime(2026, 5, 2).date(): 1,
            },
            target_counts={
                datetime(2026, 5, 1).date(): 0,
                datetime(2026, 5, 2).date(): 1,
            },
        )
        repo = ERDoseRepository(db)
        processor = ERDoseProcessor(repo)

        with redirect_stdout(StringIO()) as stdout:
            processor.run_recent_days(
                lookback_days=2,
                reference_date=datetime(2026, 5, 2).date(),
                chunk_size=100,
            )

        self.assertEqual(db.executed, [])
        self.assertEqual(len(db.partition_inserts), 0)
        self.assertIn("lookback_done start_date=2026-05-01 end_date=2026-05-02", stdout.getvalue())
        self.assertIn("checked_dates=2 reloaded_dates=0 source_rows=0 inserted=0", stdout.getvalue())

    def test_run_recent_days_truncates_and_reloads_when_counts_differ(self):
        target_date = datetime(2026, 5, 1).date()
        raw_df = pd.DataFrame([
            self._row(1, "dw-3411", SAMPLE_CONTENTS)
        ])
        db = FakeDB(
            raw_df,
            source_counts={target_date: 1},
            target_counts={target_date: 0},
        )
        repo = ERDoseRepository(db)
        processor = ERDoseProcessor(repo)

        with redirect_stdout(StringIO()) as stdout:
            processor.run_recent_days(
                lookback_days=1,
                reference_date=target_date,
                chunk_size=100,
            )

        truncate_queries = [query for query, _, _ in db.executed if query.strip().upper().startswith("TRUNCATE")]
        self.assertEqual(truncate_queries, ["truncate table prism_common.er_dose_raw_parsed_1_prt_p20260501"])
        self.assertEqual(len(db.partition_inserts), 1)
        self.assertIs(db.executed[0][2], db.connection)
        analyze_queries = [item for item in db.executed if item[0] == "ANALYZE prism_common.er_dose_raw_parsed_1_prt_p20260501"]
        self.assertEqual(len(analyze_queries), 1)
        self.assertIs(analyze_queries[0][2], db.connection)
        self.assertIn("lookback_done start_date=2026-05-01 end_date=2026-05-01", stdout.getvalue())
        self.assertIn("checked_dates=1 reloaded_dates=1 source_rows=1 inserted=1", stdout.getvalue())

    def test_run_recent_days_skips_when_counts_match_even_if_specific_row_is_missing(self):
        target_date = datetime(2026, 5, 1).date()
        raw_df = pd.DataFrame([
            self._row(1, "dw-3411", SAMPLE_CONTENTS)
        ])
        db = FakeDB(
            raw_df,
            source_counts={target_date: 1},
            target_counts={target_date: 1},
        )
        repo = ERDoseRepository(db)
        processor = ERDoseProcessor(repo)

        with redirect_stdout(StringIO()) as stdout:
            processor.run_recent_days(
                lookback_days=1,
                reference_date=target_date,
                chunk_size=100,
            )

        truncate_queries = [query for query, _, _ in db.executed if query.strip().upper().startswith("TRUNCATE")]
        self.assertEqual(truncate_queries, [])
        self.assertEqual(len(db.partition_inserts), 0)
        self.assertIn("lookback_done start_date=2026-05-01 end_date=2026-05-01", stdout.getvalue())
        self.assertIn("checked_dates=1 reloaded_dates=0 source_rows=0 inserted=0", stdout.getvalue())

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
                    "lot_id": "HJO449.1_1747_0_MP232325",
                    "lot_name": "HJO449",
                    "lot_seq": None,
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
                    "lot_seq": 2111,
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
        self.assertEqual(str(inserted_df["lot_seq"].dtype), "Int64")
        self.assertEqual(str(inserted_df["wafer_seq"].dtype), "Int64")
        self.assertEqual(str(inserted_df["n_slit"].dtype), "Int64")
        self.assertEqual(inserted_df.loc[0, "lot_id"], "HJO449.1_1747_0_MP232325")
        self.assertEqual(inserted_df.loc[0, "lot_name"], "HJO449")
        self.assertNotIn("er_date", inserted_df.columns)
        self.assertNotIn("er_index", inserted_df.columns)
        self.assertNotIn("er_line", inserted_df.columns)
        self.assertNotIn("belong", inserted_df.columns)
        self.assertNotIn("type", inserted_df.columns)
        self.assertEqual(inserted_df.loc[0, "exposure_handle"], 11388)
        self.assertTrue(pd.isna(inserted_df.loc[1, "exposure_handle"]))
        self.assertEqual(list(inserted_df["use_yn"]), ["Y", "Y"])

    def _row(self, row_no, code, contents, code_occur_time=None, eq_name="EQ1"):
        return {
            "eq_name": eq_name,
            "er_type": "EUV",
            "code": code,
            "code_occur_time": code_occur_time or datetime(2026, 5, 1, 10, 0, 0, 123456),
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
