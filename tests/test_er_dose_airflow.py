from __future__ import annotations

import os
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

from airflow_modules import er_dose_jobs


class ERDoseAirflowJobsTest(unittest.TestCase):
    def setUp(self):
        self.env_patch = patch.dict(os.environ, {"ER_DOSE_DB_DSN": "postgresql://user:pass@host/db"}, clear=True)
        self.env_patch.start()

    def tearDown(self):
        self.env_patch.stop()

    def test_window_uses_data_interval_with_ten_minute_delay(self):
        batch = MagicMock()
        with patch("airflow_modules.er_dose_jobs.PostgresDB"), patch(
            "airflow_modules.er_dose_jobs.ERDoseBatch", return_value=batch
        ):
            er_dose_jobs.run_er_dose_window(
                data_interval_start=datetime(2026, 6, 1, 10, 0, tzinfo=ZoneInfo("Asia/Seoul")),
                data_interval_end=datetime(2026, 6, 1, 10, 10, tzinfo=ZoneInfo("Asia/Seoul")),
            )

        batch.run.assert_called_once_with(
            start_time=datetime(2026, 6, 1, 9, 50),
            end_time=datetime(2026, 6, 1, 10, 0),
        )

    def test_hourly_backfill_runs_latest_one_day_in_ten_minute_chunks(self):
        batch = MagicMock()
        with patch("airflow_modules.er_dose_jobs.PostgresDB"), patch(
            "airflow_modules.er_dose_jobs.ERDoseBatch", return_value=batch
        ):
            er_dose_jobs.run_er_dose_backfill_hourly(
                data_interval_start=datetime(2026, 6, 1, 10, 0, tzinfo=ZoneInfo("Asia/Seoul")),
                data_interval_end=datetime(2026, 6, 1, 11, 0, tzinfo=ZoneInfo("Asia/Seoul")),
            )

        self.assertEqual(batch.run.call_count, 144)
        batch.run.assert_any_call(start_time=datetime(2026, 5, 31, 10, 50), end_time=datetime(2026, 5, 31, 11, 0))
        batch.run.assert_any_call(start_time=datetime(2026, 6, 1, 10, 40), end_time=datetime(2026, 6, 1, 10, 50))

    def test_daily_backfill_runs_latest_three_days_in_ten_minute_chunks(self):
        batch = MagicMock()
        with patch("airflow_modules.er_dose_jobs.PostgresDB"), patch(
            "airflow_modules.er_dose_jobs.ERDoseBatch", return_value=batch
        ):
            er_dose_jobs.run_er_dose_backfill_daily(
                data_interval_start=datetime(2026, 6, 1, 3, 0, tzinfo=ZoneInfo("Asia/Seoul")),
                data_interval_end=datetime(2026, 6, 2, 3, 0, tzinfo=ZoneInfo("Asia/Seoul")),
            )

        self.assertEqual(batch.run.call_count, 432)
        batch.run.assert_any_call(start_time=datetime(2026, 5, 30, 2, 50), end_time=datetime(2026, 5, 30, 3, 0))
        batch.run.assert_any_call(start_time=datetime(2026, 6, 2, 2, 40), end_time=datetime(2026, 6, 2, 2, 50))

    def test_missing_dsn_fails_before_batch_run(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(ValueError):
                er_dose_jobs.run_er_dose_window(
                    data_interval_start=datetime(2026, 6, 1, 10, 0, tzinfo=ZoneInfo("Asia/Seoul")),
                    data_interval_end=datetime(2026, 6, 1, 10, 10, tzinfo=ZoneInfo("Asia/Seoul")),
                )

    def test_er_dose_dag_files_use_expected_schedules(self):
        root = Path(__file__).resolve().parents[1]

        near_realtime = (root / "dags" / "er_dose_near_realtime_dag.py").read_text(encoding="utf-8")
        hourly = (root / "dags" / "er_dose_backfill_hourly_dag.py").read_text(encoding="utf-8")
        daily = (root / "dags" / "er_dose_backfill_daily_dag.py").read_text(encoding="utf-8")

        self.assertIn('dag_id="er_dose_near_realtime"', near_realtime)
        self.assertIn('schedule="*/10 * * * *"', near_realtime)
        self.assertIn("max_active_runs=1", near_realtime)
        self.assertIn('dag_id="er_dose_backfill_hourly"', hourly)
        self.assertIn('schedule="0 * * * *"', hourly)
        self.assertIn('dag_id="er_dose_backfill_daily"', daily)
        self.assertIn('schedule="0 3 * * *"', daily)


if __name__ == "__main__":
    unittest.main()
