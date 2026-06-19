from __future__ import annotations

import unittest
from datetime import date, datetime
from unittest.mock import patch

from er_dose import run_er_dose_batch


class RunERDoseBatchTest(unittest.TestCase):
    def test_cli_date_parser_er_dose_raw_maps_to_target_date(self):
        with patch("er_dose.run_er_dose_batch.PostgresDB") as postgres_db, patch(
            "er_dose.run_er_dose_batch.ERDoseRepository"
        ) as repo_cls, patch("er_dose.run_er_dose_batch.ERDoseProcessor") as processor_cls:
            result = run_er_dose_batch.main([
                "--date",
                "2026-06-16",
                "--parser",
                "ER_DOSE_RAW",
                "--dsn",
                "postgresql://user:pass@localhost:5432/db",
            ])

        self.assertEqual(result, 0)
        postgres_db.assert_called_once_with(dsn="postgresql://user:pass@localhost:5432/db")
        repo_cls.assert_called_once_with(postgres_db.return_value)
        processor_cls.assert_called_once_with(repo_cls.return_value)
        processor_cls.return_value.run.assert_called_once_with(
            start_time=None,
            end_time=None,
            chunk_size=10000,
            target_date=date(2026, 6, 16),
        )

    def test_cli_er_dose_euv_maps_to_target_date(self):
        with patch("er_dose.run_er_dose_batch.PostgresDB") as postgres_db, patch(
            "er_dose.run_er_dose_batch.ERDoseEUVRepository"
        ) as repo_cls, patch("er_dose.run_er_dose_batch.ERDoseEUVProcessor") as processor_cls:
            result = run_er_dose_batch.main([
                "--date",
                "2026-06-16",
                "--parser",
                "ER_DOSE_EUV",
            ])

        self.assertEqual(result, 0)
        postgres_db.assert_called_once_with(dsn=None)
        repo_cls.assert_called_once_with(postgres_db.return_value)
        processor_cls.assert_called_once_with(repo_cls.return_value)
        processor_cls.return_value.run.assert_called_once_with(
            start_time=None,
            end_time=None,
            chunk_size=10000,
            target_date=date(2026, 6, 16),
        )

    def test_cli_start_end_still_supported(self):
        with patch("er_dose.run_er_dose_batch.PostgresDB"), patch(
            "er_dose.run_er_dose_batch.ERDoseRepository"
        ), patch("er_dose.run_er_dose_batch.ERDoseProcessor") as processor_cls:
            result = run_er_dose_batch.main([
                "--start-time",
                "2026-06-16T00:00:00",
                "--end-time",
                "2026-06-17T00:00:00",
            ])

        self.assertEqual(result, 0)
        processor_cls.return_value.run.assert_called_once_with(
            start_time=datetime(2026, 6, 16, 0, 0, 0),
            end_time=datetime(2026, 6, 17, 0, 0, 0),
            chunk_size=10000,
            target_date=None,
        )

    def test_cli_requires_date_or_start_end(self):
        with self.assertRaises(ValueError):
            run_er_dose_batch.main([])


if __name__ == "__main__":
    unittest.main()
