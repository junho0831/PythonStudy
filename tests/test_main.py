from __future__ import annotations

import unittest
from datetime import datetime
from unittest.mock import Mock, patch

from batch_main.main import Main


class MainTest(unittest.TestCase):
    def test_rbi_target_runs_rbi_batch(self):
        env = {
            "BATCH_TARGET": "RBI",
            "RBI_INPUT_DATE": "2026-05-31",
            "RBI_PARSER": "COMBINED",
        }

        with patch("batch_main.main.run_batch") as run_batch:
            result = Main(env=env).run()

        self.assertEqual(result, 0)
        run_batch.assert_called_once_with(input_date="2026-05-31", parser_name="COMBINED")

    def test_rbi_target_uses_common_input_date_fallback(self):
        env = {
            "BATCH_TARGET": "RBI",
            "INPUT_DATE": "2026-05-31",
        }

        with patch("batch_main.main.run_batch") as run_batch:
            Main(env=env).run()

        run_batch.assert_called_once_with(input_date="2026-05-31", parser_name="COMBINED")

    def test_er_dose_target_runs_er_dose_batch(self):
        env = {
            "BATCH_TARGET": "ER_DOSE",
            "ER_DOSE_START_TIME": "2026-05-31T00:00:00",
            "ER_DOSE_END_TIME": "2026-06-01T00:00:00",
            "ER_DOSE_CHUNK_SIZE": "20000",
            "ER_DOSE_DB_DSN": "postgresql://user:pass@localhost:5432/db",
        }
        batch = Mock()

        with patch("batch_main.main.PostgresDB") as postgres_db, patch(
            "batch_main.main.ERDoseRepository"
        ) as repo_cls, patch(
            "batch_main.main.ERDoseProcessor",
            return_value=batch,
        ) as processor_cls:
            result = Main(env=env).run()

        self.assertEqual(result, 0)
        postgres_db.assert_called_once_with(dsn="postgresql://user:pass@localhost:5432/db")
        repo_cls.assert_called_once_with(postgres_db.return_value)
        processor_cls.assert_called_once_with(repo_cls.return_value)
        batch.run.assert_called_once_with(
            start_time=datetime(2026, 5, 31, 0, 0, 0),
            end_time=datetime(2026, 6, 1, 0, 0, 0),
            chunk_size=20000,
        )

    def test_er_does_alias_is_supported(self):
        env = {
            "BATCH_TARGET": "ER_DOES",
            "START_TIME": "2026-05-31T00:00:00",
            "END_TIME": "2026-06-01T00:00:00",
            "DATABASE_URL": "postgresql://user:pass@localhost:5432/db",
        }

        with patch("batch_main.main.PostgresDB") as postgres_db, patch(
            "batch_main.main.ERDoseRepository"
        ), patch("batch_main.main.ERDoseProcessor") as processor_cls:
            Main(env=env).run()

        postgres_db.assert_called_once_with(dsn="postgresql://user:pass@localhost:5432/db")
        processor_cls.return_value.run.assert_called_once()

    def test_er_dose_chunk_size_defaults_to_10000(self):
        env = {
            "BATCH_TARGET": "ER_DOSE",
            "ER_DOSE_START_TIME": "2026-05-31T00:00:00",
            "ER_DOSE_END_TIME": "2026-06-01T00:00:00",
            "DATABASE_URL": "postgresql://user:pass@localhost:5432/db",
        }
        batch = Mock()

        with patch("batch_main.main.PostgresDB"), patch(
            "batch_main.main.ERDoseRepository"
        ), patch("batch_main.main.ERDoseProcessor", return_value=batch):
            Main(env=env).run()

        batch.run.assert_called_once_with(
            start_time=datetime(2026, 5, 31, 0, 0, 0),
            end_time=datetime(2026, 6, 1, 0, 0, 0),
            chunk_size=10000,
        )

    def test_er_dose_chunk_size_must_be_positive(self):
        env = {
            "BATCH_TARGET": "ER_DOSE",
            "ER_DOSE_START_TIME": "2026-05-31T00:00:00",
            "ER_DOSE_END_TIME": "2026-06-01T00:00:00",
            "ER_DOSE_CHUNK_SIZE": "0",
            "DATABASE_URL": "postgresql://user:pass@localhost:5432/db",
        }

        with self.assertRaises(ValueError):
            Main(env=env).run()

    def test_er_dose_requires_dsn_or_database_url(self):
        env = {
            "BATCH_TARGET": "ER_DOSE",
            "ER_DOSE_START_TIME": "2026-05-31T00:00:00",
            "ER_DOSE_END_TIME": "2026-06-01T00:00:00",
        }

        with self.assertRaises(ValueError):
            Main(env=env).run()

    def test_unknown_target_raises(self):
        with self.assertRaises(ValueError):
            Main(env={"BATCH_TARGET": "UNKNOWN"}).run()


if __name__ == "__main__":
    unittest.main()
