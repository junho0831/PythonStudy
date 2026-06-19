from __future__ import annotations

import unittest
from unittest.mock import patch

import main


class RootMainTest(unittest.TestCase):
    def test_cli_er_dose_raw_maps_date_to_er_dose_raw_target_date(self):
        env = {"ER_DOSE_DB_DSN": "postgresql://user:pass@localhost:5432/db"}

        with patch("main.Main") as main_cls:
            main_cls.return_value.run.return_value = 0
            result = main.main(["--date", "2026-06-16", "--parser", "ER_DOSE_RAW"], env=env)

        self.assertEqual(result, 0)
        runtime_env = main_cls.call_args.kwargs["env"]
        self.assertEqual(runtime_env["BATCH_TARGET"], "ER_DOSE_RAW")
        self.assertEqual(runtime_env["ER_DOSE_RAW_TARGET_DATE"], "2026-06-16")

    def test_cli_er_dose_euv_maps_date_to_er_dose_euv_target_date(self):
        with patch("main.Main") as main_cls:
            main_cls.return_value.run.return_value = 0
            result = main.main(["--date", "2026-06-16", "--parser", "ER_DOSE_EUV"], env={})

        self.assertEqual(result, 0)
        runtime_env = main_cls.call_args.kwargs["env"]
        self.assertEqual(runtime_env["BATCH_TARGET"], "ER_DOSE_EUV")
        self.assertEqual(runtime_env["ER_DOSE_EUV_TARGET_DATE"], "2026-06-16")

    def test_cli_rbi_maps_to_rbi_env(self):
        with patch("main.Main") as main_cls:
            main_cls.return_value.run.return_value = 0
            result = main.main(["--date", "2026-06-16", "--parser", "COMBINED"], env={})

        self.assertEqual(result, 0)
        runtime_env = main_cls.call_args.kwargs["env"]
        self.assertEqual(runtime_env["BATCH_TARGET"], "RBI")
        self.assertEqual(runtime_env["RBI_INPUT_DATE"], "2026-06-16")
        self.assertEqual(runtime_env["RBI_PARSER"], "COMBINED")


if __name__ == "__main__":
    unittest.main()
