from __future__ import annotations

import unittest
from datetime import datetime
from decimal import Decimal

from er_dose.parsers.base import RawErLog
from er_dose.parsers.registry import parse_raw_er_log


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


class ERDoseParserTest(unittest.TestCase):
    def test_parse_sample_dose_error_log(self):
        raw = self._raw(SAMPLE_CONTENTS)

        parsed = parse_raw_er_log(raw)

        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.exposure_handle, 2631)
        self.assertEqual(parsed.action_handle, 2625)
        self.assertEqual(parsed.dose_error, Decimal("0.0461075"))
        self.assertEqual(parsed.dose_warn_level, Decimal("0"))
        self.assertEqual(parsed.de_err, Decimal("0.0461075"))
        self.assertEqual(parsed.de_warn_lvl, Decimal("0"))
        self.assertEqual(parsed.eset, 89898)
        self.assertEqual(parsed.freq, 50000)
        self.assertEqual(parsed.n_slit, 44)
        self.assertEqual(parsed.mb_enabled, True)
        self.assertEqual(parsed.function_name, "dwdc_eval_determine_dose_performance_result")
        self.assertEqual(parsed.result_type, "dwdc_warn_total_dose")

    def test_bool_false_is_supported(self):
        raw = self._raw(SAMPLE_CONTENTS.replace("mb_enabled=t", "mb_enabled=f"))

        parsed = parse_raw_er_log(raw)

        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.mb_enabled, False)

    def test_missing_nullable_fields_return_none(self):
        raw = self._raw("system warning: dw-3411 skip the dose evaluation 0.1 [%]")

        parsed = parse_raw_er_log(raw)

        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.dose_error, Decimal("0.1"))
        self.assertIsNone(parsed.exposure_handle)
        self.assertIsNone(parsed.function_name)

    def test_non_dose_log_is_skipped(self):
        raw = self._raw("system info: dw-3411 normal message")

        parsed = parse_raw_er_log(raw)

        self.assertIsNone(parsed)

    def _raw(self, contents):
        return RawErLog(
            er_line="L1",
            eq_name="EQ1",
            code="dw-3411",
            code_occur_time=datetime(2026, 4, 13, 10, 0, 0),
            code_occur_time_raw="2026-04-13 10:00:00.000000",
            log_source=None,
            contents=contents,
        )


if __name__ == "__main__":
    unittest.main()
