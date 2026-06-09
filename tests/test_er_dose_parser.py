from __future__ import annotations

import unittest
from datetime import datetime
from decimal import Decimal

from er_dose.parsers.base import RawErLog
from er_dose.parsers.dose_error_parser import parse_dose_error


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

        parsed = parse_dose_error(raw)

        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.exposure_handle, 2631)
        self.assertEqual(parsed.action_handle, 2625)
        self.assertEqual(parsed.de_err, Decimal("0.0461075"))
        self.assertEqual(parsed.n_slit, 44)

    def test_missing_nullable_fields_return_none(self):
        raw = self._raw("system warning: dw-3411 skip the dose evaluation 0.1 [%]")

        parsed = parse_dose_error(raw)

        self.assertIsNotNone(parsed)
        self.assertIsNone(parsed.exposure_handle)
        self.assertIsNone(parsed.action_handle)
        self.assertIsNone(parsed.de_err)

    def test_wafer_id_is_extracted(self):
        raw = self._raw(
            """system warning: dw-3411 skip the dose evaluation 0.1 [%]
wafer_id=1"""
        )

        parsed = parse_dose_error(raw)

        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.wafer_id, 1)

    def test_slot_seq_maps_to_wafer_seq(self):
        raw = self._raw(
            """system warning: dw-3411 skip the dose evaluation 0.1 [%]
slot_seq=3"""
        )

        parsed = parse_dose_error(raw)

        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.wafer_seq, 3)
        self.assertIsNone(parsed.wafer_id)

    def test_wafer_no_is_not_treated_as_wafer_id(self):
        raw = self._raw(
            """system warning: dw-3411 skip the dose evaluation 0.1 [%]
wafer_no=7"""
        )

        parsed = parse_dose_error(raw)

        self.assertIsNotNone(parsed)
        self.assertIsNone(parsed.wafer_id)

    def _raw(self, contents):
        return RawErLog(
            er_date=20260413,
            er_index=1,
            er_line="L1",
            eq_name="EQ1",
            code="dw-3411",
            code_occur_time=datetime(2026, 4, 13, 10, 0, 0),
            belong=None,
            type=None,
            title="Dose warning",
            contents=contents,
        )


if __name__ == "__main__":
    unittest.main()
