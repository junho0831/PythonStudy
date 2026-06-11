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

    def test_wafer_id_is_extracted_from_lot_id(self):
        raw = self._raw("loading reticle 'gvhbrtb0v8' for lot id 2111.")
        parsed = parse_dose_error(raw)
        self.assertEqual(parsed.wafer_id, 2111)

    def test_de_err_is_extracted_from_min_de_error(self):
        raw = self._raw("min_de_error=-1.38157 [%] de_max_reexp_lvl=-15 [%] de_err_lvl=-1 [%]")
        parsed = parse_dose_error(raw)
        self.assertEqual(parsed.de_err, Decimal("-1.38157"))

    def test_parse_dw_3411(self):
        contents = "de_err=0.112627 [%] de_warn_lvl=0 [%] eset:112496 [bits]\nfreq=62100 [hz] n_slit=19 mb_enabled=t action_handle=57229 exposure_handle:57366\n[dwme_analysis_log_dose_performance_result]"
        raw = self._raw(contents)
        parsed = parse_dose_error(raw)
        self.assertEqual(parsed.de_err, Decimal("0.112627"))
        self.assertEqual(parsed.n_slit, 19)
        self.assertEqual(parsed.action_handle, 57229)
        self.assertEqual(parsed.exposure_handle, 57366)

    def test_parse_dw_3425(self):
        contents = "de_err=51.8706 [%] de_err_lvl=1 [%] eset:75074.6 [bits]\nfreq=50000 [hz] n_slit=51 mb_enabled=t action_handle=857 exposure_handle:903\n[dwme_analysis_determine_dose_performance_result]"
        raw = self._raw(contents)
        parsed = parse_dose_error(raw)
        self.assertEqual(parsed.de_err, Decimal("51.8706"))
        self.assertEqual(parsed.n_slit, 51)
        self.assertEqual(parsed.action_handle, 857)
        self.assertEqual(parsed.exposure_handle, 903)

    def test_parse_dw_343a(self):
        contents = "min_de_error=-1.38157 [%] de_max_reexp_lvl=-15 [%] de_err_lvl=-1 [%] \neset:115032 [bits] freq=50000 [hz] n_slit=63 mb_enabled=t action_handle=42355 exposure_handle:42417\n[dwme_analysis_determine_dose_performance_result]"
        raw = self._raw(contents)
        parsed = parse_dose_error(raw)
        self.assertEqual(parsed.de_err, Decimal("-1.38157"))
        self.assertEqual(parsed.n_slit, 63)
        self.assertEqual(parsed.action_handle, 42355)
        self.assertEqual(parsed.exposure_handle, 42417)

    def test_parse_dw_343b(self):
        contents = "de_err=2.64948 [%] de_err_lvl=1 [%] eset:124341 [bits]\nfreq=50000 [hz] n_slit=50 mb_enabled=t action_handle=48994 exposure_handle:49100\n[dwme_analysis_determine_dose_performance_result]"
        raw = self._raw(contents)
        parsed = parse_dose_error(raw)
        self.assertEqual(parsed.de_err, Decimal("2.64948"))
        self.assertEqual(parsed.n_slit, 50)
        self.assertEqual(parsed.action_handle, 48994)
        self.assertEqual(parsed.exposure_handle, 49100)

    def test_parse_lo_0061(self):
        contents = "loading reticle 'gvhbrtb0v8' for lot id 2111."
        raw = self._raw(contents)
        parsed = parse_dose_error(raw)
        self.assertEqual(parsed.wafer_id, 2111)

    def test_parse_lo_8166(self):
        contents = "expose image(0) of production wafer(23) for lot(2111) started on chuck(wpxchuck_chuck_id_1)"
        raw = self._raw(contents)
        parsed = parse_dose_error(raw)
        self.assertEqual(parsed.wafer_id, 2111)
        self.assertEqual(parsed.wafer_seq, 23)

    def test_parse_lo_8167(self):
        contents = "expose image(0) of production wafer(23) for lot(2111) finished on chuck(wpxchuck_chuck_id_1)"
        raw = self._raw(contents)
        parsed = parse_dose_error(raw)
        self.assertEqual(parsed.wafer_id, 2111)
        self.assertEqual(parsed.wafer_seq, 23)

    def test_parse_ke_9103(self):
        contents = "die re-exposures have started."
        raw = self._raw(contents)
        parsed = parse_dose_error(raw)
        self.assertIsNone(parsed.de_err)
        self.assertIsNone(parsed.wafer_id)
        self.assertIsNone(parsed.wafer_seq)

    def _raw(self, contents):
        return RawErLog(
            er_date=20260413,
            er_index=1,
            er_line="L1",
            eq_name="EQ1",
            code="DW-3411",
            code_occur_time=datetime(2026, 4, 13, 10, 0, 0),
            belong=None,
            type=None,
            title="Dose warning",
            contents=contents,
        )


if __name__ == "__main__":
    unittest.main()
