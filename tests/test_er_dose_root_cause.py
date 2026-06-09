from __future__ import annotations

from datetime import datetime, timezone, timedelta
from decimal import Decimal

from er_dose.root_cause import parse_root_cause


SAMPLE_EUV_CONTENTS = r"""dose error detected in file: adecetdcdata_fdd_lc_eei_scanner_dose_error_event_20260504_180529_3502+0900.zip.\nroot cause : plasma oscillations\nexposure id : 25415\ntime : 2026-05-04t18:05:29.297624+09:00\nexposure length : 0.0888 [s]\nduty cycle : 99.68 [perc]\nmin. dose error : -2.02 [perc]\nmax. dose error : 0.74 [perc]\non drop euv energy : 5.24 [mj]\non drop pp energy : 98.7 [mj]\non drop mp energy : 404.3 [mj]\non drop pp dlgc=1 : 111.2 [mj]\non drop mp dlgc=1: 420.3 [mj]\nbi-cell y 3sigma : 0.227 [hz]\nfdsc y error : 7.7 [um]\nfdsc y 3sigma : 5.9 [um]\nmax. cross. interval : 20.12 [us]\nxint 3sigma : 0.128 [us]\neuv 3sigma : 1.78 [mj]\npulses_euv<0.6dt_tot : 143 [pulsecount]\nfed pulses : 107 [pulsecount]\nl2dx maxce : -2.31 [um]\nl2dy maxce : 7.90 [um]\nsensitivity at l2dx maxce : -0.88 [perc/um2]\nsensitivity at l2dy maxce : -0.05 [perc/um2]\ndose margin : 10.15 [perc]\nl2dx qc etdc 3sigma : 5.96 [m]\nl2dx qc etdc median : -2.45 [m]\nl2dy qc etdc 3sigma : 10.60 [m]\nl2dy qc etdc median : 7.53 [m]\nrbdy peak frequency hf : 1100.01 [hz]\nrbdy peak frequency lf : 40.00 [hz]\nrbdy peak frequency mf : 300.00 [hz]\nrbdy peak power hf : 1.50 [um2]\nrbdy qc etdc 3sigma : 5.97 [um]\nrbdy total power lf : 0.46 [um2]\nrbdy total power mf : 3.56 [um2]\nsoftware version : 2.0 [nxe3400 mv 250w]"""


def test_parse_euv_root_cause_contents():
    parsed = parse_root_cause(SAMPLE_EUV_CONTENTS)

    assert parsed is not None
    assert parsed.source_file_name == "adecetdcdata_fdd_lc_eei_scanner_dose_error_event_20260504_180529_3502+0900.zip"
    assert parsed.root_cause_message == "plasma oscillations"
    assert parsed.root_cause_code == "plasma_oscillations"
    assert parsed.source_exposure_id == 25415
    assert parsed.source_code_occur_time == datetime(
        2026,
        5,
        4,
        18,
        5,
        29,
        297624,
        tzinfo=timezone(timedelta(hours=9)),
    )
    assert parsed.exposure_length == Decimal("0.0888")
    assert parsed.duty_cycle == Decimal("99.68")
    assert parsed.min_dose_error == Decimal("-2.02")
    assert parsed.max_dose_error == Decimal("0.74")
    assert parsed.dose_error == Decimal("-2.02")
    assert parsed.on_drop_mp_dlgc1 == Decimal("420.3")
    assert parsed.pulses_euv_lt_0_6dt_tot == 143
    assert parsed.fed_pulses == 107
    assert parsed.rbdy_peak_frequency_hf == Decimal("1100.01")
    assert parsed.software_version == "2.0 [nxe3400 mv 250w]"


def test_non_root_cause_contents_are_skipped():
    assert parse_root_cause("system info: normal message") is None
