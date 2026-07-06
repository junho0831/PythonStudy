from __future__ import annotations

from decimal import Decimal

from er_dose.euv.euv_base import ParsedEuvRootCause
from er_dose.common.regex_utils import (
    DECIMAL_RE,
    INT_RE,
    extract_datetime_isoformat,
    extract_decimal,
    extract_int,
    extract_text,
    field_pattern,
    normalize_multiline_text,
    to_snake_code,
)

def parse_root_cause(contents: str) -> ParsedEuvRootCause | None:
    """Parse er_data_raw_euv contents for dose error root cause details."""
    if not contents:
        return None
    normalized = normalize_multiline_text(contents)
    if "dose error detected in file:" not in normalized.lower() or "root cause" not in normalized.lower():
        return None

    root_cause_message = extract_text(normalized, r"\broot\s+cause\s*:\s*(.+)", trim_trailing_period=True)
    min_dose_error = extract_decimal(normalized, r"\bmin\.\s*dose\s+error\s*:\s*" + DECIMAL_RE)
    max_dose_error = extract_decimal(normalized, r"\bmax\.\s*dose\s+error\s*:\s*" + DECIMAL_RE)

    return ParsedEuvRootCause(
        source_file_name=extract_text(normalized, r"\bdose\s+error\s+detected\s+in\s+file\s*:\s*(.+?)\s*\.?\s*$", trim_trailing_period=True),
        source_exposure_id=extract_int(normalized, r"\bexposure\s+id\s*:\s*" + INT_RE),
        source_code_occur_time=extract_datetime_isoformat(normalized, r"\btime\s*:\s*([^\s]+)"),
        root_cause_code=to_snake_code(root_cause_message),
        root_cause_message=root_cause_message,
        exposure_length=_extract_decimal_field(normalized, "exposure length"),
        duty_cycle=_extract_decimal_field(normalized, "duty cycle"),
        min_dose_error=min_dose_error,
        max_dose_error=max_dose_error,
        on_drop_euv_energy=_extract_decimal_field(normalized, "on drop euv energy"),
        on_drop_pp_energy=_extract_decimal_field(normalized, "on drop pp energy"),
        on_drop_mp_energy=_extract_decimal_field(normalized, "on drop mp energy"),
        on_drop_pp_dlgc1=_extract_decimal_field(normalized, "on drop pp dlgc=1"),
        on_drop_mp_dlgc1=_extract_decimal_field(normalized, "on drop mp dlgc=1"),
        bi_cell_y_3sigma=_extract_decimal_field(normalized, "bi-cell y 3sigma"),
        fdsc_y_error=_extract_decimal_field(normalized, "fdsc y error"),
        fdsc_y_3sigma=_extract_decimal_field(normalized, "fdsc y 3sigma"),
        max_cross_interval=_extract_decimal_field(normalized, "max. cross. interval"),
        xint_3sigma=_extract_decimal_field(normalized, "xint 3sigma"),
        euv_3sigma=_extract_decimal_field(normalized, "euv 3sigma"),
        pulses_euv_0_6dt_tot=_extract_int_field(normalized, "pulses_euv<0.6dt_tot"),
        fed_pulses=_extract_int_field(normalized, "fed pulses"),
        l2dx_maxce=_extract_decimal_field(normalized, "l2dx maxce"),
        l2dy_maxce=_extract_decimal_field(normalized, "l2dy maxce"),
        sensitivity_at_l2dx_maxce=_extract_decimal_field(normalized, "sensitivity at l2dx maxce"),
        sensitivity_at_l2dy_maxce=_extract_decimal_field(normalized, "sensitivity at l2dy maxce"),
        dose_margin=_extract_decimal_field(normalized, "dose margin"),
        l2dx_qc_etdc_3sigma=_extract_decimal_field(normalized, "l2dx qc etdc 3sigma"),
        l2dx_qc_etdc_median=_extract_decimal_field(normalized, "l2dx qc etdc median"),
        l2dy_qc_etdc_3sigma=_extract_decimal_field(normalized, "l2dy qc etdc 3sigma"),
        l2dy_qc_etdc_median=_extract_decimal_field(normalized, "l2dy qc etdc median"),
        rbdy_peak_frequency_hf=_extract_decimal_field(normalized, "rbdy peak frequency hf"),
        rbdy_peak_frequency_lf=_extract_decimal_field(normalized, "rbdy peak frequency lf"),
        rbdy_peak_frequency_mf=_extract_decimal_field(normalized, "rbdy peak frequency mf"),
        rbdy_peak_power_hf=_extract_decimal_field(normalized, "rbdy peak power hf"),
        rbdy_qc_etdc_3sigma=_extract_decimal_field(normalized, "rbdy qc etdc 3sigma"),
        rbdy_total_power_lf=_extract_decimal_field(normalized, "rbdy total power lf"),
        rbdy_total_power_mf=_extract_decimal_field(normalized, "rbdy total power mf"),
        software_version=extract_text(normalized, r"\bsoftware\s+version\s*:\s*(.+)", trim_trailing_period=True),
    )



def _extract_decimal_field(contents: str, label: str) -> Decimal | None:
    return extract_decimal(contents, field_pattern(label, DECIMAL_RE))



def _extract_int_field(contents: str, label: str) -> int | None:
    return extract_int(contents, field_pattern(label, INT_RE))
