from __future__ import annotations

from er_dose.raw.base import ParsedErDoseError, RawErLog
from er_dose.common.regex_utils import (
    DECIMAL_RE as DECIMAL_PATTERN,
    INT_RE as INT_PATTERN,
    extract_first_decimal,
    extract_first_int,
    extract_int,
)


# 소수/정수 값을 캡처한다. group(1) 값을 Decimal/int 로 변환해 사용한다.
_DECIMAL_RE = DECIMAL_PATTERN
_INT_RE = INT_PATTERN

# wafer_id 는 lot(2111), lot id 2111, wafer_id=2111 같은 표기를 모두 허용한다.
_WAFER_ID_PATTERNS = [
    rf"lot\(\s*{_INT_RE}\s*\)",
    rf"lot id\s+{_INT_RE}",
    rf"wafer_id\s*[:=]\s*{_INT_RE}",
    rf"wafer id\s*[:=]\s*{_INT_RE}",
]

# wafer_seq 는 wafer(23), wafer_seq=23, slot_seq=23 같은 표기를 모두 wafer_seq 로 본다.
_WAFER_SEQ_PATTERNS = [
    rf"wafer\(\s*{_INT_RE}\s*\)",
    rf"wafer_seq\s*[:=]\s*{_INT_RE}",
    rf"wafer seq\s*[:=]\s*{_INT_RE}",
    rf"slot_seq\s*[:=]\s*{_INT_RE}",
    rf"slot seq\s*[:=]\s*{_INT_RE}",
]

# dose error 값은 de_err=... 또는 min_de_error=... 에서 추출한다.
_DE_ERR_PATTERNS = [
    rf"de_err\s*[:=]\s*{_DECIMAL_RE}",
    rf"min_de_error\s*[:=]\s*{_DECIMAL_RE}",
]


def parse_dose_error(raw: RawErLog) -> ParsedErDoseError:
    """Parse DW-/LO-/KE- dose warning logs using raw code format."""
    contents = raw.contents
    code_norm = raw.code if raw.code else ""

    exposure_handle = None
    action_handle = None
    wafer_id = None
    wafer_seq = None
    de_err = None
    n_slit = None

    if code_norm.startswith("DW-"):
        exposure_handle = extract_int(contents, rf"exposure_handle\s*[:=]\s*{_INT_RE}")
        action_handle = extract_int(contents, rf"action_handle\s*[:=]\s*{_INT_RE}")
        wafer_id = extract_first_int(contents, _WAFER_ID_PATTERNS, minimum=1)
        wafer_seq = extract_first_int(contents, _WAFER_SEQ_PATTERNS, minimum=1)
        de_err = extract_first_decimal(contents, _DE_ERR_PATTERNS)
        n_slit = extract_int(contents, rf"n_slit\s*[:=]\s*{_INT_RE}")
    elif code_norm.startswith("LO-"):
        wafer_id = extract_first_int(contents, _WAFER_ID_PATTERNS, minimum=1)
        wafer_seq = extract_first_int(contents, _WAFER_SEQ_PATTERNS, minimum=1)

    return ParsedErDoseError(
        er_date=raw.er_date,
        er_index=raw.er_index,
        er_line=raw.er_line,
        eq_name=raw.eq_name,
        code=raw.code,
        code_occur_time=raw.code_occur_time,
        belong=raw.belong,
        type=raw.type,
        title=raw.title,
        contents=raw.contents,
        exposure_handle=exposure_handle,
        action_handle=action_handle,
        wafer_id=wafer_id,
        wafer_seq=wafer_seq,
        de_err=de_err,
        n_slit=n_slit,
    )


