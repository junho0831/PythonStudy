from __future__ import annotations

from er_dose.raw.raw_base import ParsedErDoseError, RawErLog
from er_dose.common.regex_utils import (
    DECIMAL_RE as DECIMAL_PATTERN,
    INT_RE as INT_PATTERN,
    extract_first_decimal,
    extract_first_int,
    extract_int,
    extract_text,
)


# 소수/정수 값을 캡처한다. group(1) 값을 Decimal/int 로 변환해 사용한다.
_DECIMAL_RE = DECIMAL_PATTERN
_INT_RE = INT_PATTERN

# lot_seq 는 현재 확인된 lot(2111), lot id 2111, wafer_id=2111 표기만 허용한다.
_LOT_SEQ_PATTERNS = [
    rf"lot\(\s*{_INT_RE}\s*\)",
    rf"lot id\s+{_INT_RE}",
    rf"wafer_id\s*=\s*{_INT_RE}",
]

# wafer_seq 는 현재 확인된 wafer(23) 표기만 허용한다.
_WAFER_SEQ_PATTERNS = [
    rf"wafer\(\s*{_INT_RE}\s*\)",
]

# dose error 값은 de_err=... 또는 min_de_error=... 에서 추출한다.
_DE_ERR_PATTERNS = [
    rf"de_err\s*=\s*{_DECIMAL_RE}",
    rf"min_de_error\s*=\s*{_DECIMAL_RE}",
]


def parse_dose_error(raw: RawErLog) -> ParsedErDoseError:
    """Parse DW-/LO-/KE- dose warning logs using raw code format."""
    contents = raw.contents
    contents_lower = contents.lower()
    code_norm = raw.code.upper() if raw.code else ""

    exposure_handle = None
    action_handle = None
    lot_id = None
    lot_name = None
    lot_seq = None
    wafer_seq = None
    de_err = None
    n_slit = None

    if code_norm.startswith("DW-"):
        if "exposure_handle" in contents_lower:
            exposure_handle = extract_int(contents, rf"exposure_handle\s*:\s*{_INT_RE}")
        if "action_handle" in contents_lower:
            action_handle = extract_int(contents, rf"action_handle\s*=\s*{_INT_RE}")
        if "lot(" in contents_lower or "lot id" in contents_lower or "wafer_id" in contents_lower:
            lot_seq = extract_first_int(contents, _LOT_SEQ_PATTERNS, minimum=1)
        if "wafer(" in contents_lower:
            wafer_seq = extract_first_int(contents, _WAFER_SEQ_PATTERNS, minimum=1)
        if "de_err" in contents_lower or "min_de_error" in contents_lower:
            de_err = extract_first_decimal(contents, _DE_ERR_PATTERNS)
        if "n_slit" in contents_lower:
            n_slit = extract_int(contents, rf"n_slit\s*=\s*{_INT_RE}")
    elif code_norm.startswith("LO-"):
        if code_norm == "LO-0050" and "lot '" in contents_lower:
            lot_id = extract_text(contents, r"lot\s+'([^']+)'")
            lot_name = lot_id.split(".", maxsplit=1)[0] if lot_id is not None else None
        if "lot(" in contents_lower or "lot id" in contents_lower or "wafer_id" in contents_lower:
            lot_seq = extract_first_int(contents, _LOT_SEQ_PATTERNS, minimum=1)
        if "wafer(" in contents_lower:
            wafer_seq = extract_first_int(contents, _WAFER_SEQ_PATTERNS, minimum=1)

    return ParsedErDoseError(
        eq_name=raw.eq_name,
        code=raw.code,
        code_occur_time=raw.code_occur_time,
        title=raw.title,
        contents=raw.contents,
        exposure_handle=exposure_handle,
        action_handle=action_handle,
        lot_id=lot_id,
        lot_name=lot_name,
        lot_seq=lot_seq,
        wafer_seq=wafer_seq,
        de_err=de_err,
        n_slit=n_slit,
    )
