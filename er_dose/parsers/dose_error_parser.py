from __future__ import annotations

import re
from decimal import Decimal

from typing import Any

from er_dose.parsers.base import ParsedErDoseError, RawErLog


# 소수/정수 값을 캡처한다. group(1) 값을 Decimal/int 로 변환해 사용한다.
_DECIMAL = r"([-+]?\d*\.?\d+)"
_INT = r"([-+]?\d+)"

# wafer_id 는 lot(2111), lot id 2111, wafer_id=2111 같은 표기를 모두 허용한다.
_WAFER_ID_PATTERNS = [
    rf"lot\(\s*{_INT}\s*\)",
    rf"lot id\s+{_INT}",
    rf"wafer_id\s*[:=]\s*{_INT}",
    rf"wafer id\s*[:=]\s*{_INT}",
]

# wafer_seq 는 wafer(23), wafer_seq=23, slot_seq=23 같은 표기를 모두 wafer_seq 로 본다.
_WAFER_SEQ_PATTERNS = [
    rf"wafer\(\s*{_INT}\s*\)",
    rf"wafer_seq\s*[:=]\s*{_INT}",
    rf"wafer seq\s*[:=]\s*{_INT}",
    rf"slot_seq\s*[:=]\s*{_INT}",
    rf"slot seq\s*[:=]\s*{_INT}",
]

# dose error 값은 de_err=... 또는 min_de_error=... 에서 추출한다.
_DE_ERR_PATTERNS = [
    rf"de_err\s*[:=]\s*{_DECIMAL}",
    rf"min_de_error\s*[:=]\s*{_DECIMAL}",
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
        exposure_handle = _extract_int(contents, rf"exposure_handle\s*[:=]\s*{_INT}")
        action_handle = _extract_int(contents, rf"action_handle\s*[:=]\s*{_INT}")
        wafer_id = _extract_first_int(contents, _WAFER_ID_PATTERNS, minimum=1)
        wafer_seq = _extract_first_int(contents, _WAFER_SEQ_PATTERNS, minimum=1)
        de_err = _extract_first_decimal(contents, _DE_ERR_PATTERNS)
        n_slit = _extract_int(contents, rf"n_slit\s*[:=]\s*{_INT}")
    elif code_norm.startswith("LO-"):
        wafer_id = _extract_first_int(contents, _WAFER_ID_PATTERNS, minimum=1)
        wafer_seq = _extract_first_int(contents, _WAFER_SEQ_PATTERNS, minimum=1)
    elif code_norm.startswith("KE-"):
        pass

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

def _extract_value(contents: str, pattern: str, type_cast: type) -> Any | None:
    match = re.search(pattern, contents, flags=re.IGNORECASE)
    if match is None:
        return None
    return type_cast(match.group(1))

def _extract_decimal(contents: str, pattern: str) -> Decimal | None:
    return _extract_value(contents, pattern, Decimal)

def _extract_int(contents: str, pattern: str) -> int | None:
    return _extract_value(contents, pattern, int)

def _extract_first_int(contents: str, patterns: list[str], minimum: int | None = None) -> int | None:
    for pattern in patterns:
        value = _extract_int(contents, pattern)
        if value is None:
            continue
        if minimum is not None and value < minimum:
            continue
        return value
    return None

def _extract_first_decimal(contents: str, patterns: list[str]) -> Decimal | None:
    for pattern in patterns:
        value = _extract_decimal(contents, pattern)
        if value is not None:
            return value
    return None
