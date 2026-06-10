from __future__ import annotations

import re
from decimal import Decimal

from typing import Any

from er_dose.parsers.base import (
    ParsedErDoseError, 
    RawErLog,
    DW_TARGET_CODES_NORM,
    LO_TARGET_CODES_NORM,
    KE_TARGET_CODES_NORM,
)

# 숫자 매칭 정규식을 알아보기 쉽게 간소화
_DECIMAL = r"([-+]?\d*\.?\d+)"
_INT = r"([-+]?\d+)"

_WAFER_ID_PATTERNS = [
    re.compile(rf"lot\(\s*{_INT}\s*\)", flags=re.IGNORECASE),
    re.compile(rf"lot id\s+{_INT}", flags=re.IGNORECASE),
    re.compile(rf"wafer_id\s*[:=]\s*{_INT}", flags=re.IGNORECASE),
    re.compile(rf"wafer id\s*[:=]\s*{_INT}", flags=re.IGNORECASE),
]

_WAFER_SEQ_PATTERNS = [
    re.compile(rf"wafer\(\s*{_INT}\s*\)", flags=re.IGNORECASE),
    re.compile(rf"wafer_seq\s*[:=]\s*{_INT}", flags=re.IGNORECASE),
    re.compile(rf"wafer seq\s*[:=]\s*{_INT}", flags=re.IGNORECASE),
    re.compile(rf"slot_seq\s*[:=]\s*{_INT}", flags=re.IGNORECASE),
    re.compile(rf"slot seq\s*[:=]\s*{_INT}", flags=re.IGNORECASE),
]

_DE_ERR_PATTERNS = [
    re.compile(rf"de_err\s*[:=]\s*{_DECIMAL}", flags=re.IGNORECASE),
    re.compile(rf"min_de_error\s*[:=]\s*{_DECIMAL}", flags=re.IGNORECASE),
]

_EXPOSURE_HANDLE_PATTERN = re.compile(rf"exposure_handle\s*[:=]\s*{_INT}", flags=re.IGNORECASE)
_ACTION_HANDLE_PATTERN = re.compile(rf"action_handle\s*[:=]\s*{_INT}", flags=re.IGNORECASE)
_N_SLIT_PATTERN = re.compile(rf"n_slit\s*[:=]\s*{_INT}", flags=re.IGNORECASE)


def parse_dose_error(raw: RawErLog) -> ParsedErDoseError:
    """Parse dw-xxxx dose evaluation warning logs."""
    contents = raw.contents
    code_norm = raw.code.upper().replace("-", "") if raw.code else ""
    
    exposure_handle = None
    action_handle = None
    wafer_id = None
    wafer_seq = None
    de_err = None
    n_slit = None

    if code_norm in DW_TARGET_CODES_NORM:
        exposure_handle = _extract_int_compiled(contents, _EXPOSURE_HANDLE_PATTERN)
        action_handle = _extract_int_compiled(contents, _ACTION_HANDLE_PATTERN)
        wafer_id = _extract_first_int_compiled(contents, _WAFER_ID_PATTERNS, minimum=1)
        wafer_seq = _extract_first_int_compiled(contents, _WAFER_SEQ_PATTERNS, minimum=1)
        de_err = _extract_first_decimal_compiled(contents, _DE_ERR_PATTERNS)
        n_slit = _extract_int_compiled(contents, _N_SLIT_PATTERN)
    elif code_norm in LO_TARGET_CODES_NORM:
        wafer_id = _extract_first_int_compiled(contents, _WAFER_ID_PATTERNS, minimum=1)
        wafer_seq = _extract_first_int_compiled(contents, _WAFER_SEQ_PATTERNS, minimum=1)
    elif code_norm in KE_TARGET_CODES_NORM:
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

def _extract_value_compiled(contents: str, pattern: re.Pattern, type_cast: type) -> Any | None:
    match = pattern.search(contents)
    if match is None:
        return None
    return type_cast(match.group(1))

def _extract_decimal_compiled(contents: str, pattern: re.Pattern) -> Decimal | None:
    return _extract_value_compiled(contents, pattern, Decimal)

def _extract_int_compiled(contents: str, pattern: re.Pattern) -> int | None:
    return _extract_value_compiled(contents, pattern, int)

def _extract_first_int_compiled(contents: str, patterns: list[re.Pattern], minimum: int | None = None) -> int | None:
    for pattern in patterns:
        value = _extract_int_compiled(contents, pattern)
        if value is None:
            continue
        if minimum is not None and value < minimum:
            continue
        return value
    return None

def _extract_first_decimal_compiled(contents: str, patterns: list[re.Pattern]) -> Decimal | None:
    for pattern in patterns:
        value = _extract_decimal_compiled(contents, pattern)
        if value is not None:
            return value
    return None
