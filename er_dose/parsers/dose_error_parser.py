from __future__ import annotations

import re
from decimal import Decimal

from typing import Any

from er_dose.parsers.base import ParsedErDoseError, RawErLog


# 숫자 매칭 정규식을 알아보기 쉽게 간소화
_DECIMAL = r"([-+]?\d*\.?\d+)"
_INT = r"([-+]?\d+)"

_WAFER_ID_PATTERNS = [
    rf"lot\(\s*{_INT}\s*\)",
    rf"lot id\s+{_INT}",
    rf"wafer_id\s*[:=]\s*{_INT}",
    rf"wafer id\s*[:=]\s*{_INT}",
]

_WAFER_SEQ_PATTERNS = [
    rf"wafer\(\s*{_INT}\s*\)",
    rf"wafer_seq\s*[:=]\s*{_INT}",
    rf"wafer seq\s*[:=]\s*{_INT}",
    rf"slot_seq\s*[:=]\s*{_INT}",
    rf"slot seq\s*[:=]\s*{_INT}",
]

_DE_ERR_PATTERNS = [
    rf"de_err\s*[:=]\s*{_DECIMAL}",
    rf"min_de_error\s*[:=]\s*{_DECIMAL}",
]


def parse_dose_error(raw: RawErLog) -> ParsedErDoseError:
    """Parse dw-xxxx dose evaluation warning logs."""
    contents = raw.contents
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
        # exposure_handle: 1234 또는 exposure_handle=1234
        exposure_handle=_extract_int(contents, rf"exposure_handle\s*[:=]\s*{_INT}"),
        # action_handle: 1234 또는 action_handle=1234
        action_handle=_extract_int(contents, rf"action_handle\s*[:=]\s*{_INT}"),
        # 정의된 패턴 목록 순서대로 매칭되는 값 추출
        wafer_id=_extract_first_int(contents, _WAFER_ID_PATTERNS, minimum=1),
        wafer_seq=_extract_first_int(contents, _WAFER_SEQ_PATTERNS, minimum=1),
        de_err=_extract_first_decimal(contents, _DE_ERR_PATTERNS),
        # n_slit: 44 또는 n_slit=44
        n_slit=_extract_int(contents, rf"n_slit\s*[:=]\s*{_INT}"),
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
