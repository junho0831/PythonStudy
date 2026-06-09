from __future__ import annotations

import re
from decimal import Decimal

from typing import Any

from er_dose.parsers.base import ParsedErDoseError, RawErLog


# 부호(+/-)가 포함될 수 있는 소수점 숫자 매칭 (예: 0.12, -1.5, 3)
_DECIMAL_RE = r"([+-]?\d+(?:\.\d+)?)"

# 부호(+/-)가 포함될 수 있는 정수 매칭 (예: 44, -10, 0)
_INT_RE = r"([+-]?\d+)"

# wafer_id 추출을 위한 키워드 패턴 목록 (wafer_seq, wafer id, slot_seq 등)
_WAFER_ID_PATTERNS = [
    r"\blot\s*\(\s*" + _INT_RE + r"\s*\)",
    r"\bwafer_id\s*[:=]\s*" + _INT_RE,
    r"\bwafer\s+id\s*[:=]\s*" + _INT_RE,
]

_WAFER_SEQ_PATTERNS = [
    r"\bwafer\s*\(\s*" + _INT_RE + r"\s*\)",
    r"\bwafer_seq\s*[:=]\s*" + _INT_RE,
    r"\bwafer\s+seq\s*[:=]\s*" + _INT_RE,
    r"\bslot_seq\s*[:=]\s*" + _INT_RE,
    r"\bslot\s+seq\s*[:=]\s*" + _INT_RE,
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
        # "exposure_handle: 1234" 또는 "exposure_handle=1234" 패턴에서 정수 추출
        exposure_handle=_extract_int(contents, r"\bexposure_handle\s*[:=]\s*" + _INT_RE),
        # "action_handle: 1234" 또는 "action_handle=1234" 패턴에서 정수 추출
        action_handle=_extract_int(contents, r"\baction_handle\s*[:=]\s*" + _INT_RE),
        # 여러 wafer 관련 키워드 패턴 중 가장 처음 매칭되는 1 이상의 정수 추출
        wafer_id=_extract_first_int(contents, _WAFER_ID_PATTERNS, minimum=1),
        # 여러 wafer_seq 패턴 중 가장 처음 매칭되는 1 이상의 정수 추출
        wafer_seq=_extract_first_int(contents, _WAFER_SEQ_PATTERNS, minimum=1),
        # "de_err: 0.123" 또는 "de_err=0.123" 패턴에서 소수(Decimal) 추출
        de_err=_extract_decimal(contents, r"\bde_err\s*[:=]\s*" + _DECIMAL_RE),
        # "n_slit: 44" 또는 "n_slit=44" 패턴에서 정수 추출
        n_slit=_extract_int(contents, r"\bn_slit\s*[:=]\s*" + _INT_RE),
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
