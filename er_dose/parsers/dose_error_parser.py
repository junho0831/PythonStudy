from __future__ import annotations

import re
from decimal import Decimal

from er_dose.parsers.base import ParsedErDoseError, RawErLog


_DECIMAL_RE = r"([+-]?\d+(?:\.\d+)?)"
_INT_RE = r"([+-]?\d+)"
_SUPPORT_KEYWORDS = (
    "dose evaluation",
    "de_err",
    "dwdc_eval_determine_dose_performance_result",
)

_WAFER_ID_PATTERNS = [
    r"\bwafer_seq\s*[:=]\s*" + _INT_RE,
    r"\bwafer\s+seq\s*[:=]\s*" + _INT_RE,
    r"\bwafer_id\s*[:=]\s*" + _INT_RE,
    r"\bwafer\s+id\s*[:=]\s*" + _INT_RE,
    r"\bslot_seq\s*[:=]\s*" + _INT_RE,
    r"\bslot\s+seq\s*[:=]\s*" + _INT_RE,
]


class DoseErrorParser:
    """Parser for dw-xxxx dose evaluation warning logs."""

    def supports(self, raw: RawErLog) -> bool:
        code = (raw.code or "").lower()
        contents = raw.contents.lower()
        return code.startswith("dw-") and any(keyword in contents for keyword in _SUPPORT_KEYWORDS)

    def parse(self, raw: RawErLog) -> ParsedErDoseError | None:
        if not self.supports(raw):
            return None

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
            exposure_handle=self._extract_int(contents, r"\bexposure_handle\s*[:=]\s*" + _INT_RE),
            action_handle=self._extract_int(contents, r"\baction_handle\s*[:=]\s*" + _INT_RE),
            wafer_id=self._extract_first_int(contents, _WAFER_ID_PATTERNS, minimum=1),
            de_err=self._extract_decimal(contents, r"\bde_err\s*[:=]\s*" + _DECIMAL_RE),
            n_slit=self._extract_int(contents, r"\bn_slit\s*[:=]\s*" + _INT_RE),
        )

    def _extract_decimal(self, contents: str, pattern: str) -> Decimal | None:
        match = re.search(pattern, contents, flags=re.IGNORECASE)
        if match is None:
            return None
        return Decimal(match.group(1))

    def _extract_int(self, contents: str, pattern: str) -> int | None:
        match = re.search(pattern, contents, flags=re.IGNORECASE)
        if match is None:
            return None
        return int(match.group(1))

    def _extract_first_int(self, contents: str, patterns: list[str], minimum: int | None = None) -> int | None:
        for pattern in patterns:
            value = self._extract_int(contents, pattern)
            if value is None:
                continue
            if minimum is not None and value < minimum:
                continue
            return value
        return None
