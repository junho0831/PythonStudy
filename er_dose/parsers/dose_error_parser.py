from __future__ import annotations

import re
from decimal import Decimal

from er_dose.parsers.base import ParsedErDoseError, RawErLog


_DECIMAL_RE = r"([+-]?\d+(?:\.\d+)?)"
_INT_RE = r"([+-]?\d+)"

_WAFER_SEQ_PATTERNS = [
    r"\bwafer_seq\s*[:=]\s*" + _INT_RE,
    r"\bwafer\s+seq\s*[:=]\s*" + _INT_RE,
    r"\bwafer_id\s*[:=]\s*" + _INT_RE,
    r"\bwafer\s+id\s*[:=]\s*" + _INT_RE,
    r"\bslot_seq\s*[:=]\s*" + _INT_RE,
    r"\bslot\s+seq\s*[:=]\s*" + _INT_RE,
]

_SHOT_SEQ_PATTERNS = [
    r"\bshot_seq\s*[:=]\s*" + _INT_RE,
    r"\bshot\s+seq\s*[:=]\s*" + _INT_RE,
]

_FIELD_SEQ_PATTERNS = [
    r"\bfield_seq\s*[:=]\s*" + _INT_RE,
    r"\bfield\s+seq\s*[:=]\s*" + _INT_RE,
]


class DoseErrorParser:
    """Parser for dw-xxxx dose evaluation warning logs."""

    def supports(self, raw: RawErLog) -> bool:
        code = (raw.code or "").lower()
        contents = raw.contents.lower()
        return code.startswith("dw-") and (
            "dose evaluation" in contents
            or "de_err" in contents
            or "dwdc_eval_determine_dose_performance_result" in contents
        )

    def parse(self, raw: RawErLog) -> ParsedErDoseError | None:
        if not self.supports(raw):
            return None

        contents = raw.contents
        function_name, result_type = self._extract_function_block(contents)
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
            source_exposure_id=self._extract_source_exposure_id(contents),
            action_handle=self._extract_int(contents, r"\baction_handle\s*[:=]\s*" + _INT_RE),
            wafer_seq=self._extract_first_int(contents, _WAFER_SEQ_PATTERNS, minimum=1),
            shot_seq=self._extract_first_int(contents, _SHOT_SEQ_PATTERNS),
            field_seq=self._extract_first_int(contents, _FIELD_SEQ_PATTERNS),
            dose_error=self._extract_decimal(
                contents,
                r"skip\s+the\s+dose\s+evaluation\s+" + _DECIMAL_RE + r"\s*\[%\]",
            ),
            dose_warn_level=self._extract_decimal(
                contents,
                r"exceeds\s+the\s+dose\s+evaluation\s+warning\s+level\s+" + _DECIMAL_RE + r"\s*\[%\]",
            ),
            de_err=self._extract_decimal(contents, r"\bde_err\s*[:=]\s*" + _DECIMAL_RE),
            de_warn_lvl=self._extract_decimal(contents, r"\bde_warn_lvl\s*[:=]\s*" + _DECIMAL_RE),
            eset=self._extract_int(contents, r"\beset\s*[:=]\s*" + _INT_RE),
            freq=self._extract_int(contents, r"\bfreq\s*[:=]\s*" + _INT_RE),
            n_slit=self._extract_int(contents, r"\bn_slit\s*[:=]\s*" + _INT_RE),
            mb_enabled=self._extract_bool(contents, r"\bmb_enabled\s*[:=]\s*(t|f|true|false|1|0)"),
            function_name=function_name,
            result_type=result_type,
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

    def _extract_bool(self, contents: str, pattern: str) -> bool | None:
        match = re.search(pattern, contents, flags=re.IGNORECASE)
        if match is None:
            return None
        value = match.group(1).lower()
        if value in {"t", "true", "1"}:
            return True
        if value in {"f", "false", "0"}:
            return False
        return None

    def _extract_source_exposure_id(self, contents: str) -> int | None:
        patterns = [
            r"\bsource[_\s-]*exposure[_\s-]*id\s*[:=]\s*" + _INT_RE,
            r"\bsource[_\s-]*exposure[_\s-]*handle\s*[:=]\s*" + _INT_RE,
            r"\bsource[_\s-]*exp(?:osure)?[_\s-]*id\s*[:=]\s*" + _INT_RE,
        ]
        return self._extract_first_int(contents, patterns)

    def _extract_function_block(self, contents: str) -> tuple[str | None, str | None]:
        matches = re.findall(r"\[([A-Za-z0-9_]+):([A-Za-z0-9_]+)\]", contents)
        if not matches:
            return None, None
        function_name, result_type = matches[-1]
        return function_name, result_type
