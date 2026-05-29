from __future__ import annotations

import re
from decimal import Decimal

from er_dose.parsers.base import ParsedErDoseError, RawErLog


_DECIMAL_RE = r"([+-]?\d+(?:\.\d+)?)"
_INT_RE = r"([+-]?\d+)"


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
            er_line=raw.er_line,
            eq_name=raw.eq_name,
            code=raw.code,
            code_occur_time=raw.code_occur_time,
            code_occur_time_raw=raw.code_occur_time_raw,
            log_source=raw.log_source,
            exposure_handle=self._extract_int(contents, r"\bexposure_handle\s*[:=]\s*" + _INT_RE),
            action_handle=self._extract_int(contents, r"\baction_handle\s*[:=]\s*" + _INT_RE),
            wafer_seq=None,
            shot_seq=None,
            field_seq=None,
            repair_yn=None,
            repair_result=None,
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
            parser_version="v1",
            parsing_status="SUCCESS",
            parsing_error=None,
            raw_contents=raw.contents,
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

    def _extract_function_block(self, contents: str) -> tuple[str | None, str | None]:
        matches = re.findall(r"\[([A-Za-z0-9_]+):([A-Za-z0-9_]+)\]", contents)
        if not matches:
            return None, None
        function_name, result_type = matches[-1]
        return function_name, result_type
