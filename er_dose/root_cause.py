from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


_DECIMAL_RE = r"([+-]?\d+(?:\.\d+)?)"


@dataclass(frozen=True)
class ParsedEuvRootCause:
    source_file_name: str | None
    source_exposure_id: int | None
    source_code_occur_time: datetime | None
    root_cause_code: str | None
    root_cause_message: str | None
    exposure_length: Decimal | None
    duty_cycle: Decimal | None
    min_dose_error: Decimal | None
    max_dose_error: Decimal | None
    dose_error: Decimal | None
    on_drop_euv_energy: Decimal | None
    on_drop_pp_energy: Decimal | None
    on_drop_mp_energy: Decimal | None
    on_drop_pp_dlgc1: Decimal | None
    on_drop_mp_dlgc1: Decimal | None
    bi_cell_y_3sigma: Decimal | None
    fdsc_y_error: Decimal | None
    fdsc_y_3sigma: Decimal | None
    max_cross_interval: Decimal | None
    xint_3sigma: Decimal | None
    euv_3sigma: Decimal | None
    pulses_euv_lt_0_6dt_tot: int | None
    fed_pulses: int | None
    l2dx_maxce: Decimal | None
    l2dy_maxce: Decimal | None
    sensitivity_at_l2dx_maxce: Decimal | None
    sensitivity_at_l2dy_maxce: Decimal | None
    dose_margin: Decimal | None
    l2dx_qc_etdc_3sigma: Decimal | None
    l2dx_qc_etdc_median: Decimal | None
    l2dy_qc_etdc_3sigma: Decimal | None
    l2dy_qc_etdc_median: Decimal | None
    rbdy_peak_frequency_hf: Decimal | None
    rbdy_peak_frequency_lf: Decimal | None
    rbdy_peak_frequency_mf: Decimal | None
    rbdy_peak_power_hf: Decimal | None
    rbdy_qc_etdc_3sigma: Decimal | None
    rbdy_total_power_lf: Decimal | None
    rbdy_total_power_mf: Decimal | None
    software_version: str | None


class EuvRootCauseParser:
    """Parser for er_data_raw_euv contents with dose error root cause details."""

    def supports(self, contents: str | None) -> bool:
        if not contents:
            return False
        normalized = self._normalize(contents).lower()
        return "dose error detected in file:" in normalized and "root cause" in normalized

    def parse(self, contents: str) -> ParsedEuvRootCause | None:
        if not self.supports(contents):
            return None

        normalized = self._normalize(contents)
        root_cause_message = self._extract_text(normalized, r"\broot\s+cause\s*:\s*(.+)")
        min_dose_error = self._extract_decimal(normalized, r"\bmin\.\s*dose\s+error\s*:\s*" + _DECIMAL_RE)
        max_dose_error = self._extract_decimal(normalized, r"\bmax\.\s*dose\s+error\s*:\s*" + _DECIMAL_RE)

        return ParsedEuvRootCause(
            source_file_name=self._extract_text(normalized, r"\bdose\s+error\s+detected\s+in\s+file\s*:\s*(.+?)\s*\.?\s*$"),
            source_exposure_id=self._extract_int(normalized, r"\bexposure\s+id\s*:\s*([+-]?\d+)"),
            source_code_occur_time=self._extract_time(normalized),
            root_cause_code=self._to_code(root_cause_message),
            root_cause_message=root_cause_message,
            exposure_length=self._extract_decimal_field(normalized, "exposure length"),
            duty_cycle=self._extract_decimal_field(normalized, "duty cycle"),
            min_dose_error=min_dose_error,
            max_dose_error=max_dose_error,
            dose_error=self._dominant_dose_error(min_dose_error, max_dose_error),
            on_drop_euv_energy=self._extract_decimal_field(normalized, "on drop euv energy"),
            on_drop_pp_energy=self._extract_decimal_field(normalized, "on drop pp energy"),
            on_drop_mp_energy=self._extract_decimal_field(normalized, "on drop mp energy"),
            on_drop_pp_dlgc1=self._extract_decimal_field(normalized, "on drop pp dlgc=1"),
            on_drop_mp_dlgc1=self._extract_decimal_field(normalized, "on drop mp dlgc=1"),
            bi_cell_y_3sigma=self._extract_decimal_field(normalized, "bi-cell y 3sigma"),
            fdsc_y_error=self._extract_decimal_field(normalized, "fdsc y error"),
            fdsc_y_3sigma=self._extract_decimal_field(normalized, "fdsc y 3sigma"),
            max_cross_interval=self._extract_decimal_field(normalized, "max. cross. interval"),
            xint_3sigma=self._extract_decimal_field(normalized, "xint 3sigma"),
            euv_3sigma=self._extract_decimal_field(normalized, "euv 3sigma"),
            pulses_euv_lt_0_6dt_tot=self._extract_int_field(normalized, "pulses_euv<0.6dt_tot"),
            fed_pulses=self._extract_int_field(normalized, "fed pulses"),
            l2dx_maxce=self._extract_decimal_field(normalized, "l2dx maxce"),
            l2dy_maxce=self._extract_decimal_field(normalized, "l2dy maxce"),
            sensitivity_at_l2dx_maxce=self._extract_decimal_field(normalized, "sensitivity at l2dx maxce"),
            sensitivity_at_l2dy_maxce=self._extract_decimal_field(normalized, "sensitivity at l2dy maxce"),
            dose_margin=self._extract_decimal_field(normalized, "dose margin"),
            l2dx_qc_etdc_3sigma=self._extract_decimal_field(normalized, "l2dx qc etdc 3sigma"),
            l2dx_qc_etdc_median=self._extract_decimal_field(normalized, "l2dx qc etdc median"),
            l2dy_qc_etdc_3sigma=self._extract_decimal_field(normalized, "l2dy qc etdc 3sigma"),
            l2dy_qc_etdc_median=self._extract_decimal_field(normalized, "l2dy qc etdc median"),
            rbdy_peak_frequency_hf=self._extract_decimal_field(normalized, "rbdy peak frequency hf"),
            rbdy_peak_frequency_lf=self._extract_decimal_field(normalized, "rbdy peak frequency lf"),
            rbdy_peak_frequency_mf=self._extract_decimal_field(normalized, "rbdy peak frequency mf"),
            rbdy_peak_power_hf=self._extract_decimal_field(normalized, "rbdy peak power hf"),
            rbdy_qc_etdc_3sigma=self._extract_decimal_field(normalized, "rbdy qc etdc 3sigma"),
            rbdy_total_power_lf=self._extract_decimal_field(normalized, "rbdy total power lf"),
            rbdy_total_power_mf=self._extract_decimal_field(normalized, "rbdy total power mf"),
            software_version=self._extract_text(normalized, r"\bsoftware\s+version\s*:\s*(.+)"),
        )

    def _normalize(self, contents: str) -> str:
        return contents.replace("\\n", "\n").strip()

    def _extract_text(self, contents: str, pattern: str) -> str | None:
        match = re.search(pattern, contents, flags=re.IGNORECASE | re.MULTILINE)
        if match is None:
            return None
        value = match.group(1).strip()
        return value.removesuffix(".").strip() or None

    def _extract_int(self, contents: str, pattern: str) -> int | None:
        match = re.search(pattern, contents, flags=re.IGNORECASE | re.MULTILINE)
        if match is None:
            return None
        return int(match.group(1))

    def _extract_decimal(self, contents: str, pattern: str) -> Decimal | None:
        match = re.search(pattern, contents, flags=re.IGNORECASE | re.MULTILINE)
        if match is None:
            return None
        return Decimal(match.group(1))

    def _extract_decimal_field(self, contents: str, label: str) -> Decimal | None:
        return self._extract_decimal(contents, self._field_pattern(label, _DECIMAL_RE))

    def _extract_int_field(self, contents: str, label: str) -> int | None:
        return self._extract_int(contents, self._field_pattern(label, r"([+-]?\d+)"))

    def _field_pattern(self, label: str, value_pattern: str) -> str:
        return r"^" + re.escape(label) + r"\s*:\s*" + value_pattern

    def _extract_time(self, contents: str) -> datetime | None:
        value = self._extract_text(contents, r"\btime\s*:\s*([^\s]+)")
        if value is None:
            return None
        return datetime.fromisoformat(value)

    def _to_code(self, value: str | None) -> str | None:
        if value is None:
            return None
        code = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
        return code or None

    def _dominant_dose_error(self, min_value: Decimal | None, max_value: Decimal | None) -> Decimal | None:
        values = [value for value in (min_value, max_value) if value is not None]
        if not values:
            return None
        return max(values, key=lambda value: abs(value))
