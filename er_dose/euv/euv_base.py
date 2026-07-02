from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True)
class RawErEuvLog:
    er_line: str | None
    eq_name: str | None
    er_type: str | None
    code: str | None
    code_occur_time: datetime | None
    belong: str | None
    type: str | None
    title: str | None
    contents: str
    reason_code: str | None
    task: str | None
    compile_script: str | None


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
    pulses_euv_0_6dt_tot: int | None
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
