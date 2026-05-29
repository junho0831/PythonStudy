from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Protocol


@dataclass(frozen=True)
class RawErLog:
    er_line: str | None
    eq_name: str | None
    code: str | None
    code_occur_time: datetime
    code_occur_time_raw: str
    log_source: str | None
    contents: str


@dataclass(frozen=True)
class ParsedErDoseError:
    er_line: str | None
    eq_name: str | None
    code: str | None
    code_occur_time: datetime
    code_occur_time_raw: str
    log_source: str | None
    exposure_handle: int | None
    action_handle: int | None
    wafer_seq: int | None
    shot_seq: int | None
    field_seq: int | None
    repair_yn: bool | None
    repair_result: str | None
    dose_error: Decimal | None
    dose_warn_level: Decimal | None
    de_err: Decimal | None
    de_warn_lvl: Decimal | None
    eset: int | None
    freq: int | None
    n_slit: int | None
    mb_enabled: bool | None
    function_name: str | None
    result_type: str | None
    parser_version: str
    parsing_status: str
    parsing_error: str | None
    raw_contents: str


class ERLogParser(Protocol):
    def supports(self, raw: RawErLog) -> bool:
        ...

    def parse(self, raw: RawErLog) -> ParsedErDoseError | None:
        ...
