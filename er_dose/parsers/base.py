from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Protocol


@dataclass(frozen=True)
class RawErLog:
    er_date: int | None
    er_index: int | None
    er_line: str | None
    eq_name: str | None
    code: str | None
    code_occur_time: datetime
    belong: str | None
    type: str | None
    title: str | None
    contents: str


@dataclass(frozen=True)
class ParsedErDoseError:
    er_date: int | None
    er_index: int | None
    er_line: str | None
    eq_name: str | None
    code: str | None
    code_occur_time: datetime
    belong: str | None
    type: str | None
    title: str | None
    contents: str
    exposure_handle: int | None
    source_exposure_id: int | None
    action_handle: int | None
    wafer_seq: int | None
    shot_seq: int | None
    field_seq: int | None
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


class ERLogParser(Protocol):
    def supports(self, raw: RawErLog) -> bool:
        ...

    def parse(self, raw: RawErLog) -> ParsedErDoseError | None:
        ...
