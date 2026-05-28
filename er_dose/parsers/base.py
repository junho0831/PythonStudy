from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Protocol


@dataclass(frozen=True)
class RawErLog:
    raw_id: int
    er_line: str | None
    eq_name: str | None
    code: str | None
    code_occur_time: datetime
    contents: str


@dataclass(frozen=True)
class ParsedErDoseError:
    raw_id: int
    er_line: str | None
    eq_name: str | None
    code: str | None
    code_occur_time: datetime
    exposure_handle: int | None
    action_handle: int | None
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
    raw_contents: str


class ERLogParser(Protocol):
    def supports(self, raw: RawErLog) -> bool:
        ...

    def parse(self, raw: RawErLog) -> ParsedErDoseError | None:
        ...

