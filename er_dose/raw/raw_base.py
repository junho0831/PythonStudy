from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


DW_TARGET_CODES = (
    "DW-3411",
    "DW-3425",
    "DW-343A",
    "DW-343B",
)

LO_TARGET_CODES = (
    "LO-0050",
    "LO-0061",
    "LO-8166",
    "LO-8167",
)

KE_TARGET_CODES = (
    "KE-9103",
    "KE-9104",
)

TARGET_CODES = DW_TARGET_CODES + LO_TARGET_CODES + KE_TARGET_CODES

_norm = lambda codes: {c.replace("-", "").upper() for c in codes}
DW_TARGET_CODES_NORM = _norm(DW_TARGET_CODES)
LO_TARGET_CODES_NORM = _norm(LO_TARGET_CODES)
KE_TARGET_CODES_NORM = _norm(KE_TARGET_CODES)


@dataclass(frozen=True)
class RawErLog:
    eq_name: str | None
    code: str | None
    code_occur_time: datetime
    title: str | None
    contents: str


@dataclass(frozen=True)
class ParsedErDoseError:
    eq_name: str | None
    code: str | None
    code_occur_time: datetime
    title: str | None
    contents: str
    exposure_handle: int | None
    action_handle: int | None
    lot_id: str | None
    lot_name: str | None
    lot_seq: int | None
    wafer_seq: int | None
    de_err: Decimal | None
    n_slit: int | None
    use_yn: str = "Y"
