from __future__ import annotations

from er_dose.parsers.base import ParsedErDoseError, RawErLog
from er_dose.parsers.dose_error_parser import DoseErrorParser


PARSERS = [
    DoseErrorParser(),
]


def parse_raw_er_log(raw: RawErLog) -> ParsedErDoseError | None:
    for parser in PARSERS:
        if parser.supports(raw):
            return parser.parse(raw)
    return None

