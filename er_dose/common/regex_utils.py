from __future__ import annotations

import re
from datetime import datetime
from decimal import Decimal
from typing import Callable, TypeVar


T = TypeVar("T")
REGEX_FLAGS = re.IGNORECASE | re.MULTILINE
DECIMAL_RE = r"([+-]?\d+(?:\.\d+)?)"
INT_RE = r"([+-]?\d+)"


def normalize_multiline_text(contents: str) -> str:
    return contents.replace("\\n", "\n").strip()


def extract_value(contents: str, pattern: str, type_cast: Callable[[str], T]) -> T | None:
    match = re.search(pattern, contents, flags=REGEX_FLAGS)
    if match is None:
        return None
    return type_cast(match.group(1))


def extract_text(contents: str, pattern: str, *, trim_trailing_period: bool = False) -> str | None:
    value = extract_value(contents, pattern, str)
    if value is None:
        return None
    value = value.strip()
    if trim_trailing_period:
        value = value.removesuffix(".").strip()
    return value or None


def extract_int(contents: str, pattern: str) -> int | None:
    return extract_value(contents, pattern, int)


def extract_decimal(contents: str, pattern: str) -> Decimal | None:
    return extract_value(contents, pattern, Decimal)


def extract_datetime_isoformat(contents: str, pattern: str) -> datetime | None:
    value = extract_text(contents, pattern)
    if value is None:
        return None
    return datetime.fromisoformat(value)


def extract_first_int(contents: str, patterns: list[str], minimum: int | None = None) -> int | None:
    for pattern in patterns:
        value = extract_int(contents, pattern)
        if value is None:
            continue
        if minimum is not None and value < minimum:
            continue
        return value
    return None


def extract_first_decimal(contents: str, patterns: list[str]) -> Decimal | None:
    for pattern in patterns:
        value = extract_decimal(contents, pattern)
        if value is not None:
            return value
    return None


def field_pattern(label: str, value_pattern: str) -> str:
    return r"^" + re.escape(label) + r"\s*:\s*" + value_pattern


def to_snake_code(value: str | None) -> str | None:
    if value is None:
        return None
    code = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return code or None
