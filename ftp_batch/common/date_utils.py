from __future__ import annotations

from datetime import datetime, timedelta


def normalize_anchor_date(input_date: str) -> str:
    try:
        return datetime.strptime(input_date, "%Y-%m-%d").strftime("%Y%m%d")
    except ValueError as exc:
        raise ValueError(f"날짜 형식 오류: {input_date} (YYYY-MM-DD 필요)") from exc


def get_target_dates(input_date: str) -> list[str]:
    anchor = datetime.strptime(normalize_anchor_date(input_date), "%Y%m%d")
    return [
        (anchor - timedelta(days=1)).strftime("%Y%m%d"),
        anchor.strftime("%Y%m%d"),
    ]
