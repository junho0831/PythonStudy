from __future__ import annotations

from datetime import date, datetime
from typing import Any

import pandas as pd

from er_dose.infra.postgres_db import PostgresDB


def insert_equipment_count(
    db: PostgresDB,
    table_name: str,
    target_date: date,
    rows: list[dict[str, Any]],
) -> None:
    if not rows:
        return

    df = pd.DataFrame(rows, columns=["eq_name", "source_count", "target_count"])
    df.insert(0, "target_date", target_date)
    db.bulk_insert_df(table_name, df)


def write_equipment_count_log(
    repository,
    start_time: datetime,
    end_time: datetime,
) -> None:
    rows = repository.fetch_equipment_counts(start_time, end_time)
    if rows:
        repository.insert_equipment_count(target_date=start_time.date(), rows=rows)
