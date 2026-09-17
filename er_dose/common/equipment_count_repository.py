from __future__ import annotations

from datetime import date, datetime
from typing import Any

from er_dose.infra.postgres_db import PostgresDB


def insert_equipment_count(
    db: PostgresDB,
    table_name: str,
    target_date: date,
    rows: list[dict[str, Any]],
) -> None:
    if not rows:
        return

    values = []
    params = {"target_date": target_date}
    for index, row in enumerate(rows):
        values.append(
            f"(:target_date, :eq_name_{index}, :source_count_{index}, :target_count_{index})"
        )
        params[f"eq_name_{index}"] = row["eq_name"]
        params[f"source_count_{index}"] = row["source_count"]
        params[f"target_count_{index}"] = row["target_count"]

    query = f"""
        insert into {table_name} (
            target_date,
            eq_name,
            source_count,
            target_count
        ) values {", ".join(values)}
    """
    db.execute(query, params=params)


def write_equipment_count_log(
    repository,
    start_time: datetime,
    end_time: datetime,
) -> None:
    rows = repository.fetch_equipment_counts(start_time, end_time)
    if rows:
        repository.insert_equipment_count(target_date=start_time.date(), rows=rows)
