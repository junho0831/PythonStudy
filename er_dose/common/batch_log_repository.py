from __future__ import annotations

import json
from datetime import date
from typing import Any

from er_dose.infra.postgres_db import PostgresDB


BATCH_LOG_TABLE = "mbeat.batch_event_log"


def insert_batch_log(
    db: PostgresDB,
    batch_name: str,
    target_date: date,
    event_type: str,
    message: str,
    data: dict[str, Any],
) -> None:
    query = f"""
        insert into {BATCH_LOG_TABLE} (
            batch_name,
            target_date,
            event_type,
            message,
            data
        ) values (
            :batch_name,
            :target_date,
            :event_type,
            :message,
            cast(:data as jsonb)
        )
    """
    db.execute(
        query,
        params={
            "batch_name": batch_name,
            "target_date": target_date,
            "event_type": event_type,
            "message": message,
            "data": json.dumps(data, ensure_ascii=False, default=str),
        },
    )
