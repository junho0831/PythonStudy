from __future__ import annotations

import os
from datetime import timedelta
from zoneinfo import ZoneInfo

from er_dose.batch import ERDoseBatch
from er_dose.infra.postgres_db import PostgresDB


LOCAL_TZ = ZoneInfo("Asia/Seoul")
PROCESSING_DELAY = timedelta(minutes=10)
BACKFILL_CHUNK = timedelta(minutes=10)


def run_er_dose_window(**context) -> None:
    start_time = _to_local_naive(context["data_interval_start"]) - PROCESSING_DELAY
    end_time = _to_local_naive(context["data_interval_end"]) - PROCESSING_DELAY
    _run_batch(start_time=start_time, end_time=end_time)


def run_er_dose_backfill_hourly(**context) -> None:
    end_time = _to_local_naive(context["data_interval_end"]) - PROCESSING_DELAY
    start_time = end_time - timedelta(days=1)
    _run_range_by_chunks(start_time=start_time, end_time=end_time)


def run_er_dose_backfill_daily(**context) -> None:
    end_time = _to_local_naive(context["data_interval_end"]) - PROCESSING_DELAY
    start_time = end_time - timedelta(days=3)
    _run_range_by_chunks(start_time=start_time, end_time=end_time)


def _run_batch(start_time, end_time) -> None:
    dsn = os.getenv("ER_DOSE_DB_DSN") or os.getenv("DATABASE_URL")
    if not dsn:
        raise ValueError("ER_DOSE_DB_DSN or DATABASE_URL environment variable is required")

    db = PostgresDB(dsn=dsn)
    batch = ERDoseBatch(db)
    batch.run(start_time=start_time, end_time=end_time)


def _run_range_by_chunks(start_time, end_time) -> None:
    current = start_time
    while current < end_time:
        next_time = min(current + BACKFILL_CHUNK, end_time)
        _run_batch(start_time=current, end_time=next_time)
        current = next_time


def _to_local_naive(value):
    if value.tzinfo is None:
        local_value = value
    else:
        local_value = value.astimezone(LOCAL_TZ)
    return local_value.replace(tzinfo=None)
