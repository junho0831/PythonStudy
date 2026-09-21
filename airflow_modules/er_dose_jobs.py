from __future__ import annotations

from datetime import date, datetime, timedelta

from airflow_modules.er_dose_db import PostgresDB
from airflow_modules.er_dose_repository import (
    ERDoseRepository, ERDoseEUVRepository, write_equipment_count_log,
)


PLAN_XCOM_KEY = "ER_DOSE_PLAN"


def needs_reload(repository, target_date: date) -> bool:
    source_count = repository.fetch_source_count(target_date)
    target_count = repository.fetch_target_count(target_date)
    if source_count == target_count:
        return False
    if target_count > 0:
        return repository.fetch_source_count(target_date, distinct=True) != target_count
    return True


def plan_dates(ti, date_task_id: str) -> None:
    """Use the existing date task's XCom without calculating another lookback."""
    value = ti.xcom_pull(task_ids=date_task_id, key="date")
    if not isinstance(value, str):
        raise ValueError("date XCom must be a YYYY-MM-DD string")
    target_date = datetime.strptime(value, "%Y-%m-%d").date()
    if target_date.isoformat() != value:
        raise ValueError("date XCom must be a YYYY-MM-DD string")
    start_time = datetime.combine(target_date, datetime.min.time())
    db = PostgresDB()
    plan = [{
        "target_date": value,
        "start_time": start_time.isoformat(),
        "end_time": (start_time + timedelta(days=1)).isoformat(),
        "raw": needs_reload(ERDoseRepository(db), target_date),
        "euv": needs_reload(ERDoseEUVRepository(db), target_date),
    }]
    ti.xcom_push(key=PLAN_XCOM_KEY, value=plan)


def report_day(job_type: str, day_index: int, ti, plan_task_id: str) -> None:
    if job_type not in ("die_yield", "root_cause", "raw_count", "euv_count"):
        raise ValueError("unknown ER Dose report job")
    plan = ti.xcom_pull(task_ids=plan_task_id, key=PLAN_XCOM_KEY)
    if plan is None:
        raise ValueError("saved ER Dose plan is missing; do not recompute dates during retry")
    window = plan[day_index]
    start_time = datetime.fromisoformat(window["start_time"])
    end_time = datetime.fromisoformat(window["end_time"])
    db = PostgresDB()
    repository = ERDoseRepository(db)
    if job_type == "die_yield":
        repository.delete_die_yield_daily_summary(start_time, end_time)
        repository.insert_die_yield_daily_summary(start_time, end_time)
    elif job_type == "root_cause":
        repository.delete_root_cause_daily_summary(start_time, end_time)
        repository.insert_root_cause_daily_summary(start_time, end_time)
    elif job_type == "raw_count":
        write_equipment_count_log(repository, start_time, end_time)
    else:
        write_equipment_count_log(ERDoseEUVRepository(db), start_time, end_time)
