from __future__ import annotations

import os
from datetime import date, datetime

from airflow_modules.ftp_batch_jobs import run_batch
from er_dose.infra.postgres_db import PostgresDB
from er_dose.processor import ERDoseProcessor
from er_dose.repository import ERDoseRepository


class Main:
    def __init__(self, env=None):
        self.env = os.environ if env is None else env

    def run(self) -> int:
        target = self._get_required("BATCH_TARGET").upper().strip()
        if target == "RBI":
            return self.run_rbi()
        if target in {"ER_DOSE_RAW", "ER_DOSE"}:
            return self.run_er_dose_raw()
        if target == "ER_DOSE_EUV":
            return self.run_er_dose_euv()
        raise ValueError("BATCH_TARGET must be RBI, ER_DOSE_RAW, or ER_DOSE_EUV")

    def run_rbi(self) -> int:
        input_date = self._get_required("RBI_INPUT_DATE", fallback_key="INPUT_DATE")
        parser_name = self.env.get("RBI_PARSER", "COMBINED").upper().strip()
        run_batch(input_date=input_date, parser_name=parser_name)
        return 0

    def run_er_dose_raw(self) -> int:
        target_date_value = (
            self.env.get("ER_DOSE_RAW_TARGET_DATE")
            or self.env.get("ER_DOSE_TARGET_DATE")
            or self.env.get("TARGET_DATE")
        )
        target_date = self._parse_date(target_date_value) if target_date_value else None

        if target_date is None:
            start_time = self._parse_datetime(self._get_required("ER_DOSE_START_TIME", fallback_key="START_TIME"))
            end_time = self._parse_datetime(self._get_required("ER_DOSE_END_TIME", fallback_key="END_TIME"))
            if start_time >= end_time:
                raise ValueError("ER_DOSE_START_TIME must be earlier than ER_DOSE_END_TIME")
        else:
            start_time = None
            end_time = None

        chunk_size = self._parse_optional_int(self.env.get("ER_DOSE_CHUNK_SIZE"), field_name="ER_DOSE_CHUNK_SIZE") or 10000
        dsn = self._get_required("ER_DOSE_DB_DSN", fallback_key="DATABASE_URL")

        db = PostgresDB(dsn=dsn)
        repository = ERDoseRepository(db)
        processor = ERDoseProcessor(repository)
        processor.run(start_time=start_time, end_time=end_time, chunk_size=chunk_size, target_date=target_date)
        return 0

    def run_er_dose_euv(self) -> int:
        raise NotImplementedError("ER_DOSE_EUV is not implemented yet")

    def _get_required(self, key: str, fallback_key: str | None = None) -> str:
        value = self.env.get(key)
        if value:
            return value
        if fallback_key:
            fallback_value = self.env.get(fallback_key)
            if fallback_value:
                return fallback_value
        if fallback_key:
            raise ValueError(f"{key} or {fallback_key} environment variable is required")
        raise ValueError(f"{key} environment variable is required")

    def _parse_datetime(self, value: str) -> datetime:
        try:
            return datetime.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(f"invalid datetime: {value}") from exc

    def _parse_date(self, value: str) -> date:
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(f"invalid date: {value}") from exc

    def _parse_optional_int(self, value: str | None, field_name: str) -> int | None:
        if value is None or value.strip() == "":
            return None
        parsed = int(value)
        if parsed <= 0:
            raise ValueError(f"{field_name} must be greater than 0")
        return parsed


def main() -> int:
    return Main().run()
