from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Protocol


class CountReloadRepository(Protocol):
    def fetch_source_count(self, target_date: date, distinct: bool = False) -> int:
        ...

    def fetch_target_count(self, target_date: date) -> int:
        ...

    def fetch_equipment_counts(self, target_date: date) -> list[dict[str, str | int]]:
        ...

    def insert_batch_log(
        self,
        batch_name: str,
        target_date: date,
        event_type: str,
        message: str,
        data: dict[str, Any],
    ) -> int:
        ...

    def truncate_target_partition(self, target_date: date, connection=None) -> int:
        ...

    def transaction(self):
        ...


class CountReloadProcessor:
    log_prefix = "[ER_DOSE]"
    batch_name = "ER_DOSE"
    repository: CountReloadRepository

    def _run_window(
        self,
        start_time: datetime,
        end_time: datetime,
        chunk_size: int,
        connection=None,
    ) -> int:
        raise NotImplementedError

    def run_recent_days(
        self,
        lookback_days: int = 2,
        reference_date: date | None = None,
        chunk_size: int = 10000,
    ) -> None:
        if lookback_days <= 0:
            raise ValueError("lookback_days must be greater than 0")
        if chunk_size <= 0:
            raise ValueError("chunk_size must be greater than 0")

        end_date = reference_date or date.today()
        start_date = end_date - timedelta(days=lookback_days - 1)

        checked_dates = 0
        reloaded_dates = 0
        source_rows = 0
        inserted_rows = 0
        current_date = start_date
        while current_date <= end_date:
            checked_dates += 1
            equipment_counts = self.repository.fetch_equipment_counts(current_date)
            source_count = sum(int(item["source_count"]) for item in equipment_counts)
            target_count = sum(int(item["target_count"]) for item in equipment_counts)

            reload_required = source_count != target_count
            if reload_required and target_count > 0:
                distinct_source_count = self.repository.fetch_source_count(current_date, distinct=True)
                if distinct_source_count == target_count:
                    reload_required = False

            if reload_required:
                reloaded_dates += 1
                source_rows += source_count
                inserted_rows += self._reload_target_date(target_date=current_date, chunk_size=chunk_size)
                equipment_counts = self.repository.fetch_equipment_counts(current_date)

            self._write_equipment_count_log(
                target_date=current_date,
                action="RELOADED" if reload_required else "SKIPPED",
                equipment_counts=equipment_counts,
            )
            current_date += timedelta(days=1)

        print(
            f"{self.log_prefix} "
            f"lookback_done start_date={start_date.isoformat()} "
            f"end_date={end_date.isoformat()} "
            f"checked_dates={checked_dates} "
            f"reloaded_dates={reloaded_dates} "
            f"source_rows={source_rows} "
            f"inserted={inserted_rows}"
        )

    @staticmethod
    def _window_target_dates(start_time: datetime, end_time: datetime) -> set[str]:
        current_date = start_time.date()
        target_dates = set()
        while current_date <= (end_time - timedelta(microseconds=1)).date():
            target_dates.add(current_date.isoformat())
            current_date += timedelta(days=1)
        return target_dates

    def _write_equipment_count_logs(
        self,
        start_time: datetime,
        end_time: datetime,
        action: str,
    ) -> None:
        current_date = start_time.date()
        last_date = (end_time - timedelta(microseconds=1)).date()
        while current_date <= last_date:
            self._write_equipment_count_log(target_date=current_date, action=action)
            current_date += timedelta(days=1)

    def _write_equipment_count_log(
        self,
        target_date: date,
        action: str,
        equipment_counts: list[dict[str, str | int]] | None = None,
    ) -> None:
        if equipment_counts is None:
            equipment_counts = self.repository.fetch_equipment_counts(target_date)
        source_count = sum(int(item["source_count"]) for item in equipment_counts)
        target_count = sum(int(item["target_count"]) for item in equipment_counts)
        matched = all(
            int(item["source_count"]) == int(item["target_count"])
            for item in equipment_counts
        )
        self.repository.insert_batch_log(
            batch_name=self.batch_name,
            target_date=target_date,
            event_type="EQUIPMENT_COUNT",
            message=(
                f"equipment count source={source_count} "
                f"target={target_count} matched={str(matched).lower()}"
            ),
            data={
                "action": action,
                "source_count": source_count,
                "target_count": target_count,
                "matched": matched,
                "equipment_counts": equipment_counts,
            },
        )
        print(
            f"{self.log_prefix} "
            f"equipment_count_log target_date={target_date.isoformat()} "
            f"equipment_count={len(equipment_counts)} "
            f"source_count={source_count} "
            f"target_count={target_count} "
            f"matched={str(matched).lower()}"
        )

    def _reload_target_date(self, target_date: date, chunk_size: int) -> int:
        start_time = datetime.combine(target_date, datetime.min.time())
        end_time = start_time + timedelta(days=1)
        with self.repository.transaction() as connection:
            self.repository.truncate_target_partition(target_date, connection=connection)
            return self._run_window(
                start_time=start_time,
                end_time=end_time,
                chunk_size=chunk_size,
                connection=connection,
            )
