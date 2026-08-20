from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Protocol


class CountReloadRepository(Protocol):
    def fetch_source_count(self, target_date: date) -> int:
        ...

    def fetch_target_count(self, target_date: date) -> int:
        ...

    def truncate_target_partition(self, target_date: date, connection=None) -> int:
        ...

    def transaction(self):
        ...


class CountReloadProcessor:
    log_prefix = "[ER_DOSE]"
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
            source_count = self.repository.fetch_source_count(current_date)
            target_count = self.repository.fetch_target_count(current_date)

            if source_count == target_count:
                current_date += timedelta(days=1)
                continue

            reloaded_dates += 1
            source_rows += source_count
            inserted_rows += self._reload_target_date(target_date=current_date, chunk_size=chunk_size)
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
