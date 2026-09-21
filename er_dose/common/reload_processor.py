from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Protocol


class CountReloadRepository(Protocol):
    def fetch_source_count(self, target_date: date, distinct: bool = False) -> int:
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
