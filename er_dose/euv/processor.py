from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime, timedelta
from time import perf_counter
from typing import Any

import pandas as pd

from er_dose.euv.base import RawErEuvLog
from er_dose.euv.repository import ERDoseEUVRepository
from er_dose.euv.parser import parse_root_cause


class ERDoseEUVProcessor:
    def __init__(self, repository: ERDoseEUVRepository):
        self.repository = repository

    def run(
        self,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        chunk_size: int = 10000,
        target_date: date | None = None,
    ) -> None:
        if target_date is not None:
            start_time = datetime.combine(target_date, datetime.min.time())
            end_time = start_time + timedelta(days=1)

        if start_time is None or end_time is None:
            raise ValueError("start_time and end_time are required")
        if start_time >= end_time:
            raise ValueError("start_time must be earlier than end_time")
        if chunk_size <= 0:
            raise ValueError("chunk_size must be greater than 0")

        fetched_count = 0
        insert_count = 0

        print(
            "[ER_DOSE_EUV] "
            f"start_time={start_time.isoformat()} "
            f"end_time={end_time.isoformat()} "
            f"chunk_size={chunk_size}"
        )

        chunk_started_at = perf_counter()
        for chunk_index, raw_df in enumerate(
            self.repository.fetch_raw_logs_in_chunks(
                start_time=start_time,
                end_time=end_time,
                chunk_size=chunk_size,
            ),
            start=1,
        ):
            chunk_fetched = int(len(raw_df))
            fetched_count += chunk_fetched
            fetched_at = perf_counter()
            fetch_sec = fetched_at - chunk_started_at
            print(
                "[ER_DOSE_EUV] "
                f"chunk={chunk_index} "
                f"fetched={chunk_fetched} "
                f"fetched_total={fetched_count} "
                f"fetch_sec={fetch_sec:.3f}"
            )

            parse_started_at = perf_counter()
            parsed_rows = self._parse_chunk(raw_df)
            parsed_at = perf_counter()
            parse_sec = parsed_at - parse_started_at
            parsed_count = len(parsed_rows)
            print(
                "[ER_DOSE_EUV] "
                f"chunk={chunk_index} "
                f"parsed={parsed_count} "
                f"parse_sec={parse_sec:.3f}"
            )

            if not parsed_rows:
                chunk_total_sec = parsed_at - chunk_started_at
                rows_per_sec = 0.0 if chunk_total_sec <= 0 else chunk_fetched / chunk_total_sec
                print(
                    "[ER_DOSE_EUV] "
                    f"chunk={chunk_index} "
                    f"inserted=0 "
                    f"inserted_total={insert_count} "
                    f"insert_sec=0.000 "
                    f"total_sec={chunk_total_sec:.3f} "
                    f"rows_per_sec={rows_per_sec:.1f}"
                )
                chunk_started_at = perf_counter()
                continue

            insert_started_at = perf_counter()
            parsed_df = pd.DataFrame(parsed_rows)
            chunk_inserted = self.repository.insert_root_causes_df(parsed_df)
            inserted_at = perf_counter()
            insert_sec = inserted_at - insert_started_at
            insert_count += chunk_inserted
            chunk_total_sec = inserted_at - chunk_started_at
            rows_per_sec = 0.0 if chunk_total_sec <= 0 else chunk_fetched / chunk_total_sec
            print(
                "[ER_DOSE_EUV] "
                f"chunk={chunk_index} "
                f"inserted={chunk_inserted} "
                f"inserted_total={insert_count} "
                f"insert_sec={insert_sec:.3f} "
                f"total_sec={chunk_total_sec:.3f} "
                f"rows_per_sec={rows_per_sec:.1f}"
            )
            chunk_started_at = perf_counter()

        print(
            "[ER_DOSE_EUV] "
            f"done fetched={fetched_count} "
            f"inserted={insert_count}"
        )

    def _parse_chunk(self, raw_df) -> list[dict[str, Any]]:
        parsed_rows: list[dict[str, Any]] = []

        for _, row in raw_df.iterrows():
            raw = self._row_to_raw_log(row)
            parsed = parse_root_cause(raw.contents)
            if parsed is None:
                continue

            parsed_rows.append(
                {
                    **asdict(raw),
                    **asdict(parsed),
                }
            )

        return parsed_rows

    def _row_to_raw_log(self, row: Any) -> RawErEuvLog:
        contents = row.get("contents")

        return RawErEuvLog(
            er_line=self._nullable_str(row.get("er_line")),
            eq_name=self._nullable_str(row.get("eq_name")),
            er_type=self._nullable_str(row.get("er_type")),
            code=self._nullable_str(row.get("code")),
            code_occur_time=self._normalize_datetime(row.get("code_occur_time")),
            belong=self._nullable_str(row.get("belong")),
            type=self._nullable_str(row.get("type")),
            title=self._nullable_str(row.get("title")),
            contents=str(contents) if pd.notna(contents) else "",
            reason_code=self._nullable_str(row.get("reason_code")),
            task=self._nullable_str(row.get("task")),
            compile_script=self._nullable_str(row.get("compile_script")),
        )

    def _nullable_str(self, value: Any) -> str | None:
        if value is None or pd.isna(value):
            return None
        return str(value)

    def _normalize_datetime(self, value: Any) -> datetime | None:
        if hasattr(value, "to_pydatetime"):
            return value.to_pydatetime()
        return value
