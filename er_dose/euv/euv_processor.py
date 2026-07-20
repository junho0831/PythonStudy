from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd

from er_dose.euv.euv_base import RawErEuvLog
from er_dose.euv.euv_repository import ERDoseEUVRepository
from er_dose.euv.euv_parser import parse_root_cause


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
            print(
                "[ER_DOSE_EUV] "
                f"chunk={chunk_index} "
                f"fetched={chunk_fetched} "
                f"fetched_total={fetched_count}"
            )

            parsed_rows = self._parse_chunk(raw_df)
            parsed_count = len(parsed_rows)
            print(
                "[ER_DOSE_EUV] "
                f"chunk={chunk_index} "
                f"parsed={parsed_count}"
            )

            if not parsed_rows:
                print(
                    "[ER_DOSE_EUV] "
                    f"chunk={chunk_index} "
                    f"inserted=0 "
                    f"inserted_total={insert_count}"
                )
                continue

            parsed_df = pd.DataFrame(parsed_rows)
            chunk_inserted = self.repository.insert_root_causes_df(parsed_df)
            insert_count += chunk_inserted
            print(
                "[ER_DOSE_EUV] "
                f"chunk={chunk_index} "
                f"inserted={chunk_inserted} "
                f"inserted_total={insert_count}"
            )

        print(
            "[ER_DOSE_EUV] "
            f"done fetched={fetched_count} "
            f"inserted={insert_count}"
        )

    def _parse_chunk(self, raw_df) -> list[dict[str, Any]]:
        parsed_rows: list[dict[str, Any]] = []

        for row in raw_df.itertuples(index=False):
            raw = self._row_to_raw_log(row._asdict())
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
            eq_name=self._nullable_str(row.get("eq_name")),
            er_type=self._nullable_str(row.get("er_type")),
            code=self._nullable_str(row.get("code")),
            code_occur_time=self._normalize_datetime(row.get("code_occur_time")),
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
