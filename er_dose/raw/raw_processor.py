from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

import pandas as pd

from er_dose.raw.raw_base import RawErLog
from er_dose.raw.raw_parser import parse_dose_error
from er_dose.raw.raw_repository import ERDoseRepository


DoseErrorValue = Decimal | int | bool | str | datetime | None
EXPOSURE_HANDLE_JUMP_THRESHOLD = 1000


class ERDoseProcessor:
    def __init__(self, repository: ERDoseRepository):
        self.repository = repository
        # 설비별 가장 최근의 lot_seq, wafer_seq를 기억 (청크가 나뉘어도 유지)
        self.lot_states: dict[str, dict[str, int | None]] = {}
        self.exposure_handles: dict[str, int] = {}

    def run(
        self,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        chunk_size: int = 10000,
        target_date: date | None = None,
        lookback_days: int = 4,
        reference_date: date | None = None,
    ) -> None:
        if target_date is not None:
            start_time = datetime.combine(target_date, datetime.min.time())
            end_time = start_time + timedelta(days=1)

        if start_time is None and end_time is None:
            self.run_recent_days(
                lookback_days=lookback_days,
                reference_date=reference_date,
                chunk_size=chunk_size,
            )
            return
        if start_time is None or end_time is None:
            raise ValueError("start_time and end_time are required")
        if start_time >= end_time:
            raise ValueError("start_time must be earlier than end_time")
        if chunk_size <= 0:
            raise ValueError("chunk_size must be greater than 0")

        self._run_window(start_time=start_time, end_time=end_time, chunk_size=chunk_size)

    def run_recent_days(
        self,
        lookback_days: int = 4,
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
            "[ER_DOSE] "
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

    def _run_window(
        self,
        start_time: datetime,
        end_time: datetime,
        chunk_size: int,
        connection=None,
    ) -> int:
        self.lot_states = self.repository.fetch_latest_lot_states(start_time)
        self.exposure_handles = {}

        fetched_count = 0
        insert_count = 0

        print(
            "[ER_DOSE] "
            f"start_time={start_time.isoformat()} "
            f"end_time={end_time.isoformat()} "
            f"chunk_size={chunk_size} "
            f"preloaded_eq={len(self.lot_states)}"
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
                "[ER_DOSE] "
                f"chunk={chunk_index} "
                f"fetched={chunk_fetched} "
                f"fetched_total={fetched_count}"
            )

            parsed_rows = self._parse_chunk(raw_df)
            parsed_count = len(parsed_rows)
            print(
                "[ER_DOSE] "
                f"chunk={chunk_index} "
                f"parsed={parsed_count}"
            )

            if not parsed_rows:
                print(
                    "[ER_DOSE] "
                    f"chunk={chunk_index} "
                    f"inserted=0 "
                    f"inserted_total={insert_count}"
                )
                continue

            parsed_df = pd.DataFrame(parsed_rows)
            chunk_inserted = self.repository.insert_parsed_df(parsed_df, connection=connection)
            insert_count += chunk_inserted
            print(
                "[ER_DOSE] "
                f"chunk={chunk_index} "
                f"inserted={chunk_inserted} "
                f"inserted_total={insert_count}"
            )

        print(
            "[ER_DOSE] "
            f"done fetched={fetched_count} "
            f"inserted={insert_count}"
        )
        return insert_count

    def _row_to_raw_log(self, row: Any) -> RawErLog:
        code_occur_time = self._normalize_datetime(row.get("code_occur_time"))
        contents = row.get("contents")

        return RawErLog(
            eq_name=self._nullable_str(row.get("eq_name")),
            code=self._nullable_str(row.get("code")),
            code_occur_time=code_occur_time,
            title=self._nullable_str(row.get("title")),
            contents=str(contents) if pd.notna(contents) else "",
        )

    def _nullable_str(self, value: Any) -> str | None:
        if value is None or pd.isna(value):
            return None
        return str(value)

    def _parse_chunk(self, raw_df) -> list[dict[str, DoseErrorValue]]:
        parsed_rows: list[dict[str, DoseErrorValue]] = []

        for row in raw_df.itertuples(index=False):
            raw = self._row_to_raw_log(row._asdict())
            parsed_dict = asdict(parse_dose_error(raw))

            eq_name = parsed_dict.get("eq_name")
            code = parsed_dict.get("code")
            exposure_handle = parsed_dict.get("exposure_handle")
            if eq_name is not None and code is not None and code.upper().startswith("DW-") and exposure_handle is not None:
                previous_exposure_handle = self.exposure_handles.get(eq_name)
                if previous_exposure_handle is not None:
                    exposure_handle_diff = exposure_handle - previous_exposure_handle
                    if exposure_handle_diff >= EXPOSURE_HANDLE_JUMP_THRESHOLD:
                        print(
                            "[ER_DOSE] "
                            f"skip_test_shot eq_name={eq_name} "
                            f"prev_exposure_handle={previous_exposure_handle} "
                            f"exposure_handle={exposure_handle} "
                            f"diff={exposure_handle_diff}"
                        )
                        continue
                self.exposure_handles[eq_name] = exposure_handle

            if eq_name is not None:
                state = self.lot_states.setdefault(eq_name, {"lot_seq": None, "wafer_seq": None})

                if parsed_dict.get("lot_seq") is not None:
                    state["lot_seq"] = parsed_dict["lot_seq"]
                else:
                    parsed_dict["lot_seq"] = state["lot_seq"]

                if parsed_dict.get("wafer_seq") is not None:
                    state["wafer_seq"] = parsed_dict["wafer_seq"]
                else:
                    parsed_dict["wafer_seq"] = state["wafer_seq"]

            parsed_rows.append(parsed_dict)

        return parsed_rows

    def _normalize_datetime(self, value: Any) -> datetime | None:
        if hasattr(value, "to_pydatetime"):
            return value.to_pydatetime()
        return value
