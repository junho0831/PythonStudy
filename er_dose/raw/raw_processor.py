from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

import pandas as pd

from er_dose.common.reload_processor import CountReloadProcessor
from er_dose.raw.raw_base import RawErLog
from er_dose.raw.raw_parser import parse_dose_error
from er_dose.raw.raw_repository import ERDoseRepository
from er_dose.euv.euv_base import RawErEuvLog
from er_dose.euv.euv_parser import parse_root_cause
from er_dose.euv.euv_repository import ERDoseEUVRepository


DoseErrorValue = Decimal | int | bool | str | datetime | None
EXPOSURE_HANDLE_JUMP_THRESHOLD = 1000


class ERDoseProcessor(CountReloadProcessor):
    log_prefix = "[ER_DOSE]"

    def __init__(self, repository: ERDoseRepository):
        self.repository = repository
        # 설비별 가장 최근 lot 값을 기억한다. 청크가 나뉘어도 유지된다.
        self.lot_states: dict[str, dict[str, int | str | None]] = {}
        self.exposure_handles: dict[str, int] = {}

    def run(
        self,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        chunk_size: int = 10000,
        target_date: date | None = None,
        lookback_days: int = 2,
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
        inserted_target_dates: set[str] = set()
        pending_insert = None

        print(
            "[ER_DOSE] "
            f"start_time={start_time.isoformat()} "
            f"end_time={end_time.isoformat()} "
            f"chunk_size={chunk_size} "
            f"preloaded_eq={len(self.lot_states)}"
        )

        with ThreadPoolExecutor(max_workers=1, thread_name_prefix="er-dose-insert") as insert_executor:
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

                if pending_insert is not None:
                    inserted_chunk_index, insert_future = pending_insert
                    chunk_inserted = insert_future.result()
                    insert_count += chunk_inserted
                    print(
                        "[ER_DOSE] "
                        f"chunk={inserted_chunk_index} "
                        f"inserted={chunk_inserted} "
                        f"inserted_total={insert_count}"
                    )
                    pending_insert = None

                if not parsed_rows:
                    print(
                        "[ER_DOSE] "
                        f"chunk={chunk_index} "
                        f"inserted=0 "
                        f"inserted_total={insert_count}"
                    )
                    continue

                parsed_df = pd.DataFrame(parsed_rows)
                chunk_occur_time = self._normalize_datetime(parsed_df.iloc[0]["code_occur_time"])
                if chunk_occur_time is None:
                    raise ValueError("code_occur_time is required")
                chunk_target_date = chunk_occur_time.date().isoformat()
                inserted_target_dates.add(chunk_target_date)
                pending_insert = (
                    chunk_index,
                    insert_executor.submit(self.repository.insert_parsed_df, parsed_df),
                )

            if pending_insert is not None:
                inserted_chunk_index, insert_future = pending_insert
                chunk_inserted = insert_future.result()
                insert_count += chunk_inserted
                print(
                    "[ER_DOSE] "
                    f"chunk={inserted_chunk_index} "
                    f"inserted={chunk_inserted} "
                    f"inserted_total={insert_count}"
                )

        for target_date in sorted(inserted_target_dates):
            self.repository.analyze_target_partition(target_date, connection=connection)
            self.repository.upsert_die_yield_daily_summary(target_date, connection=connection)
            self.repository.upsert_root_cause_daily_summary(target_date, connection=connection)
            print(
                "[ER_DOSE] "
                f"summary updated (die_yield, root_cause) partition_date={target_date}"
            )

        print(
            "[ER_DOSE] "
            f"done fetched={fetched_count} "
            f"inserted={insert_count}"
        )
        return insert_count

    def _reload_target_date(self, target_date: date, chunk_size: int) -> int:
        start_time = datetime.combine(target_date, datetime.min.time())
        end_time = start_time + timedelta(days=1)
        self.repository.truncate_target_partition(target_date)
        return self._run_window(
            start_time=start_time,
            end_time=end_time,
            chunk_size=chunk_size,
        )

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
            parsed_dict = vars(parse_dose_error(raw)).copy()

            eq_name = parsed_dict.get("eq_name")
            code = parsed_dict.get("code")
            code_norm = code.upper() if code is not None else ""
            exposure_handle = parsed_dict.get("exposure_handle")
            if eq_name is not None and code_norm.startswith("DW-") and exposure_handle is not None:
                previous_exposure_handle = self.exposure_handles.get(eq_name)
                if previous_exposure_handle is not None:
                    exposure_handle_diff = exposure_handle - previous_exposure_handle
                    if exposure_handle_diff >= EXPOSURE_HANDLE_JUMP_THRESHOLD:
                        parsed_dict["use_yn"] = "N"
                        print(
                            "[ER_DOSE] "
                            f"mark_unused_test_shot eq_name={eq_name} "
                            f"prev_exposure_handle={previous_exposure_handle} "
                            f"exposure_handle={exposure_handle} "
                            f"diff={exposure_handle_diff}"
                        )
                    else:
                        self.exposure_handles[eq_name] = exposure_handle
                else:
                    self.exposure_handles[eq_name] = exposure_handle


            if eq_name is not None:
                state = self.lot_states.setdefault(
                    eq_name,
                    {
                        "lot_id": None,
                        "lot_name": None,
                        "lot_seq": None,
                        "wafer_seq": None,
                    },
                )

                if parsed_dict.get("lot_id") is not None:
                    state["lot_id"] = parsed_dict["lot_id"]
                else:
                    parsed_dict["lot_id"] = state["lot_id"]

                if parsed_dict.get("lot_name") is not None:
                    state["lot_name"] = parsed_dict["lot_name"]
                else:
                    parsed_dict["lot_name"] = state["lot_name"]

                if parsed_dict.get("lot_seq") is not None:
                    if code_norm != "LO-0050":
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



class ERDoseEUVProcessor(CountReloadProcessor):
    log_prefix = "[ER_DOSE_EUV]"

    def __init__(self, repository: ERDoseEUVRepository):
        self.repository = repository

    def run(
        self,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        chunk_size: int = 10000,
        lookback_days: int = 2,
        reference_date: date | None = None,
        target_date: date | None = None,
    ) -> None:
        if target_date is not None:
            self.run_recent_days(
                lookback_days=1,
                reference_date=target_date,
                chunk_size=chunk_size,
            )
            return

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

    def _run_window(
        self,
        start_time: datetime,
        end_time: datetime,
        chunk_size: int,
        connection=None,
    ) -> int:
        fetched_count = 0
        insert_count = 0
        inserted_target_dates: set[str] = set()

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
            inserted_target_dates.update(
                pd.to_datetime(parsed_df["code_occur_time"]).dt.strftime("%Y-%m-%d").dropna().unique()
            )
            chunk_inserted = self.repository.insert_root_causes_df(parsed_df, connection=connection, analyze=False)
            insert_count += chunk_inserted
            print(
                "[ER_DOSE_EUV] "
                f"chunk={chunk_index} "
                f"inserted={chunk_inserted} "
                f"inserted_total={insert_count}"
            )

        for target_date in sorted(inserted_target_dates):
            self.repository.analyze_target_partition(target_date, connection=connection)
            self.repository.upsert_root_cause_daily_summary(target_date, connection=connection)
            print(
                "[ER_DOSE_EUV] "
                f"summary updated (root_cause) partition_date={target_date}"
            )

        print(
            "[ER_DOSE_EUV] "
            f"done fetched={fetched_count} "
            f"inserted={insert_count}"
        )
        return insert_count

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
