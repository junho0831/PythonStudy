from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from decimal import Decimal
from typing import Any

import pandas as pd

from er_dose.parsers.base import RawErLog
from er_dose.parsers.dose_error_parser import parse_dose_error
from er_dose.repository import ERDoseRepository


DoseErrorValue = Decimal | int | bool | str | datetime | None


class ERDoseProcessor:
    def __init__(self, repository: ERDoseRepository):
        self.repository = repository
        # 설비별 가장 최근의 wafer_id, wafer_seq를 기억 (청크가 나뉘어도 유지)
        self.wafer_states: dict[str, dict[str, int | None]] = {}

    def run(
        self,
        start_time: datetime,
        end_time: datetime,
        chunk_size: int = 10000,
    ) -> None:
        if chunk_size <= 0:
            raise ValueError("chunk_size must be greater than 0")

        self.wafer_states = self.repository.fetch_latest_wafer_states(start_time)

        fetched_count = 0
        insert_count = 0

        print(
            "[ER_DOSE] "
            f"start_time={start_time.isoformat()} "
            f"end_time={end_time.isoformat()} "
            f"chunk_size={chunk_size} "
            f"preloaded_eq={len(self.wafer_states)}"
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
                continue

            parsed_df = pd.DataFrame(parsed_rows)
            chunk_inserted = self.repository.insert_parsed_df(parsed_df)
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

    def _row_to_raw_log(self, row: Any) -> RawErLog:
        code_occur_time = self._normalize_datetime(row.get("code_occur_time"))
        contents = row.get("contents")

        return RawErLog(
            er_date=self._nullable_int(row.get("er_date")),
            er_index=self._nullable_int(row.get("er_index")),
            er_line=self._nullable_str(row.get("er_line")),
            eq_name=self._nullable_str(row.get("eq_name")),
            code=self._nullable_str(row.get("code")),
            code_occur_time=code_occur_time,
            belong=self._nullable_str(row.get("belong")),
            type=self._nullable_str(row.get("type")),
            title=self._nullable_str(row.get("title")),
            contents=str(contents) if pd.notna(contents) else "",
        )

    def _nullable_str(self, value: Any) -> str | None:
        if value is None or pd.isna(value):
            return None
        return str(value)

    def _nullable_int(self, value: Any) -> int | None:
        if value is None or pd.isna(value):
            return None
        return int(value)

    def _parse_chunk(self, raw_df) -> list[dict[str, DoseErrorValue]]:
        parsed_rows: list[dict[str, DoseErrorValue]] = []

        for _, row in raw_df.iterrows():
            raw = self._row_to_raw_log(row)
            parsed_dict = asdict(parse_dose_error(raw))

            eq_name = parsed_dict.get("eq_name")
            if eq_name is not None:
                state = self.wafer_states.setdefault(eq_name, {"wafer_id": None, "wafer_seq": None})

                if parsed_dict.get("wafer_id") is not None:
                    state["wafer_id"] = parsed_dict["wafer_id"]
                else:
                    parsed_dict["wafer_id"] = state["wafer_id"]

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
