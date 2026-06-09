from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timedelta
from decimal import Decimal

import pandas as pd

from er_dose.infra.postgres_db import PostgresDB
from er_dose.parsers.base import RawErLog
from er_dose.parsers.dose_error_parser import parse_dose_error


MAIN_RAW_TABLE = "mbeat.er_data_raw"
PARSED_TABLE = "mbeat.er_dose_error_parsed"
TARGET_CODES = (
    "DW3411",
    "DW3425",
    "DW343A",
    "DW343B",
    "LO0061",
    "LO8166",
    "LO8167",
    "KE9103",
    "KE9104",
)
DoseErrorValue = Decimal | int | bool | str | datetime | None


class ERDoseBatch:
    def __init__(self, db: PostgresDB):
        self.db = db
        # 설비별 가장 최근의 wafer_id, wafer_seq를 기억 (청크가 나뉘어도 유지)
        self.wafer_states: dict[str, dict[str, int | None]] = {}

    def run(
        self,
        start_time: datetime,
        end_time: datetime,
        limit: int | None = None,
        chunk_size: int = 10000,
    ) -> None:
        if chunk_size <= 0:
            raise ValueError("chunk_size must be greater than 0")

        self.ensure_partitions(start_time=start_time, end_time=end_time)
        fetched_count = 0
        insert_count = 0

        for raw_df in self.fetch_raw_logs_in_chunks(
            start_time=start_time,
            end_time=end_time,
            limit=limit,
            chunk_size=chunk_size,
        ):
            fetched_count += int(len(raw_df))
            parsed_rows = self._parse_chunk(raw_df)

            if parsed_rows:
                parsed_df = pd.DataFrame(parsed_rows)
                with self.db.transaction() as connection:
                    insert_count += self.db.copy_insert_df(PARSED_TABLE, parsed_df, connection=connection)

        print(
            "[ER_DOSE] "
            f"fetched={fetched_count} "
            f"inserted={insert_count}"
        )

    def fetch_raw_logs(self, start_time: datetime, end_time: datetime, limit: int | None = None):
        query, params = self._build_fetch_raw_logs_query(start_time=start_time, end_time=end_time, limit=limit)
        return self.db.fetch_df(query, params=params)

    def fetch_raw_logs_in_chunks(
        self,
        start_time: datetime,
        end_time: datetime,
        limit: int | None = None,
        chunk_size: int = 10000,
    ):
        query, params = self._build_fetch_raw_logs_query(start_time=start_time, end_time=end_time, limit=limit)
        return self.db.fetch_df_in_chunks(query, params=params, chunk_size=chunk_size)

    def _build_fetch_raw_logs_query(self, start_time: datetime, end_time: datetime, limit: int | None = None):
        params = {
            "start_time": start_time,
            "end_time": end_time,
        }
        limit_sql = ""
        if limit is not None:
            if limit <= 0:
                raise ValueError("limit must be greater than 0")
            params["limit"] = limit
            limit_sql = "limit %(limit)s"

        target_codes_sql = ", ".join(f"'{code}'" for code in TARGET_CODES)

        query = f"""
            select
                r.er_date,
                r.er_index,
                r.er_line,
                r.eq_name,
                r.code,
                r.code_occur_time,
                r.belong,
                r."type" as type,
                r.title,
                r.contents
            from {MAIN_RAW_TABLE} r
            where r.code_occur_time >= %(start_time)s
              and r.code_occur_time < %(end_time)s
              and upper(replace(coalesce(r.code, ''), '-', '')) in ({target_codes_sql})
            order by r.code_occur_time
            {limit_sql}
        """
        return query, params

    def ensure_partitions(self, start_time: datetime, end_time: datetime) -> None:
        for day_start in self._iter_day_starts(start_time, end_time):
            next_day = day_start + timedelta(days=1)
            partition_name = f"er_dose_error_parsed_{day_start:%Y%m%d}"
            query = f"""
                create table if not exists mbeat.{partition_name}
                partition of {PARSED_TABLE}
                for values from (%(start_time)s) to (%(end_time)s)
            """
            self.db.execute(
                query,
                params={
                    "start_time": day_start,
                    "end_time": next_day,
                },
            )

    def _row_to_raw_log(self, row) -> RawErLog:
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

    def _nullable_str(self, value):
        if value is None or pd.isna(value):
            return None
        return str(value)

    def _nullable_int(self, value):
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

    def _normalize_datetime(self, value):
        if hasattr(value, "to_pydatetime"):
            return value.to_pydatetime()
        return value

    def _iter_day_starts(self, start_time: datetime, end_time: datetime):
        current = start_time.replace(hour=0, minute=0, second=0, microsecond=0)
        while current < end_time:
            yield current
            current += timedelta(days=1)
