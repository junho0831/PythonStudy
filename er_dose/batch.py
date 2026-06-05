from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timedelta
from decimal import Decimal

import pandas as pd

from er_dose.infra.postgres_db import PostgresDB
from er_dose.parsers.base import RawErLog
from er_dose.parsers.registry import parse_raw_er_log


MAIN_RAW_TABLE = "mbeat.er_data_raw"
PARSED_TABLE = "mbeat.er_dose_error_parsed"
DoseErrorValue = Decimal | int | bool | str | datetime | None


class ERDoseBatch:
    def __init__(self, db: PostgresDB):
        self.db = db

    def run(
        self,
        start_time: datetime,
        end_time: datetime,
        limit: int | None = None,
        chunk_size: int = 10000,
    ) -> dict[str, int]:
        if chunk_size <= 0:
            raise ValueError("chunk_size must be greater than 0")

        self.ensure_partitions(start_time=start_time, end_time=end_time)
        fetched_count = 0
        success_count = 0
        regex_fail_count = 0
        parser_error_count = 0
        insert_count = 0

        for raw_df in self.fetch_raw_logs_in_chunks(
            start_time=start_time,
            end_time=end_time,
            limit=limit,
            chunk_size=chunk_size,
        ):
            fetched_count += int(len(raw_df))
            parsed_rows = []

            for _, row in raw_df.iterrows():
                try:
                    raw = self._row_to_raw_log(row)
                    parsed = parse_raw_er_log(raw)
                    if parsed is None:
                        regex_fail_count += 1
                        parsed_rows.append(self._build_unparsed_row(raw))
                        continue
                    parsed_rows.append(asdict(parsed))
                    success_count += 1
                except Exception as exc:
                    parser_error_count += 1
                    parsed_rows.append(self._build_error_row(row, exc))

            if parsed_rows:
                parsed_df = pd.DataFrame(parsed_rows)
                with self.db.transaction() as connection:
                    insert_count += self.db.copy_insert_df(PARSED_TABLE, parsed_df, connection=connection)

        summary = {
            "fetched": int(fetched_count),
            "success": int(success_count),
            "regex_fail": int(regex_fail_count),
            "parser_error": int(parser_error_count),
            "inserted": int(insert_count),
        }

        print(
            "[ER_DOSE] "
            f"fetched={summary['fetched']} "
            f"success={summary['success']} "
            f"regex_fail={summary['regex_fail']} "
            f"parser_error={summary['parser_error']} "
            f"inserted={summary['inserted']}"
        )

        return summary

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
              and (
                  r.code ilike 'dw-%%'
                  or r.contents ilike '%%dose evaluation%%'
                  or r.contents ilike '%%de_err%%'
                  or r.contents ilike '%%dwdc_eval_determine_dose_performance_result%%'
              )
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
        code_occur_time = row.get("code_occur_time")
        contents = row.get("contents")

        if pd.isna(code_occur_time):
            raise ValueError("code_occur_time is required")
        if contents is None or pd.isna(contents):
            raise ValueError("contents is required")

        if hasattr(code_occur_time, "to_pydatetime"):
            code_occur_time = code_occur_time.to_pydatetime()

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
            contents=str(contents),
        )

    def _nullable_str(self, value):
        if value is None or pd.isna(value):
            return None
        return str(value)

    def _nullable_int(self, value):
        if value is None or pd.isna(value):
            return None
        return int(value)

    def _build_error_row(self, row, exc: Exception) -> dict[str, DoseErrorValue]:
        try:
            raw = self._row_to_raw_log(row)
            return self._build_unparsed_row(raw)
        except Exception:
            code_occur_time = row.get("code_occur_time")
            if hasattr(code_occur_time, "to_pydatetime"):
                code_occur_time = code_occur_time.to_pydatetime()
            if pd.isna(code_occur_time):
                code_occur_time = datetime.min
            return self._empty_parsed_row(
                er_date=self._nullable_int(row.get("er_date")),
                er_index=self._nullable_int(row.get("er_index")),
                er_line=self._nullable_str(row.get("er_line")),
                eq_name=self._nullable_str(row.get("eq_name")),
                code=self._nullable_str(row.get("code")),
                code_occur_time=code_occur_time,
                belong=self._nullable_str(row.get("belong")),
                type=self._nullable_str(row.get("type")),
                title=self._nullable_str(row.get("title")),
                contents="" if row.get("contents") is None or pd.isna(row.get("contents")) else str(row.get("contents")),
            )

    def _build_unparsed_row(self, raw: RawErLog) -> dict[str, DoseErrorValue]:
        return self._empty_parsed_row(
            er_date=raw.er_date,
            er_index=raw.er_index,
            er_line=raw.er_line,
            eq_name=raw.eq_name,
            code=raw.code,
            code_occur_time=raw.code_occur_time,
            belong=raw.belong,
            type=raw.type,
            title=raw.title,
            contents=raw.contents,
        )

    def _empty_parsed_row(
        self,
        er_date: int | None,
        er_index: int | None,
        er_line: str | None,
        eq_name: str | None,
        code: str | None,
        code_occur_time: datetime,
        belong: str | None,
        type: str | None,
        title: str | None,
        contents: str,
    ) -> dict[str, DoseErrorValue]:
        return {
            "er_date": er_date,
            "er_index": er_index,
            "er_line": er_line,
            "eq_name": eq_name,
            "code": code,
            "code_occur_time": code_occur_time,
            "belong": belong,
            "type": type,
            "title": title,
            "contents": contents,
            "exposure_handle": None,
            "action_handle": None,
            "wafer_id": None,
            "de_err": None,
            "n_slit": None,
        }

    def _iter_day_starts(self, start_time: datetime, end_time: datetime):
        current = start_time.replace(hour=0, minute=0, second=0, microsecond=0)
        while current < end_time:
            yield current
            current += timedelta(days=1)
