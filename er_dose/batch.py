from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from decimal import Decimal

import pandas as pd

from er_dose.infra.postgres_db import PostgresDB
from er_dose.parsers.base import RawErLog
from er_dose.parsers.registry import parse_raw_er_log


RAW_TABLE = "mbeat.er_data_raw"
PARSED_TABLE = "mbeat.er_dose_error_parsed"
PARSER_VERSION = "v1"
DoseErrorValue = Decimal | int | bool | str | datetime | None


class ERDoseBatch:
    def __init__(self, db: PostgresDB):
        self.db = db

    def run(self, start_time: datetime, end_time: datetime, limit: int | None = None) -> dict[str, int]:
        self.ensure_partitions(start_time=start_time, end_time=end_time)
        raw_df = self.fetch_raw_logs(start_time=start_time, end_time=end_time, limit=limit)
        parsed_rows = []
        success_count = 0
        regex_fail_count = 0
        parser_error_count = 0

        for _, row in raw_df.iterrows():
            try:
                raw = self._row_to_raw_log(row)
                parsed = parse_raw_er_log(raw)
                if parsed is None:
                    regex_fail_count += 1
                    parsed_rows.append(self._build_status_row(raw, "REGEX_FAIL", "No parser matched contents."))
                    continue
                parsed_rows.append(asdict(parsed))
                success_count += 1
            except Exception as exc:
                parser_error_count += 1
                parsed_rows.append(self._build_error_row(row, exc))

        insert_count = 0
        with self.db.transaction() as connection:
            if parsed_rows:
                parsed_df = pd.DataFrame(parsed_rows)
                insert_count = self.db.bulk_insert_df(PARSED_TABLE, parsed_df, connection=connection)

        summary = {
            "fetched": int(len(raw_df)),
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
                r.er_line,
                r.eq_name,
                r.code,
                r.code_occur_time,
                r.belong,
                r.type,
                r.contents
            from {RAW_TABLE} r
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
        return self.db.fetch_df(query, params=params)

    def ensure_partitions(self, start_time: datetime, end_time: datetime) -> None:
        for month_start in self._iter_month_starts(start_time, end_time):
            next_month = self._next_month(month_start)
            partition_name = f"er_dose_error_parsed_{month_start:%Y%m}"
            query = f"""
                create table if not exists mbeat.{partition_name}
                partition of {PARSED_TABLE}
                for values from (%(start_time)s) to (%(end_time)s)
            """
            self.db.execute(
                query,
                params={
                    "start_time": month_start,
                    "end_time": next_month,
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
            er_line=self._nullable_str(row.get("er_line")),
            eq_name=self._nullable_str(row.get("eq_name")),
            code=self._nullable_str(row.get("code")),
            code_occur_time=code_occur_time,
            code_occur_time_raw=self._format_timestamp_raw(code_occur_time),
            log_source=self._build_log_source(row),
            contents=str(contents),
        )

    def _nullable_str(self, value):
        if value is None or pd.isna(value):
            return None
        return str(value)

    def _build_error_row(self, row, exc: Exception) -> dict[str, DoseErrorValue]:
        try:
            raw = self._row_to_raw_log(row)
            return self._build_status_row(raw, "PARSER_ERROR", str(exc))
        except Exception:
            code_occur_time = row.get("code_occur_time")
            if hasattr(code_occur_time, "to_pydatetime"):
                code_occur_time = code_occur_time.to_pydatetime()
            if pd.isna(code_occur_time):
                code_occur_time = datetime.min
            return self._empty_parsed_row(
                er_line=self._nullable_str(row.get("er_line")),
                eq_name=self._nullable_str(row.get("eq_name")),
                code=self._nullable_str(row.get("code")),
                code_occur_time=code_occur_time,
                code_occur_time_raw=self._format_timestamp_raw(code_occur_time),
                log_source=self._build_log_source(row),
                raw_contents="" if row.get("contents") is None or pd.isna(row.get("contents")) else str(row.get("contents")),
                parsing_status="PARSER_ERROR",
                parsing_error=str(exc),
            )

    def _build_status_row(self, raw: RawErLog, parsing_status: str, parsing_error: str | None) -> dict[str, DoseErrorValue]:
        return self._empty_parsed_row(
            er_line=raw.er_line,
            eq_name=raw.eq_name,
            code=raw.code,
            code_occur_time=raw.code_occur_time,
            code_occur_time_raw=raw.code_occur_time_raw,
            log_source=raw.log_source,
            raw_contents=raw.contents,
            parsing_status=parsing_status,
            parsing_error=parsing_error,
        )

    def _empty_parsed_row(
        self,
        er_line: str | None,
        eq_name: str | None,
        code: str | None,
        code_occur_time: datetime,
        code_occur_time_raw: str,
        log_source: str | None,
        raw_contents: str,
        parsing_status: str,
        parsing_error: str | None,
    ) -> dict[str, DoseErrorValue]:
        return {
            "er_line": er_line,
            "eq_name": eq_name,
            "code": code,
            "code_occur_time": code_occur_time,
            "code_occur_time_raw": code_occur_time_raw,
            "log_source": log_source,
            "exposure_handle": None,
            "action_handle": None,
            "wafer_seq": None,
            "shot_seq": None,
            "field_seq": None,
            "repair_yn": None,
            "repair_result": None,
            "dose_error": None,
            "dose_warn_level": None,
            "de_err": None,
            "de_warn_lvl": None,
            "eset": None,
            "freq": None,
            "n_slit": None,
            "mb_enabled": None,
            "function_name": None,
            "result_type": None,
            "parser_version": PARSER_VERSION,
            "parsing_status": parsing_status,
            "parsing_error": parsing_error,
            "raw_contents": raw_contents,
        }

    def _format_timestamp_raw(self, value: datetime) -> str:
        return value.strftime("%Y-%m-%d %H:%M:%S.%f")

    def _build_log_source(self, row) -> str | None:
        belong = self._nullable_str(row.get("belong"))
        log_type = self._nullable_str(row.get("type"))
        if belong and log_type:
            return f"{belong}:{log_type}"
        return belong or log_type

    def _iter_month_starts(self, start_time: datetime, end_time: datetime):
        current = start_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        while current < end_time:
            yield current
            current = self._next_month(current)

    def _next_month(self, value: datetime) -> datetime:
        if value.month == 12:
            return value.replace(year=value.year + 1, month=1)
        return value.replace(month=value.month + 1)
