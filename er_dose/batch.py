from __future__ import annotations

from dataclasses import asdict
from datetime import datetime

import pandas as pd

from er_dose.infra.postgres_db import PostgresDB
from er_dose.parsers.base import RawErLog
from er_dose.parsers.registry import parse_raw_er_log


RAW_TABLE = "mbeat.er_data_raw"
PARSED_TABLE = "mbeat.er_dose_error_parsed"


class ERDoseBatch:
    def __init__(self, db: PostgresDB):
        self.db = db

    def run(self, start_time: datetime, end_time: datetime, limit: int | None = None) -> dict[str, int]:
        raw_df = self.fetch_raw_logs(start_time=start_time, end_time=end_time, limit=limit)
        parsed_rows = []
        skipped_count = 0
        failed_samples = []

        for _, row in raw_df.iterrows():
            try:
                raw = self._row_to_raw_log(row)
                parsed = parse_raw_er_log(raw)
                if parsed is None:
                    skipped_count += 1
                    continue
                parsed_rows.append(asdict(parsed))
            except Exception as exc:
                if len(failed_samples) < 5:
                    failed_samples.append(f"raw_id={row.get('raw_id')} error={exc}")

        insert_count = 0
        if parsed_rows:
            parsed_df = pd.DataFrame(parsed_rows)
            insert_count = self.db.bulk_insert_df(
                PARSED_TABLE,
                parsed_df,
                on_conflict_column="raw_id",
            )

        summary = {
            "fetched": int(len(raw_df)),
            "parsed": int(len(parsed_rows)),
            "inserted": int(insert_count),
            "skipped": int(skipped_count),
            "failed": int(len(failed_samples)),
        }

        print(
            "[ER_DOSE] "
            f"fetched={summary['fetched']} "
            f"parsed={summary['parsed']} "
            f"inserted={summary['inserted']} "
            f"skipped={summary['skipped']} "
            f"failed={summary['failed']}"
        )
        for sample in failed_samples:
            print(f"[ER_DOSE][PARSE_FAIL] {sample}")

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
                r.id as raw_id,
                r.er_line,
                r.eq_name,
                r.code,
                r.code_occur_time,
                r.contents
            from {RAW_TABLE} r
            where r.code_occur_time >= %(start_time)s
              and r.code_occur_time < %(end_time)s
              and r.contents is not null
              and not exists (
                  select 1
                  from {PARSED_TABLE} p
                  where p.raw_id = r.id
              )
            order by r.code_occur_time, r.id
            {limit_sql}
        """
        return self.db.fetch_df(query, params=params)

    def _row_to_raw_log(self, row) -> RawErLog:
        raw_id = row.get("raw_id")
        code_occur_time = row.get("code_occur_time")
        contents = row.get("contents")

        if pd.isna(raw_id):
            raise ValueError("raw_id is required")
        if pd.isna(code_occur_time):
            raise ValueError("code_occur_time is required")
        if contents is None or pd.isna(contents):
            raise ValueError("contents is required")

        if hasattr(code_occur_time, "to_pydatetime"):
            code_occur_time = code_occur_time.to_pydatetime()

        return RawErLog(
            raw_id=int(raw_id),
            er_line=self._nullable_str(row.get("er_line")),
            eq_name=self._nullable_str(row.get("eq_name")),
            code=self._nullable_str(row.get("code")),
            code_occur_time=code_occur_time,
            contents=str(contents),
        )

    def _nullable_str(self, value):
        if value is None or pd.isna(value):
            return None
        return str(value)

