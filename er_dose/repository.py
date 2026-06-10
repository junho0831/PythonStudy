from __future__ import annotations

from datetime import datetime, timedelta

from er_dose.infra.postgres_db import PostgresDB


MAIN_RAW_TABLE = "mbeat.er_data_raw"
PARSED_TABLE = "mbeat.er_dose_error_parsed"
TARGET_CODES = (
    "DW-3411",
    "DW-3425",
    "DW-343A",
    "DW-343B",
    "LO-0061",
    "LO-8166",
    "LO-8167",
    "KE-9103",
    "KE-9104",
)


class ERDoseRepository:
    def __init__(self, db: PostgresDB):
        self.db = db

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

        target_codes_sql = ", ".join(f"'{code.replace('-', '').upper()}'" for code in TARGET_CODES)

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

    def insert_parsed_df(self, df, connection=None):
        return self.db.copy_insert_df(PARSED_TABLE, df, connection=connection)

    def transaction(self):
        return self.db.transaction()

    def _iter_day_starts(self, start_time: datetime, end_time: datetime):
        current = start_time.replace(hour=0, minute=0, second=0, microsecond=0)
        while current < end_time:
            yield current
            current += timedelta(days=1)
