from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Iterator

import pandas as pd

from er_dose.common.sql_filters import active_nxe_eq_filter
from er_dose.infra.postgres_db import PostgresDB


MAIN_RAW_TABLE = "mbeat.er_data_raw"
PARSED_TABLE = "prism_common.er_dose_raw_parsed"
TARGET_CODES = (
    "DW-3411",
    "DW-3425",
    "DW-343A",
    "DW-343B",
    "LO-0050",
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
        chunk_size: int = 10000,
    ) -> Iterator[pd.DataFrame]:
        current_start = start_time
        while current_start < end_time:
            next_day_start = datetime.combine(current_start.date() + timedelta(days=1), datetime.min.time())
            current_end = min(next_day_start, end_time)

            query, params = self._build_fetch_raw_logs_query(
                start_time=current_start,
                end_time=current_end,
            )
            yield from self.db.select_in_chunks(query, params=params, chunk_size=chunk_size)

            current_start = current_end

    def fetch_latest_lot_states(self, start_time: datetime) -> dict[str, dict[str, int | str | None]]:
        previous_day_start = datetime.combine((start_time - timedelta(days=1)).date(), datetime.min.time())
        query = f"""
            select distinct on (p.eq_name)
                p.eq_name,
                p.lot_id,
                p.lot_name,
                p.lot_seq,
                p.wafer_seq
            from {PARSED_TABLE} p
            where p.code_occur_time >= :previous_day_start
              and p.code_occur_time < :start_time
              and p.eq_name is not null
              and (p.lot_id is not null or p.lot_name is not null or p.lot_seq is not null or p.wafer_seq is not null)
              and {active_nxe_eq_filter("p.eq_name")}
            order by p.eq_name, p.code_occur_time desc
        """
        df = self.db.select(query, params={"previous_day_start": previous_day_start, "start_time": start_time})
        if df is None or df.empty:
            return {}

        lot_states: dict[str, dict[str, int | str | None]] = {}
        for _, row in df.iterrows():
            eq_name = row["eq_name"]
            if pd.isna(eq_name):
                continue
            lot_states[str(eq_name)] = {
                "lot_id": None if pd.isna(row.get("lot_id")) else str(row["lot_id"]),
                "lot_name": None if pd.isna(row.get("lot_name")) else str(row["lot_name"]),
                "lot_seq": None if pd.isna(row.get("lot_seq")) else int(row["lot_seq"]),
                "wafer_seq": None if pd.isna(row.get("wafer_seq")) else int(row["wafer_seq"]),
            }
        return lot_states

    def fetch_source_count(self, target_date: date) -> int:
        start_time = datetime.combine(target_date, datetime.min.time())
        end_time = start_time + timedelta(days=1)
        target_codes_sql = ", ".join(f"'{code}'" for code in TARGET_CODES)
        query = f"""
            select count(*) as row_count
            from {MAIN_RAW_TABLE} r
            where r.code_occur_time >= :start_time
              and r.code_occur_time < :end_time
              and r.code in ({target_codes_sql})
              and {active_nxe_eq_filter("r.eq_name")}
        """
        df = self.db.select(query, params={"start_time": start_time, "end_time": end_time})
        if df is None or df.empty:
            return 0
        return int(df.iloc[0]["row_count"])

    def fetch_target_count(self, target_date: date) -> int:
        start_time = datetime.combine(target_date, datetime.min.time())
        end_time = start_time + timedelta(days=1)
        target_codes_sql = ", ".join(f"'{code}'" for code in TARGET_CODES)
        query = f"""
            select count(*) as row_count
            from {PARSED_TABLE} p
            where p.code_occur_time >= :start_time
              and p.code_occur_time < :end_time
              and p.code in ({target_codes_sql})
              and {active_nxe_eq_filter("p.eq_name")}
        """
        df = self.db.select(query, params={"start_time": start_time, "end_time": end_time})
        if df is None or df.empty:
            return 0
        return int(df.iloc[0]["row_count"])

    def truncate_target_partition(self, target_date: date, connection=None) -> int:
        parsed_table = self._partition_table_name(PARSED_TABLE, target_date)
        return self.db.execute(f"truncate table {parsed_table}", connection=connection)

    def _build_fetch_raw_logs_query(
        self,
        start_time: datetime,
        end_time: datetime,
    ) -> tuple[str, dict[str, datetime]]:
        params = {
            "start_time": start_time,
            "end_time": end_time,
        }

        target_codes_sql = ", ".join(f"'{code}'" for code in TARGET_CODES)
        raw_table = self._partition_table_name(MAIN_RAW_TABLE, start_time.date())

        query = f"""
            select
                r.eq_name,
                r.code,
                r.code_occur_time,
                r.title,
                r.contents
            from {raw_table} r
            where r.code_occur_time >= :start_time
              and r.code_occur_time < :end_time
              and r.code in ({target_codes_sql})
              and {active_nxe_eq_filter("r.eq_name")}
            order by r.code_occur_time, r.eq_name, r.er_date, r.er_index
        """
        return query, params


    def _partition_table_name(self, table_name: str, target_date: date) -> str:
        return f'{table_name}_1_prt_p{target_date.strftime("%Y%m%d")}'


    def insert_parsed_df(self, parsed_df):
        if parsed_df is None or parsed_df.empty:
            return 0

        # prism_common.er_dose_raw_parsed 에 존재하는 컬럼만 적재한다.
        table_columns = [
            "eq_name",
            "code",
            "code_occur_time",
            "title",
            "contents",
            "exposure_handle",
            "action_handle",
            "lot_id",
            "lot_name",
            "lot_seq",
            "wafer_seq",
            "de_err",
            "n_slit",
            "created_at",
            "use_yn",
        ]

        df_to_insert = parsed_df.copy()
        if "created_at" not in df_to_insert.columns:
            df_to_insert["created_at"] = datetime.now()
        if "use_yn" not in df_to_insert.columns:
            df_to_insert["use_yn"] = "Y"

        # COPY 대상 테이블 컬럼과 정확히 맞춘다.
        insert_columns = [col for col in table_columns if col in df_to_insert.columns]
        df_to_insert = df_to_insert[insert_columns].copy()

        int_columns = [
            "exposure_handle",
            "action_handle",
            "lot_seq",
            "wafer_seq",
            "n_slit",
        ]
        for column in int_columns:
            if column in df_to_insert.columns:
                df_to_insert[column] = pd.to_numeric(df_to_insert[column], errors="coerce").astype("Int64")

        schema, table_name = PARSED_TABLE.split(".", maxsplit=1)
        target_date = pd.Timestamp(df_to_insert.iloc[0]["code_occur_time"]).strftime("%Y-%m-%d")
        print(
            "[ER_DOSE] "
            f"partition_date={target_date} "
            f"rows={len(df_to_insert)}"
        )
        self.db.copy_insert_to_partition_table(
            schema=schema,
            table_name=table_name,
            target_date=target_date,
            df=df_to_insert,
        )
        return len(df_to_insert)

    def analyze_target_partition(self, target_date: str, connection=None) -> int:
        partition_table = f"{PARSED_TABLE}_1_prt_p{target_date.replace('-', '')}"
        return self.db.execute(f"ANALYZE {partition_table}", connection=connection)

    def upsert_die_yield_daily_summary(self, target_date: date | str, connection=None) -> int:
        if isinstance(target_date, str):
            target_date = datetime.strptime(target_date, "%Y-%m-%d").date()
        start_time = datetime.combine(target_date, datetime.min.time())
        end_time = start_time + timedelta(days=1)

        query = f"""
            insert into prism_common.de_trend_die_yield_daily (
                occur_date,
                eq_name,
                total_die,
                reject_shot,
                to_repair_die,
                repair_nok,
                total_wafer,
                reject_wafer,
                created_at
            )
            with target_lot_finish as (
                select
                    f.eq_name,
                    f.lot_seq,
                    f.code_occur_time as finish_time
                from {PARSED_TABLE} f
                where f.code_occur_time >= :start_time
                  and f.code_occur_time < :end_time
                  and f.code = 'LO-0051'
                  and f.lot_seq is not null
            ),
            lot_process_data as (
                select
                    f.eq_name,
                    f.lot_seq,
                    max(s.code_occur_time) as start_time,
                    f.finish_time
                from target_lot_finish f
                join {PARSED_TABLE} s
                  on s.eq_name = f.eq_name
                 and s.lot_seq = f.lot_seq
                 and s.code = 'LO-0050'
                 and s.code_occur_time < f.finish_time
                group by f.eq_name, f.lot_seq, f.finish_time
            ),
            wafer_code_data as (
                select
                    p.finish_time::date as occur_date,
                    d.eq_name,
                    d.lot_seq,
                    d.wafer_seq,
                    count(*) filter (where d.code = 'DW-3411') as normal_die,
                    count(*) filter (where d.code = 'DW-3425') as reject_shot,
                    count(*) filter (where d.code = 'DW-343A') as to_repair_die,
                    count(*) filter (where d.code = 'DW-343B') as repair_nok,
                    max(case when d.code in ('DW-3425', 'DW-343B') then 1 else 0 end) as reject_yn,
                    max(case when d.lot_seq is not null and d.wafer_seq is not null then 1 else 0 end) as valid_wafer_yn
                from lot_process_data p
                join {PARSED_TABLE} d
                  on d.eq_name = p.eq_name
                 and d.lot_seq = p.lot_seq
                 and d.code_occur_time >= p.start_time
                 and d.code_occur_time <= p.finish_time
                where d.code in ('DW-3411', 'DW-3425', 'DW-343A', 'DW-343B')
                group by p.finish_time::date, d.eq_name, d.lot_seq, d.wafer_seq
            )
            select
                occur_date,
                eq_name,
                sum(normal_die + reject_shot + repair_nok) as total_die,
                sum(reject_shot) as reject_shot,
                sum(to_repair_die) as to_repair_die,
                sum(repair_nok) as repair_nok,
                sum(valid_wafer_yn) as total_wafer,
                sum(case when valid_wafer_yn = 1 then reject_yn else 0 end) as reject_wafer,
                now() as created_at
            from wafer_code_data
            group by occur_date, eq_name
            on conflict (occur_date, eq_name)
            do update set
                total_die = excluded.total_die,
                reject_shot = excluded.reject_shot,
                to_repair_die = excluded.to_repair_die,
                repair_nok = excluded.repair_nok,
                total_wafer = excluded.total_wafer,
                reject_wafer = excluded.reject_wafer,
                created_at = now();
        """
        return self.db.execute(query, params={"start_time": start_time, "end_time": end_time}, connection=connection)

    def upsert_root_cause_daily_summary(self, target_date: date | str, connection=None) -> int:
        if isinstance(target_date, str):
            target_date = datetime.strptime(target_date, "%Y-%m-%d").date()
        start_time = datetime.combine(target_date, datetime.min.time())
        end_time = start_time + timedelta(days=1)

        query = """
            insert into prism_common.de_trend_root_cause_daily (
                occur_date,
                eq_name,
                root_cause,
                frequency,
                created_at
            )
            select
                p.code_occur_time::date as occur_date,
                p.eq_name,
                p.root_cause,
                count(*) as frequency,
                now() as created_at
            from prism_common.er_dose_euv_parsed p
            where p.code_occur_time >= :start_time
              and p.code_occur_time < :end_time
              and p.eq_name is not null
              and p.root_cause is not null
            group by p.code_occur_time::date, p.eq_name, p.root_cause
            on conflict (occur_date, eq_name, root_cause)
            do update set
                frequency = excluded.frequency,
                created_at = now();
        """
        return self.db.execute(query, params={"start_time": start_time, "end_time": end_time}, connection=connection)

    def transaction(self):
        return self.db.transaction()
