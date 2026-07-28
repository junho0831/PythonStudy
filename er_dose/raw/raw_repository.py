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

    def insert_parsed_df(self, df: pd.DataFrame, connection=None, analyze: bool = True) -> int:
        if df is None or df.empty:
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

        df_to_insert = df.copy()
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

        # 파티션 날짜별로 나눠 적재한다.
        df_to_insert["_target_date"] = (
            pd.to_datetime(df_to_insert["code_occur_time"]).dt.strftime("%Y-%m-%d")
        )

        schema, table_name = PARSED_TABLE.split(".", maxsplit=1)

        inserted_count = 0
        for target_date, group_df in df_to_insert.groupby("_target_date"):
            group_df_clean = group_df.drop(columns=["_target_date"])
            print(
                "[ER_DOSE] "
                f"partition_date={target_date} "
                f"rows={len(group_df_clean)}"
            )
            self.db.copy_insert_to_partition_table(
                schema=schema,
                table_name=table_name,
                target_date=target_date,
                df=group_df_clean,
                connection=connection,
                analyze=analyze,
            )
            inserted_count += len(group_df_clean)

        return inserted_count

    def analyze_target_partition(self, target_date: str, connection=None) -> int:
        partition_table = f"{PARSED_TABLE}_1_prt_p{target_date.replace('-', '')}"
        return self.db.execute(f"ANALYZE {partition_table}", connection=connection)

    def transaction(self):
        return self.db.transaction()
