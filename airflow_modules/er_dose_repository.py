"""Airflow-owned queries. Deploy independently of the remote parser project."""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd

from airflow_modules.er_dose_db import PostgresDB

MAIN_RAW_TABLE = "mbeat.er_data_raw"
PARSED_TABLE = "prism_common.er_dose_raw_parsed"
EUV_RAW_TABLE = "mbeat.er_data_raw_euv"
ROOT_CAUSE_TABLE = "prism_common.er_dose_euv_parsed"
TARGET_CODES = (
    "DW-3411",
    "DW-3425",
    "DW-343A",
    "DW-343B",
    "LO-0050",
    "LO-0051",
    "LO-0052",
    "LO-0061",
    "LO-8166",
    "LO-8167",
    "KE-9103",
    "KE-9104",
)


def active_nxe_eq_filter(column_name: str) -> str:
    return f"""{column_name} in (
                  select eqp.eqp_id
                  from prism_dev.photo_eqp_info eqp
                  where eqp.use_yn = 'Y'
                    and eqp.eqp_model_name like 'NXE%'
              )"""


def insert_equipment_count(
    db: PostgresDB,
    table_name: str,
    target_date: date,
    rows: list[dict[str, Any]],
) -> None:
    if not rows:
        return

    df = pd.DataFrame(rows, columns=["eq_name", "source_count", "target_count"])
    df.insert(0, "target_date", target_date)
    db.bulk_insert_df(table_name, df)


def write_equipment_count_log(
    repository,
    start_time: datetime,
    end_time: datetime,
) -> None:
    rows = repository.fetch_equipment_counts(start_time, end_time)
    repository.delete_equipment_count(target_date=start_time.date())
    if rows:
        repository.insert_equipment_count(target_date=start_time.date(), rows=rows)


class ERDoseRepository:
    def __init__(self, db: PostgresDB):
        self.db = db

    def delete_equipment_count(self, target_date: date) -> None:
        query = """
            delete from mbeat.er_dose_raw_equipment_count_log
            where target_date = :target_date
        """
        self.db.execute(query, params={"target_date": target_date})

    def insert_equipment_count(
        self,
        target_date: date,
        rows: list[dict[str, Any]],
    ) -> None:
        insert_equipment_count(
            self.db,
            table_name="mbeat.er_dose_raw_equipment_count_log",
            target_date=target_date,
            rows=rows,
        )

    def fetch_source_count(self, target_date: date, distinct: bool = False) -> int:
        start_time = datetime.combine(target_date, datetime.min.time())
        end_time = start_time + timedelta(days=1)
        target_codes_sql = ", ".join(f"'{code}'" for code in TARGET_CODES)
        count_sql = "count(distinct (r.eq_name, r.code, r.code_occur_time))" if distinct else "count(*)"
        query = f"""
            select {count_sql} as row_count
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

    def fetch_equipment_counts(self, start_time: datetime, end_time: datetime) -> list[dict[str, Any]]:
        target_codes_sql = ", ".join(f"'{code}'" for code in TARGET_CODES)
        query = f"""
            with source_counts as (
                select
                    r.eq_name,
                    count(*) as source_count
                from {MAIN_RAW_TABLE} r
                where r.code_occur_time >= :start_time
                  and r.code_occur_time < :end_time
                  and r.code in ({target_codes_sql})
                  and {active_nxe_eq_filter("r.eq_name")}
                group by r.eq_name
            ),
            target_counts as (
                select
                    p.eq_name,
                    count(*) as target_count
                from {PARSED_TABLE} p
                where p.code_occur_time >= :start_time
                  and p.code_occur_time < :end_time
                  and p.code in ({target_codes_sql})
                  and {active_nxe_eq_filter("p.eq_name")}
                group by p.eq_name
            )
            select
                coalesce(s.eq_name, t.eq_name) as eq_name,
                coalesce(s.source_count, 0) as source_count,
                coalesce(t.target_count, 0) as target_count
            from source_counts s
            full outer join target_counts t on t.eq_name = s.eq_name
            order by eq_name
        """
        df = self.db.select(query, params={"start_time": start_time, "end_time": end_time})
        if df is None or df.empty:
            return []
        return [
            {"eq_name": row.eq_name, "source_count": int(row.source_count), "target_count": int(row.target_count)}
            for row in df.itertuples(index=False)
        ]

    def delete_die_yield_daily_summary(self, start_time: datetime, end_time: datetime) -> None:
        delete_query = """
            delete from prism_common.de_trend_die_yield_daily
            where occur_date >= cast(:start_time as date)
              and occur_date < :end_time
        """
        self.db.execute(delete_query, params={"start_time": start_time, "end_time": end_time})

    def insert_die_yield_daily_summary(self, start_time: datetime, end_time: datetime) -> None:
        insert_query = f"""
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
            group by occur_date, eq_name;
        """
        self.db.execute(insert_query, params={"start_time": start_time, "end_time": end_time})

    def delete_root_cause_daily_summary(self, start_time: datetime, end_time: datetime) -> None:
        delete_query = """
            delete from prism_common.de_trend_root_cause_daily
            where occur_date >= cast(:start_time as date)
              and occur_date < :end_time
        """
        self.db.execute(delete_query, params={"start_time": start_time, "end_time": end_time})

    def insert_root_cause_daily_summary(self, start_time: datetime, end_time: datetime) -> None:
        insert_query = """
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
            group by p.code_occur_time::date, p.eq_name, p.root_cause;
        """
        self.db.execute(insert_query, params={"start_time": start_time, "end_time": end_time})



class ERDoseEUVRepository:
    def __init__(self, db: PostgresDB):
        self.db = db

    def delete_equipment_count(self, target_date: date) -> None:
        query = """
            delete from mbeat.er_dose_euv_equipment_count_log
            where target_date = :target_date
        """
        self.db.execute(query, params={"target_date": target_date})

    def insert_equipment_count(
        self,
        target_date: date,
        rows: list[dict[str, Any]],
    ) -> None:
        insert_equipment_count(
            self.db,
            table_name="mbeat.er_dose_euv_equipment_count_log",
            target_date=target_date,
            rows=rows,
        )

    def fetch_source_count(self, target_date: date, distinct: bool = False) -> int:
        start_time = datetime.combine(target_date, datetime.min.time())
        end_time = start_time + timedelta(days=1)
        count_sql = "count(distinct (r.eq_name, r.code, r.code_occur_time))" if distinct else "count(*)"
        query = f"""
            select {count_sql} as row_count
            from {EUV_RAW_TABLE} r
            where r.code_occur_time >= :start_time
              and r.code_occur_time < :end_time
              and r.code = 'OSD-0200'
        """
        df = self.db.select(query, params={"start_time": start_time, "end_time": end_time})
        if df is None or df.empty:
            return 0
        return int(df.iloc[0]["row_count"])

    def fetch_target_count(self, target_date: date) -> int:
        start_time = datetime.combine(target_date, datetime.min.time())
        end_time = start_time + timedelta(days=1)
        query = f"""
            select count(*) as row_count
            from {ROOT_CAUSE_TABLE} p
            where p.code_occur_time >= :start_time
              and p.code_occur_time < :end_time
              and p.code = 'OSD-0200'
        """
        df = self.db.select(query, params={"start_time": start_time, "end_time": end_time})
        if df is None or df.empty:
            return 0
        return int(df.iloc[0]["row_count"])

    def fetch_equipment_counts(self, start_time: datetime, end_time: datetime) -> list[dict[str, Any]]:
        query = f"""
            with source_counts as (
                select
                    r.eq_name,
                    count(*) as source_count
                from {EUV_RAW_TABLE} r
                where r.code_occur_time >= :start_time
                  and r.code_occur_time < :end_time
                  and r.code = 'OSD-0200'
                group by r.eq_name
            ),
            target_counts as (
                select
                    p.eq_name,
                    count(*) as target_count
                from {ROOT_CAUSE_TABLE} p
                where p.code_occur_time >= :start_time
                  and p.code_occur_time < :end_time
                  and p.code = 'OSD-0200'
                group by p.eq_name
            )
            select
                coalesce(s.eq_name, t.eq_name) as eq_name,
                coalesce(s.source_count, 0) as source_count,
                coalesce(t.target_count, 0) as target_count
            from source_counts s
            full outer join target_counts t on t.eq_name = s.eq_name
            order by eq_name
        """
        df = self.db.select(query, params={"start_time": start_time, "end_time": end_time})
        if df is None or df.empty:
            return []
        return [
            {"eq_name": row.eq_name, "source_count": int(row.source_count), "target_count": int(row.target_count)}
            for row in df.itertuples(index=False)
        ]
