from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Iterator

import pandas as pd

from er_dose.common.sql_filters import active_nxe_eq_filter
from er_dose.infra.postgres_db import PostgresDB


EUV_RAW_TABLE = "mbeat.er_data_raw_euv"
ROOT_CAUSE_TABLE = "prism_common.er_dose_euv_parsed"

PARSED_TO_DB_COLUMN_MAP = {
    "source_exposure_id": "exposure_id",
    "source_code_occur_time": "time",
    "source_file_name": "dose_error_detected_in_file",
    "root_cause_message": "root_cause",
    "exposure_length": "exposure_length",
    "duty_cycle": "duty_cycle",
    "min_dose_error": "min_dose_error",
    "max_dose_error": "max_dose_error",
    "on_drop_euv_energy": "on_drop_euv_energy",
    "on_drop_pp_energy": "on_drop_pp_energy",
    "on_drop_mp_energy": "on_drop_mp_energy",
    "on_drop_pp_dlgc1": "on_drop_pp_dlgc_1",
    "on_drop_mp_dlgc1": "on_drop_mp_dlgc_1",
    "bi_cell_y_3sigma": "bi_cell_y_3sigma",
    "fdsc_y_error": "fdsc_y_error",
    "fdsc_y_3sigma": "fdsc_y_3sigma",
    "max_cross_interval": "max_cross_interval",
    "xint_3sigma": "xint_3sigma",
    "euv_3sigma": "euv_3sigma",
    "pulses_euv_0_6dt_tot": "pulses_euv_0_6dt_tot",
    "fed_pulses": "fed_pulses",
    "l2dx_maxce": "l2dx_maxce",
    "l2dy_maxce": "l2dy_maxce",
    "sensitivity_at_l2dx_maxce": "sensitivity_at_l2dx_maxce",
    "sensitivity_at_l2dy_maxce": "sensitivity_at_l2dy_maxce",
    "dose_margin": "dose_margin",
    "l2dx_qc_etdc_3sigma": "l2dx_qc_etdc_3sigma",
    "l2dx_qc_etdc_median": "l2dx_qc_etdc_median",
    "l2dy_qc_etdc_3sigma": "l2dy_qc_etdc_3sigma",
    "l2dy_qc_etdc_median": "l2dy_qc_etdc_median",
    "rbdy_peak_frequency_hf": "rbdy_peak_frequency_hf",
    "rbdy_peak_frequency_lf": "rbdy_peak_frequency_lf",
    "rbdy_peak_frequency_mf": "rbdy_peak_frequency_mf",
    "rbdy_peak_power_hf": "rbdy_peak_power_hf",
    "rbdy_qc_etdc_3sigma": "rbdy_qc_etdc_3sigma",
    "rbdy_total_power_lf": "rbdy_total_power_lf",
    "rbdy_total_power_mf": "rbdy_total_power_mf",
    "software_version": "software_version",
}


class ERDoseEUVRepository:
    def __init__(self, db: PostgresDB):
        self.db = db

    def fetch_source_count(self, target_date: date) -> int:
        start_time = datetime.combine(target_date, datetime.min.time())
        end_time = start_time + timedelta(days=1)
        query = f"""
            select count(*) as row_count
            from {EUV_RAW_TABLE} r
            where r.code_occur_time >= :start_time
              and r.code_occur_time < :end_time
              and lower(r.contents) like '%dose error detected in file:%'
              and lower(r.contents) like '%root cause%'
              and {active_nxe_eq_filter("r.eq_name")}
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
              and {active_nxe_eq_filter("p.eq_name")}
        """
        df = self.db.select(query, params={"start_time": start_time, "end_time": end_time})
        if df is None or df.empty:
            return 0
        return int(df.iloc[0]["row_count"])

    def truncate_target_partition(self, target_date: date, connection=None) -> int:
        parsed_table = self._partition_table_name(ROOT_CAUSE_TABLE, target_date)
        return self.db.execute(f"truncate table {parsed_table}", connection=connection)

    def fetch_raw_logs_in_chunks(
        self,
        start_time: datetime,
        end_time: datetime,
        chunk_size: int = 10000,
    ) -> Iterator[pd.DataFrame]:
        params = {
            "start_time": start_time,
            "end_time": end_time,
        }
        query = f"""
            select
                r.eq_name,
                r.er_type,
                r.code,
                r.code_occur_time,
                r.title,
                r.contents,
                r.reason_code,
                r.task,
                r.compile_script
            from {EUV_RAW_TABLE} r
            where r.code_occur_time >= :start_time
              and r.code_occur_time < :end_time
              and {active_nxe_eq_filter("r.eq_name")}
            order by r.code_occur_time, r.eq_name, r.er_line
        """
        return self.db.select_in_chunks(query, params=params, chunk_size=chunk_size)

    def insert_root_causes_df(self, df: pd.DataFrame, connection=None, analyze: bool = True) -> int:
        if df is None or df.empty:
            return 0

        df_to_insert = df.rename(columns=PARSED_TO_DB_COLUMN_MAP).copy()

        table_columns = [
            "eq_name",
            "er_type",
            "code",
            "code_occur_time",
            "title",
            "contents",
            "reason_code",
            "task",
            "compile_script",
            "exposure_id",
            "time",
            "dose_error_detected_in_file",
            "root_cause_code",
            "root_cause",
            "exposure_length",
            "duty_cycle",
            "min_dose_error",
            "max_dose_error",
            "on_drop_euv_energy",
            "on_drop_pp_energy",
            "on_drop_mp_energy",
            "on_drop_pp_dlgc_1",
            "on_drop_mp_dlgc_1",
            "bi_cell_y_3sigma",
            "fdsc_y_error",
            "fdsc_y_3sigma",
            "max_cross_interval",
            "xint_3sigma",
            "euv_3sigma",
            "pulses_euv_0_6dt_tot",
            "fed_pulses",
            "l2dx_maxce",
            "l2dy_maxce",
            "sensitivity_at_l2dx_maxce",
            "sensitivity_at_l2dy_maxce",
            "dose_margin",
            "l2dx_qc_etdc_3sigma",
            "l2dx_qc_etdc_median",
            "l2dy_qc_etdc_3sigma",
            "l2dy_qc_etdc_median",
            "rbdy_peak_frequency_hf",
            "rbdy_peak_frequency_lf",
            "rbdy_peak_frequency_mf",
            "rbdy_peak_power_hf",
            "rbdy_qc_etdc_3sigma",
            "rbdy_total_power_lf",
            "rbdy_total_power_mf",
            "software_version",
            "created_at",
        ]

        if "created_at" not in df_to_insert.columns:
            df_to_insert["created_at"] = datetime.now()

        df_to_insert = df_to_insert[[col for col in table_columns if col in df_to_insert.columns]].copy()

        int_columns = [
            "exposure_id",
            "pulses_euv_0_6dt_tot",
            "fed_pulses",
        ]
        for column in int_columns:
            if column in df_to_insert.columns:
                df_to_insert[column] = pd.to_numeric(df_to_insert[column], errors="coerce").astype("Int64")

        df_to_insert["_target_date"] = pd.to_datetime(df_to_insert["code_occur_time"]).dt.strftime("%Y-%m-%d")

        schema, table_name = ROOT_CAUSE_TABLE.split(".", maxsplit=1)

        inserted_count = 0
        for target_date, group_df in df_to_insert.groupby("_target_date"):
            group_df_clean = group_df.drop(columns=["_target_date"])
            print(
                "[ER_DOSE_EUV] "
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
        partition_table = f"{ROOT_CAUSE_TABLE}_1_prt_p{target_date.replace('-', '')}"
        return self.db.execute(f"ANALYZE {partition_table}", connection=connection)

    def upsert_root_cause_daily_summary(self, target_date: date | str, connection=None) -> int:
        if isinstance(target_date, str):
            target_date = datetime.strptime(target_date, "%Y-%m-%d").date()
        start_time = datetime.combine(target_date, datetime.min.time())
        end_time = start_time + timedelta(days=1)

        query = f"""
            insert into prism_common.de_trend_root_cause_daily (
                occur_date,
                eq_name,
                root_cause,
                frequency,
                created_at
            )
            with root_cause_data as (
                select
                    e.code_occur_time::date as occur_date,
                    e.eq_name,
                    replace(
                        trim(
                            split_part(
                                case
                                    when e.root_cause like '%CE & MP%' then replace(e.root_cause, 'CE & MP', 'CE @ MP')
                                    when e.root_cause like '%l2Dx & l2Dy%' then replace(e.root_cause, 'L2Dx & L2Dy', 'L2Dx @ L2Dx')
                                    when e.root_cause like '%E&T%' then replace(e.root_cause, 'E&T', 'E@T')
                                    else e.root_cause
                                end,
                                '&',
                                1
                            )
                        ),
                        '@',
                        '&'
                    ) as root_cause
                from {ROOT_CAUSE_TABLE} e
                where e.code_occur_time >= :start_time
                  and e.code_occur_time < :end_time
                  and e.code = 'OSD-0200'
                  and e.root_cause is not null
                  and trim(e.root_cause) != ''
            )
            select
                occur_date,
                eq_name,
                root_cause,
                count(*) as frequency,
                now() as created_at
            from root_cause_data
            where root_cause is not null
              and root_cause != ''
            group by occur_date, eq_name, root_cause
            on conflict (occur_date, eq_name, root_cause)
            do update set
                frequency = excluded.frequency,
                created_at = now();
        """
        return self.db.execute(query, params={"start_time": start_time, "end_time": end_time}, connection=connection)

    def transaction(self):
        return self.db.transaction()

    def _partition_table_name(self, table_name: str, target_date: date) -> str:
        return f'{table_name}_1_prt_p{target_date.strftime("%Y%m%d")}'
