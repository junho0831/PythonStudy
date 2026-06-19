from __future__ import annotations

from datetime import datetime
from typing import Iterator

import pandas as pd

from er_dose.infra.postgres_db import PostgresDB


EUV_RAW_TABLE = "mbeat.er_data_raw_euv"
ROOT_CAUSE_TABLE = "prism_common.er_dose_error_root_cause"


class ERDoseEUVRepository:
    def __init__(self, db: PostgresDB):
        self.db = db

    def fetch_raw_logs_in_chunks(
        self,
        start_time: datetime,
        end_time: datetime,
        chunk_size: int = 10000,
    ) -> Iterator[pd.DataFrame]:
        query = f"""
            select
                r.er_line,
                r.eq_name,
                r.er_type,
                r.code,
                r.code_occur_time,
                r.belong,
                r."type" as type,
                r.title,
                r.contents,
                r.reason_code,
                r.task,
                r.compile_script
            from {EUV_RAW_TABLE} r
            where r.code_occur_time >= :start_time
              and r.code_occur_time < :end_time
            order by r.code_occur_time, r.eq_name, r.er_line
        """
        params = {
            "start_time": start_time,
            "end_time": end_time,
        }
        return self.db.select_in_chunks(query, params=params, chunk_size=chunk_size)

    def insert_root_causes_df(self, df: pd.DataFrame) -> int:
        if df is None or df.empty:
            return 0

        table_columns = [
            "er_line",
            "eq_name",
            "er_type",
            "code",
            "code_occur_time",
            "belong",
            "type",
            "title",
            "contents",
            "reason_code",
            "task",
            "compile_script",
            "source_exposure_id",
            "source_code_occur_time",
            "dose_error",
            "source_file_name",
            "root_cause_code",
            "root_cause_message",
            "exposure_length",
            "duty_cycle",
            "min_dose_error",
            "max_dose_error",
            "on_drop_euv_energy",
            "on_drop_pp_energy",
            "on_drop_mp_energy",
            "on_drop_pp_dlgc1",
            "on_drop_mp_dlgc1",
            "bi_cell_y_3sigma",
            "fdsc_y_error",
            "fdsc_y_3sigma",
            "max_cross_interval",
            "xint_3sigma",
            "euv_3sigma",
            "pulses_euv_lt_0_6dt_tot",
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

        df_to_insert = df[[col for col in table_columns if col in df.columns]].copy()
        if "created_at" not in df_to_insert.columns:
            df_to_insert["created_at"] = datetime.now()

        int_columns = [
            "source_exposure_id",
            "pulses_euv_lt_0_6dt_tot",
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
            )
            inserted_count += len(group_df_clean)

        return inserted_count
