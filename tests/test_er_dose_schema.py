from __future__ import annotations

from pathlib import Path


DDL_PATH = Path(__file__).resolve().parents[1] / "er_dose" / "sql" / "create_er_dose_raw_parsed.sql"
RAW_EUV_DDL_PATH = Path(__file__).resolve().parents[1] / "er_dose" / "sql" / "create_er_data_raw_euv.sql"
ROOT_CAUSE_DDL_PATH = Path(__file__).resolve().parents[1] / "er_dose" / "sql" / "create_er_dose_euv_parsed.sql"
ROOT_CAUSE_RENAME_PATH = Path(__file__).resolve().parents[1] / "er_dose" / "sql" / "rename_er_dose_euv_parsed_columns.sql"


def _ddl() -> str:
    return DDL_PATH.read_text(encoding="utf-8").lower()


def _raw_euv_ddl() -> str:
    return RAW_EUV_DDL_PATH.read_text(encoding="utf-8").lower()


def _root_cause_ddl() -> str:
    return ROOT_CAUSE_DDL_PATH.read_text(encoding="utf-8").lower()


def _root_cause_rename_sql() -> str:
    return ROOT_CAUSE_RENAME_PATH.read_text(encoding="utf-8").lower()


def test_parsed_table_primary_key_matches_documented_partition_key():
    ddl = _ddl()

    assert "primary key (code_occur_time)" in ddl
    assert "primary key (id, code_occur_time)" not in ddl
    assert "id                  bigserial" not in ddl


def test_parsed_table_has_line_eq_time_index():
    ddl = _ddl()

    assert "idx_er_dose_raw_parsed_line_eq_time" in ddl
    assert "(er_line, eq_name, code_occur_time)" in ddl
    assert "log_source" not in ddl
    assert "er_date             int4" in ddl
    assert "er_index            int4" in ddl
    assert "belong              varchar(12)" in ddl
    assert '"type"              varchar(8)' in ddl
    assert "title               varchar" in ddl
    assert "contents            varchar" in ddl
    assert "parser_version" not in ddl
    assert "parsing_status" not in ddl
    assert "parsing_error" not in ddl
    assert "idx_er_dose_error_line_eq_exposure_time" not in ddl
    assert "source_exposure_id" not in ddl
    assert "wafer_id            integer" in ddl
    assert "wafer_seq           integer" in ddl
    assert "de_err              numeric(12,7)" in ddl
    assert "n_slit              integer" in ddl


def test_parsed_table_documents_retained_parsed_columns():
    ddl = _ddl()

    assert "comment on column prism_common.er_dose_raw_parsed.wafer_id" in ddl
    assert "matches lot_report.slot_seq" in ddl
    assert "parsed from de_err" in ddl


def test_raw_euv_table_matches_source_schema():
    ddl = _raw_euv_ddl()

    assert "create table if not exists mbeat.er_data_raw_euv" in ddl
    assert "er_line          varchar(20) not null" in ddl
    assert "eq_name          varchar(20) not null" in ddl
    assert "er_type          varchar(10) not null" in ddl
    assert '"type"           varchar(8)' in ddl
    assert "idx_er_data_raw_euv_occur_time" in ddl


def test_root_cause_table_is_fe_facing_matching_table():
    ddl = _root_cause_ddl()

    assert "create table if not exists prism_common.er_dose_euv_parsed" in ddl
    assert "partition by range (code_occur_time)" in ddl
    assert "scanner_exposure_handle bigint" not in ddl
    assert "er_type                 varchar(10)" in ddl
    assert "belong                  varchar(12)" in ddl
    assert '"type"                  varchar(8)' in ddl
    assert "title                   varchar" in ddl
    assert "contents                varchar" in ddl
    assert "reason_code             varchar(20)" in ddl
    assert "task                    varchar" in ddl
    assert "compile_script          varchar" in ddl
    assert "exposure_id             bigint" in ddl
    assert "source_code             varchar(20)" not in ddl
    assert "source_log_source" not in ddl
    assert "source_belong           varchar(12)" not in ddl
    assert "source_type             varchar(8)" not in ddl
    assert "match_status" not in ddl
    assert "dose_error              numeric(12,7)" not in ddl
    assert "dose_error_detected_in_file text" in ddl
    assert "min_dose_error          numeric(12,7)" in ddl
    assert "max_dose_error          numeric(12,7)" in ddl
    assert "pulses_euv_0_6dt_tot    integer" in ddl
    assert "software_version        text" in ddl
    assert "parser_version" not in ddl
    assert "raw_description" not in ddl
    assert "idx_er_dose_euv_parsed_line_eq_time" in ddl
    assert "(er_line, eq_name, code_occur_time)" in ddl
    assert "idx_er_dose_root_cause_scanner_exposure" not in ddl
    assert "idx_er_dose_root_cause_source_exposure" not in ddl
    assert "independent from er_dose_raw_parsed" in ddl
    assert "er_data_raw_euv" in ddl


def test_root_cause_rename_sql_renames_existing_columns():
    sql = _root_cause_rename_sql()

    assert "call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'source_exposure_id', 'exposure_id')" in sql
    assert "call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'exposure id', 'exposure_id')" in sql
    assert "call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'source_file_name', 'dose_error_detected_in_file')" in sql
    assert "call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'dose error detected in file', 'dose_error_detected_in_file')" in sql
    assert "call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'root_cause_message', 'root_cause')" in sql
    assert "call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'min. dose error', 'min_dose_error')" in sql
    assert "call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'pulses_euv<0.6dt_tot', 'pulses_euv_0_6dt_tot')" in sql
    assert "call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'pulses_euv_lt_0_6dt_tot', 'pulses_euv_0_6dt_tot')" in sql
    assert "call rename_column_if_exists('prism_common.er_dose_euv_parsed', 'software version', 'software_version')" in sql
    assert "from pg_inherits" in sql
