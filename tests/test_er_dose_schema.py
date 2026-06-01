from __future__ import annotations

from pathlib import Path


DDL_PATH = Path(__file__).resolve().parents[1] / "er_dose" / "sql" / "create_er_dose_error_parsed.sql"
PARSED_ALTER_PATH = Path(__file__).resolve().parents[1] / "er_dose" / "sql" / "alter_er_dose_error_parsed_preserve_source_fields.sql"
RAW_EUV_DDL_PATH = Path(__file__).resolve().parents[1] / "er_dose" / "sql" / "create_er_data_raw_euv.sql"
ROOT_CAUSE_DDL_PATH = Path(__file__).resolve().parents[1] / "er_dose" / "sql" / "create_er_dose_error_root_cause.sql"
ROOT_CAUSE_ALTER_PATH = (
    Path(__file__).resolve().parents[1] / "er_dose" / "sql" / "alter_er_dose_error_root_cause_add_euv_metrics.sql"
)


def _ddl() -> str:
    return DDL_PATH.read_text(encoding="utf-8").lower()


def _parsed_alter() -> str:
    return PARSED_ALTER_PATH.read_text(encoding="utf-8").lower()


def _raw_euv_ddl() -> str:
    return RAW_EUV_DDL_PATH.read_text(encoding="utf-8").lower()


def _root_cause_ddl() -> str:
    return ROOT_CAUSE_DDL_PATH.read_text(encoding="utf-8").lower()


def _root_cause_alter() -> str:
    return ROOT_CAUSE_ALTER_PATH.read_text(encoding="utf-8").lower()


def test_parsed_table_primary_key_matches_documented_partition_key():
    ddl = _ddl()

    assert "primary key (code_occur_time)" in ddl
    assert "primary key (id, code_occur_time)" not in ddl
    assert "id                  bigserial" not in ddl


def test_parsed_table_has_line_eq_time_index():
    ddl = _ddl()

    assert "idx_er_dose_error_line_eq_time" in ddl
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
    assert "source_exposure_id  bigint" in ddl
    assert "idx_er_dose_error_source_exposure_time" not in ddl


def test_parsed_existing_table_migration_preserves_raw_source_fields():
    ddl = _parsed_alter()

    assert "alter table mbeat.er_dose_error_parsed" in ddl
    assert "drop column if exists log_source" in ddl
    assert "drop column if exists raw_contents" in ddl
    assert "drop column if exists repair_yn" in ddl
    assert "drop column if exists repair_result" in ddl
    assert "drop column if exists parser_version" in ddl
    assert "drop column if exists parsing_status" in ddl
    assert "drop column if exists parsing_error" in ddl
    assert "add column if not exists er_date int4" in ddl
    assert "add column if not exists er_index int4" in ddl
    assert "add column if not exists er_line varchar(20)" in ddl
    assert "add column if not exists eq_name varchar(20)" in ddl
    assert "add column if not exists code varchar(20)" in ddl
    assert "add column if not exists code_occur_time timestamp(6)" in ddl
    assert "add column if not exists belong varchar(12)" in ddl
    assert 'add column if not exists "type" varchar(8)' in ddl
    assert "add column if not exists title varchar" in ddl
    assert "add column if not exists contents varchar" in ddl


def test_parsed_table_documents_lot_report_sequence_semantics():
    ddl = _ddl()

    assert "matches lot_report.slot_seq" in ddl
    assert "not lot_report.wafer_id" in ddl
    assert "max(abs(dose error))" in ddl
    assert "po_sd_slot_info_detail.dose_err_tot_valn" in ddl


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

    assert "create table if not exists mbeat.er_dose_error_root_cause" in ddl
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
    assert "source_exposure_id      bigint" in ddl
    assert "source_code             varchar(20)" not in ddl
    assert "source_log_source" not in ddl
    assert "source_belong           varchar(12)" not in ddl
    assert "source_type             varchar(8)" not in ddl
    assert "match_status" not in ddl
    assert "source_file_name        text" in ddl
    assert "min_dose_error          numeric(12,7)" in ddl
    assert "max_dose_error          numeric(12,7)" in ddl
    assert "pulses_euv_lt_0_6dt_tot integer" in ddl
    assert "software_version        text" in ddl
    assert "parser_version" not in ddl
    assert "raw_description" not in ddl
    assert "idx_er_dose_root_cause_line_eq_time" in ddl
    assert "(er_line, eq_name, code_occur_time)" in ddl
    assert "idx_er_dose_root_cause_scanner_exposure" not in ddl
    assert "idx_er_dose_root_cause_source_exposure" not in ddl
    assert "independent from er_dose_error_parsed" in ddl
    assert "er_data_raw_euv" in ddl


def test_root_cause_existing_table_migration_adds_euv_metric_columns():
    ddl = _root_cause_alter()

    assert "alter table mbeat.er_dose_error_root_cause" in ddl
    assert "drop column if exists source_log_source" in ddl
    assert "drop column if exists source_belong" in ddl
    assert "drop column if exists source_type" in ddl
    assert "drop column if exists raw_description" in ddl
    assert "drop column if exists scanner_exposure_handle" in ddl
    assert "drop column if exists source_code" in ddl
    assert "drop column if exists match_status" in ddl
    assert "drop column if exists parser_version" in ddl
    assert "add column if not exists er_type varchar(10)" in ddl
    assert "add column if not exists belong varchar(12)" in ddl
    assert 'add column if not exists "type" varchar(8)' in ddl
    assert "add column if not exists title varchar" in ddl
    assert "add column if not exists contents varchar" in ddl
    assert "add column if not exists reason_code varchar(20)" in ddl
    assert "add column if not exists task varchar" in ddl
    assert "add column if not exists compile_script varchar" in ddl
    assert "add column if not exists source_belong varchar(12)" not in ddl
    assert "add column if not exists source_type varchar(8)" not in ddl
    assert "add column if not exists source_file_name text" in ddl
    assert "add column if not exists min_dose_error numeric(12,7)" in ddl
    assert "add column if not exists pulses_euv_lt_0_6dt_tot integer" in ddl
    assert "add column if not exists software_version text" in ddl
