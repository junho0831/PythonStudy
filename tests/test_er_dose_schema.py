from __future__ import annotations

from pathlib import Path


DDL_PATH = Path(__file__).resolve().parents[1] / "er_dose" / "sql" / "create_er_dose_error_parsed.sql"


def _ddl() -> str:
    return DDL_PATH.read_text(encoding="utf-8").lower()


def test_parsed_table_primary_key_matches_documented_partition_key():
    ddl = _ddl()

    assert "primary key (code_occur_time)" in ddl
    assert "primary key (id, code_occur_time)" not in ddl
    assert "id                  bigserial" not in ddl


def test_parsed_table_has_documented_exposure_sequence_index():
    ddl = _ddl()

    assert "idx_er_dose_error_line_eq_exposure_time" in ddl
    assert "(er_line, eq_name, exposure_handle, code_occur_time)" in ddl
