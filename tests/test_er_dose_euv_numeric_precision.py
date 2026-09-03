from pathlib import Path


SQL_DIR = Path(__file__).resolve().parents[1] / "er_dose" / "sql"
DDL_PATH = SQL_DIR / "create_er_dose_euv_parsed.sql"
MIGRATION_PATH = SQL_DIR / "migrate_er_dose_euv_parsed_numeric_precision.sql"

NUMERIC_COLUMNS = {
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
}


def test_euv_metric_columns_use_numeric_20_10():
    ddl = DDL_PATH.read_text(encoding="utf-8").lower()
    columns = {
        line.strip().split()[0]
        for line in ddl.splitlines()
        if "numeric(20,10)" in line
    }

    assert columns == NUMERIC_COLUMNS
    assert "numeric(12,7)" not in ddl


def test_numeric_precision_migration_alters_parent_once():
    sql = MIGRATION_PATH.read_text(encoding="utf-8").lower()
    alter_lines = [
        line.strip()
        for line in sql.splitlines()
        if line.strip().startswith("alter column")
    ]
    columns = {line.split()[2] for line in alter_lines}

    assert "alter table if exists prism_common.er_dose_euv_parsed" in sql
    assert "alter table only" not in sql
    assert columns == NUMERIC_COLUMNS
    assert len(alter_lines) == len(NUMERIC_COLUMNS)
    assert all("type numeric(20,10)" in line for line in alter_lines)
