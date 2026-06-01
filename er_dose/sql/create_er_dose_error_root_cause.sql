create schema if not exists mbeat;

create table if not exists mbeat.er_dose_error_root_cause (
    er_line                 varchar(20),
    eq_name                 varchar(20),
    er_type                 varchar(10),
    code                    varchar(20),
    code_occur_time         timestamp(6) not null,
    belong                  varchar(12),
    "type"                  varchar(8),
    title                   varchar,
    contents                varchar,
    reason_code             varchar(20),
    task                    varchar,
    compile_script          varchar,
    source_exposure_id      bigint,
    source_code_occur_time  timestamp(6),
    dose_error              numeric(12,7),
    source_file_name        text,
    root_cause_code         text,
    root_cause_message      text,
    exposure_length         numeric(12,7),
    duty_cycle              numeric(12,7),
    min_dose_error          numeric(12,7),
    max_dose_error          numeric(12,7),
    on_drop_euv_energy      numeric(12,7),
    on_drop_pp_energy       numeric(12,7),
    on_drop_mp_energy       numeric(12,7),
    on_drop_pp_dlgc1        numeric(12,7),
    on_drop_mp_dlgc1        numeric(12,7),
    bi_cell_y_3sigma        numeric(12,7),
    fdsc_y_error            numeric(12,7),
    fdsc_y_3sigma           numeric(12,7),
    max_cross_interval      numeric(12,7),
    xint_3sigma             numeric(12,7),
    euv_3sigma              numeric(12,7),
    pulses_euv_lt_0_6dt_tot integer,
    fed_pulses              integer,
    l2dx_maxce              numeric(12,7),
    l2dy_maxce              numeric(12,7),
    sensitivity_at_l2dx_maxce numeric(12,7),
    sensitivity_at_l2dy_maxce numeric(12,7),
    dose_margin             numeric(12,7),
    l2dx_qc_etdc_3sigma     numeric(12,7),
    l2dx_qc_etdc_median     numeric(12,7),
    l2dy_qc_etdc_3sigma     numeric(12,7),
    l2dy_qc_etdc_median     numeric(12,7),
    rbdy_peak_frequency_hf  numeric(12,7),
    rbdy_peak_frequency_lf  numeric(12,7),
    rbdy_peak_frequency_mf  numeric(12,7),
    rbdy_peak_power_hf      numeric(12,7),
    rbdy_qc_etdc_3sigma     numeric(12,7),
    rbdy_total_power_lf     numeric(12,7),
    rbdy_total_power_mf     numeric(12,7),
    software_version        text,
    created_at              timestamp default now()
)
partition by range (code_occur_time);

create index if not exists idx_er_dose_root_cause_line_eq_time
on mbeat.er_dose_error_root_cause (er_line, eq_name, code_occur_time);

comment on table mbeat.er_dose_error_root_cause is
'FE-facing dose error root cause table. Independent from er_dose_error_parsed; source description candidates are read from er_data_raw_euv.';
