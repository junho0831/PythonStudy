create schema if not exists prism_common;

create table if not exists prism_common.er_dose_euv_parsed (
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
    exposure_id             bigint,
    time                    timestamp(6),
    dose_error_detected_in_file text,
    root_cause_code         text,
    root_cause              text,
    exposure_length         numeric(12,7),
    duty_cycle              numeric(12,7),
    min_dose_error          numeric(12,7),
    max_dose_error          numeric(12,7),
    on_drop_euv_energy      numeric(12,7),
    on_drop_pp_energy       numeric(12,7),
    on_drop_mp_energy       numeric(12,7),
    on_drop_pp_dlgc_1       numeric(12,7),
    on_drop_mp_dlgc_1       numeric(12,7),
    bi_cell_y_3sigma        numeric(12,7),
    fdsc_y_error            numeric(12,7),
    fdsc_y_3sigma           numeric(12,7),
    max_cross_interval      numeric(12,7),
    xint_3sigma             numeric(12,7),
    euv_3sigma              numeric(12,7),
    pulses_euv_0_6dt_tot    integer,
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

create index if not exists idx_er_dose_euv_parsed_line_eq_time
on prism_common.er_dose_euv_parsed (er_line, eq_name, code_occur_time);

comment on table prism_common.er_dose_euv_parsed is
'FE-facing dose error root cause table. Independent from er_dose_raw_parsed; source description candidates are read from er_data_raw_euv.';
