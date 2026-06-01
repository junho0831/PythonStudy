create schema if not exists mbeat;

create table if not exists mbeat.er_dose_error_parsed (
    er_date             int4,
    er_index            int4,
    er_line             varchar(20),
    eq_name             varchar(20),
    code                varchar(20),
    code_occur_time     timestamp(6) not null,
    belong              varchar(12),
    "type"              varchar(8),
    title               varchar,
    contents            varchar,
    exposure_handle     bigint,
    source_exposure_id  bigint,
    action_handle       bigint,
    wafer_seq           integer,
    shot_seq            integer,
    field_seq           integer,
    dose_error          numeric(12,7),
    dose_warn_level     numeric(12,7),
    de_err              numeric(12,7),
    de_warn_lvl         numeric(12,7),
    eset                bigint,
    freq                integer,
    n_slit              integer,
    mb_enabled          boolean,
    function_name       text,
    result_type         text,
    created_at          timestamp default now(),
    primary key (code_occur_time)
)
partition by range (code_occur_time);

create index if not exists idx_er_dose_error_line_eq_time
on mbeat.er_dose_error_parsed (er_line, eq_name, code_occur_time);

comment on column mbeat.er_dose_error_parsed.wafer_seq is
'ER wafer sequence / slot sequence. Starts at 1 and matches lot_report.slot_seq, not lot_report.wafer_id.';

comment on column mbeat.er_dose_error_parsed.dose_error is
'ER max(abs(dose error)) value. Equivalent to po_sd_slot_info_detail.dose_err_tot_valn when available.';

comment on column mbeat.er_dose_error_parsed.source_exposure_id is
'Source exposure id parsed from ER log description when exported with the scanner dose warning.';
