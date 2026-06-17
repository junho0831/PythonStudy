create schema if not exists prism_common;

create table if not exists prism_common.er_dose_error_parsed (
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
    action_handle       bigint,
    wafer_id            integer,
    wafer_seq           integer,
    de_err              numeric(12,7),
    n_slit              integer,
    created_at          timestamp default now(),
    primary key (code_occur_time)
)
partition by range (code_occur_time);

create index if not exists idx_er_dose_error_line_eq_time
on prism_common.er_dose_error_parsed (er_line, eq_name, code_occur_time);

comment on column prism_common.er_dose_error_parsed.wafer_id is
'ER wafer id / slot sequence parsed from the raw message. Starts at 1 and matches lot_report.slot_seq.';

comment on column prism_common.er_dose_error_parsed.de_err is
'Dose error value parsed from de_err in the raw message when available.';
