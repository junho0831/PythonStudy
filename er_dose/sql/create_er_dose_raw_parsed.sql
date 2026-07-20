create schema if not exists prism_common;

create table if not exists prism_common.er_dose_raw_parsed (
    eq_name             varchar(20),
    code                varchar(20),
    code_occur_time     timestamp(6) not null,
    title               varchar,
    contents            varchar,
    exposure_handle     bigint,
    action_handle       bigint,
    lot_id              varchar,
    lot_name            varchar,
    lot_seq             integer,
    wafer_seq           integer,
    de_err              numeric(12,7),
    n_slit              integer,
    created_at          timestamp default now(),
    primary key (code_occur_time)
)
partition by range (code_occur_time);

create index if not exists idx_er_dose_raw_parsed_eq_time
on prism_common.er_dose_raw_parsed (eq_name, code_occur_time);

comment on column prism_common.er_dose_raw_parsed.lot_seq is
'ER lot sequence parsed from the raw message. Starts at 1 and matches lot_report.slot_seq.';

comment on column prism_common.er_dose_raw_parsed.de_err is
'Dose error value parsed from de_err in the raw message when available.';
