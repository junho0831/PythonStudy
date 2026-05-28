create schema if not exists mbeat;

create table if not exists mbeat.er_dose_error_parsed (
    id                  bigserial,
    er_date             integer,
    er_index            integer,
    raw_id              bigint,
    er_line             varchar(20),
    eq_name             varchar(20),
    code                varchar(20),
    code_occur_time     timestamp(6) not null,
    code_occur_time_raw varchar(40),
    log_source          varchar(20),
    exposure_handle     bigint,
    action_handle       bigint,
    wafer_seq           integer,
    shot_seq            integer,
    field_seq           integer,
    repair_yn           boolean,
    repair_result       varchar(20),
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
    parser_version      varchar(20),
    parsing_status      varchar(20),
    parsing_error       text,
    raw_contents        text,
    created_at          timestamp default now(),
    primary key (id, code_occur_time)
)
partition by range (code_occur_time);

create unique index if not exists uq_er_dose_error_parsed_raw_time
on mbeat.er_dose_error_parsed (er_date, er_index, code_occur_time);

create index if not exists idx_er_dose_error_eq_time
on mbeat.er_dose_error_parsed (eq_name, code_occur_time);

create index if not exists idx_er_dose_error_code_time
on mbeat.er_dose_error_parsed (code, code_occur_time);

create index if not exists idx_er_dose_error_exposure_time
on mbeat.er_dose_error_parsed (exposure_handle, code_occur_time);
