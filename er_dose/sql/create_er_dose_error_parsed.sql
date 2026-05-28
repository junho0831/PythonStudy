create schema if not exists mbeat;

create table if not exists mbeat.er_dose_error_parsed (
    raw_id             bigint not null,
    er_line            varchar(20),
    eq_name            varchar(20),
    code               varchar(20),
    code_occur_time    timestamp not null,
    exposure_handle    bigint,
    action_handle      bigint,
    dose_error         numeric(12,7),
    dose_warn_level    numeric(12,7),
    de_err             numeric(12,7),
    de_warn_lvl        numeric(12,7),
    eset               bigint,
    freq               integer,
    n_slit             integer,
    mb_enabled         boolean,
    function_name      text,
    result_type        text,
    raw_contents       text not null,
    created_at         timestamp not null default now(),
    constraint uq_er_dose_error_parsed_raw_id unique (raw_id)
);

create index if not exists idx_er_dose_error_time
on mbeat.er_dose_error_parsed (code_occur_time);

create index if not exists idx_er_dose_error_eq_time
on mbeat.er_dose_error_parsed (eq_name, code_occur_time);

create index if not exists idx_er_dose_error_exposure
on mbeat.er_dose_error_parsed (exposure_handle);

