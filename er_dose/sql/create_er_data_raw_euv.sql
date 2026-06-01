create schema if not exists mbeat;

create table if not exists mbeat.er_data_raw_euv (
    er_line          varchar(20) not null,
    eq_name          varchar(20) not null,
    er_type          varchar(10) not null,
    code             varchar(20),
    code_occur_time  timestamp,
    belong           varchar(12),
    "type"           varchar(8),
    title            varchar,
    contents         varchar,
    reason_code      varchar(20),
    task             varchar,
    compile_script   varchar
);

create index if not exists idx_er_data_raw_euv_occur_time
on mbeat.er_data_raw_euv (code_occur_time);

create index if not exists idx_er_data_raw_euv_line_eq_time
on mbeat.er_data_raw_euv (er_line, eq_name, code_occur_time);
