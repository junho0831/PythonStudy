create schema if not exists mbeat;

create table if not exists mbeat.er_dose_raw_equipment_count_log (
    id              bigserial primary key,
    target_date     date,
    eq_name         varchar(100) not null,
    source_count    bigint not null,
    target_count    bigint not null,
    created_at      timestamp default now() not null
);

-- PostgreSQL 9.4 compatible index creation.
do $$
begin
    if not exists (
        select 1 from pg_catalog.pg_indexes
        where schemaname = 'mbeat' and indexname = 'idx_er_dose_raw_equipment_count_log_date'
    ) then
        create index idx_er_dose_raw_equipment_count_log_date
        on mbeat.er_dose_raw_equipment_count_log (target_date, eq_name, created_at desc);
    end if;
end
$$;

create table if not exists mbeat.er_dose_euv_equipment_count_log (
    id              bigserial primary key,
    target_date     date,
    eq_name         varchar(100) not null,
    source_count    bigint not null,
    target_count    bigint not null,
    created_at      timestamp default now() not null
);

-- PostgreSQL 9.4 compatible index creation.
do $$
begin
    if not exists (
        select 1 from pg_catalog.pg_indexes
        where schemaname = 'mbeat' and indexname = 'idx_er_dose_euv_equipment_count_log_date'
    ) then
        create index idx_er_dose_euv_equipment_count_log_date
        on mbeat.er_dose_euv_equipment_count_log (target_date, eq_name, created_at desc);
    end if;
end
$$;
