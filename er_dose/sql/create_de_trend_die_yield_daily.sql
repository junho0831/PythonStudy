create table if not exists prism_common.de_trend_die_yield_daily (
    occur_date      date not null,
    eq_name         varchar not null,
    total_die       int8 default 0 not null,
    reject_shot     int8 default 0 not null,
    to_repair_die   int8 default 0 not null,
    repair_nok      int8 default 0 not null,
    total_wafer     int8 default 0 not null,
    reject_wafer    int8 default 0 not null,
    created_at      timestamp default now() null,
    primary key (occur_date, eq_name)
);
