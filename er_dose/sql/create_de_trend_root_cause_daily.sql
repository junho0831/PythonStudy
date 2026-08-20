create table if not exists prism_common.de_trend_root_cause_daily (
    occur_date      date not null,
    eq_name         varchar not null,
    root_cause      varchar not null,
    frequency       int8 default 0 not null,
    created_at      timestamp default now() null,
    primary key (occur_date, eq_name, root_cause)
);
