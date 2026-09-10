create schema if not exists mbeat;

create table if not exists mbeat.batch_event_log (
    id              bigserial primary key,
    batch_name      varchar(100) not null,
    target_date     date,
    event_type      varchar(50) not null,
    message         text,
    data            jsonb not null default '{}'::jsonb,
    created_at      timestamp default now() not null
);

create index if not exists idx_batch_event_log_batch_date
on mbeat.batch_event_log (batch_name, target_date, created_at desc);
