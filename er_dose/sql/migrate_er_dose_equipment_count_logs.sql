-- Stop log writers and apply create_er_dose_equipment_count_logs.sql first.
-- Supports both JSON and per-equipment column versions of batch_event_log.
-- The DO statement is atomic; it does not change summary DELETE/INSERT commits.
do $$
declare
    old_batch text;
    destination text;
    json_format boolean;
begin
    if not exists (
        select 1 from information_schema.tables
        where table_schema = 'mbeat' and table_name = 'batch_event_log'
    ) then
        return;
    end if;

    lock table mbeat.batch_event_log in access exclusive mode;
    select exists (
        select 1 from information_schema.columns
        where table_schema = 'mbeat' and table_name = 'batch_event_log' and column_name = 'data'
    ) into json_format;

    if json_format then
        if exists (
            select 1 from mbeat.batch_event_log
            where batch_name in ('ER_DOSE_RAW', 'ER_DOSE_EUV')
              and event_type = 'EQUIPMENT_COUNT'
              and jsonb_typeof(data -> 'equipment_counts') is distinct from 'array'
        ) then
            raise exception 'Unsupported equipment-count payload; original logs were not changed';
        end if;
    end if;

    for old_batch, destination in
        select * from (values
            ('ER_DOSE_RAW', 'er_dose_raw_equipment_count_log'),
            ('ER_DOSE_EUV', 'er_dose_euv_equipment_count_log')
        ) as targets(batch_name, table_name)
    loop
        if json_format then
            execute format($query$
                insert into mbeat.%I (target_date, eq_name, source_count, target_count, created_at)
                select l.target_date, e.item ->> 'eq_name',
                       (e.item ->> 'source_count')::bigint,
                       (e.item ->> 'target_count')::bigint, l.created_at
                from mbeat.batch_event_log l
                cross join lateral jsonb_array_elements(l.data -> 'equipment_counts') as e(item)
                where l.batch_name = $1 and l.event_type = 'EQUIPMENT_COUNT'
            $query$, destination) using old_batch;
        else
            execute format($query$
                insert into mbeat.%I (target_date, eq_name, source_count, target_count, created_at)
                select target_date, eq_name, source_count, target_count, created_at
                from mbeat.batch_event_log
                where batch_name = $1 and event_type = 'EQUIPMENT_COUNT'
            $query$, destination) using old_batch;
        end if;
    end loop;

    delete from mbeat.batch_event_log
    where batch_name in ('ER_DOSE_RAW', 'ER_DOSE_EUV') and event_type = 'EQUIPMENT_COUNT';
    -- Retain unrelated batches/events in the legacy table, if any.
    if not exists (select 1 from mbeat.batch_event_log) then
        drop table mbeat.batch_event_log;
    end if;
end
$$;
