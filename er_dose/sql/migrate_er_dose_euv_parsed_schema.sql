do $$
declare
    target_table regclass := to_regclass('prism_common.er_dose_euv_parsed');
begin
    if target_table is null then
        raise notice 'table prism_common.er_dose_euv_parsed does not exist; skip migration';
        return;
    end if;

    if exists (select 1 from pg_attribute where attrelid = target_table and attname = 'er_line' and attnum > 0 and not attisdropped) then
        alter table prism_common.er_dose_euv_parsed drop column er_line;
    end if;
    if exists (select 1 from pg_attribute where attrelid = target_table and attname = 'belong' and attnum > 0 and not attisdropped) then
        alter table prism_common.er_dose_euv_parsed drop column belong;
    end if;
    if exists (select 1 from pg_attribute where attrelid = target_table and attname = 'type' and attnum > 0 and not attisdropped) then
        alter table prism_common.er_dose_euv_parsed drop column "type";
    end if;
end $$;

drop index if exists prism_common.idx_er_dose_euv_parsed_line_eq_time;

create index if not exists idx_er_dose_euv_parsed_eq_time
on prism_common.er_dose_euv_parsed (eq_name, code_occur_time);
