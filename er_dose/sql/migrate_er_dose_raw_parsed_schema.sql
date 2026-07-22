do $$
declare
    target_table regclass := to_regclass('prism_common.er_dose_raw_parsed');
begin
    if target_table is null then
        raise notice 'table prism_common.er_dose_raw_parsed does not exist; skip migration';
        return;
    end if;

    if exists (select 1 from pg_attribute where attrelid = target_table and attname = 'wafer_id' and attnum > 0 and not attisdropped)
       and not exists (select 1 from pg_attribute where attrelid = target_table and attname = 'lot_seq' and attnum > 0 and not attisdropped) then
        alter table prism_common.er_dose_raw_parsed rename column wafer_id to lot_seq;
    elsif exists (select 1 from pg_attribute where attrelid = target_table and attname = 'wafer_id' and attnum > 0 and not attisdropped) then
        alter table prism_common.er_dose_raw_parsed drop column wafer_id;
    end if;

    if exists (select 1 from pg_attribute where attrelid = target_table and attname = 'er_date' and attnum > 0 and not attisdropped) then
        alter table prism_common.er_dose_raw_parsed drop column er_date;
    end if;
    if exists (select 1 from pg_attribute where attrelid = target_table and attname = 'er_index' and attnum > 0 and not attisdropped) then
        alter table prism_common.er_dose_raw_parsed drop column er_index;
    end if;
    if exists (select 1 from pg_attribute where attrelid = target_table and attname = 'er_line' and attnum > 0 and not attisdropped) then
        alter table prism_common.er_dose_raw_parsed drop column er_line;
    end if;
    if exists (select 1 from pg_attribute where attrelid = target_table and attname = 'belong' and attnum > 0 and not attisdropped) then
        alter table prism_common.er_dose_raw_parsed drop column belong;
    end if;
    if exists (select 1 from pg_attribute where attrelid = target_table and attname = 'type' and attnum > 0 and not attisdropped) then
        alter table prism_common.er_dose_raw_parsed drop column "type";
    end if;

    if not exists (select 1 from pg_attribute where attrelid = target_table and attname = 'lot_id' and attnum > 0 and not attisdropped) then
        alter table prism_common.er_dose_raw_parsed add column lot_id varchar;
    end if;
    if not exists (select 1 from pg_attribute where attrelid = target_table and attname = 'lot_name' and attnum > 0 and not attisdropped) then
        alter table prism_common.er_dose_raw_parsed add column lot_name varchar;
    end if;
    if not exists (select 1 from pg_attribute where attrelid = target_table and attname = 'use_yn' and attnum > 0 and not attisdropped) then
        alter table prism_common.er_dose_raw_parsed add column use_yn varchar(1) default 'Y';
    end if;

    alter table prism_common.er_dose_raw_parsed
    alter column use_yn set default 'Y';

    if exists (select 1 from pg_attribute where attrelid = target_table and attname = 'lot_seq' and attnum > 0 and not attisdropped) then
        comment on column prism_common.er_dose_raw_parsed.lot_seq is
        'ER lot sequence parsed from the raw message. Starts at 1 and matches lot_report.slot_seq.';
    end if;
    if exists (select 1 from pg_attribute where attrelid = target_table and attname = 'use_yn' and attnum > 0 and not attisdropped) then
        comment on column prism_common.er_dose_raw_parsed.use_yn is
        'Y for normal parsed rows. N for DW exposure handle jump rows kept for count consistency but excluded from normal analysis.';
    end if;
end $$;

drop index if exists prism_common.idx_er_dose_raw_parsed_line_eq_time;

create index if not exists idx_er_dose_raw_parsed_eq_time
on prism_common.er_dose_raw_parsed (eq_name, code_occur_time);
