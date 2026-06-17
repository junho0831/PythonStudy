drop index if exists prism_common.uq_er_dose_error_parsed_raw_time;

alter table if exists prism_common.er_dose_error_parsed
    drop column if exists er_date,
    drop column if exists er_index,
    drop column if exists raw_id;
