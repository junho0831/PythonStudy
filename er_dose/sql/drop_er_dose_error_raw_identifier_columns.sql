drop index if exists mbeat.uq_er_dose_error_parsed_raw_time;

alter table if exists mbeat.er_dose_error_parsed
    drop column if exists er_date,
    drop column if exists er_index,
    drop column if exists raw_id;
