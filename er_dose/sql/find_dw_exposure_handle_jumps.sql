with exposure_flow as (
    select
        eq_name,
        code,
        code_occur_time,
        er_date,
        er_index,
        exposure_handle,
        lag(exposure_handle) over (
            partition by eq_name
            order by code_occur_time, er_date, er_index
        ) as prev_exposure_handle
    from prism_common.er_dose_raw_parsed
    where code like 'DW-%'
      and exposure_handle is not null
)
select
    eq_name,
    code,
    code_occur_time,
    er_date,
    er_index,
    prev_exposure_handle,
    exposure_handle,
    exposure_handle - prev_exposure_handle as exposure_handle_diff
from exposure_flow
where prev_exposure_handle is not null
  and exposure_handle - prev_exposure_handle > 1000
order by eq_name, code_occur_time, er_date, er_index;
