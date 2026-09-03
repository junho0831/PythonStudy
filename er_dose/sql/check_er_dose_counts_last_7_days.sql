-- [RAW] 날짜별 원천 vs 타깃 건수 비교 (오늘 포함 최근 7일)
with dates as (
    select current_date - day_offset as target_date
    from generate_series(0, 6) as days(day_offset)
),
source_counts as (
    select
        r.code_occur_time::date as target_date,
        count(*) as source_count
    from mbeat.er_data_raw r
    where r.code_occur_time >= (current_date - 6)::timestamp
      and r.code_occur_time < (current_date + 1)::timestamp
      and r.code in (
          'DW-3411',
          'DW-3425',
          'DW-343A',
          'DW-343B',
          'LO-0050',
          'LO-0051',
          'LO-0052',
          'LO-0061',
          'LO-8166',
          'LO-8167',
          'KE-9103',
          'KE-9104'
      )
      and r.eq_name in (
          select eqp.eqp_id
          from prism_dev.photo_eqp_info eqp
          where eqp.use_yn = 'Y'
            and eqp.eqp_model_name like 'NXE%'
      )
    group by r.code_occur_time::date
),
target_counts as (
    select
        p.code_occur_time::date as target_date,
        count(*) as target_count
    from prism_common.er_dose_raw_parsed p
    where p.code_occur_time >= (current_date - 6)::timestamp
      and p.code_occur_time < (current_date + 1)::timestamp
      and p.code in (
          'DW-3411',
          'DW-3425',
          'DW-343A',
          'DW-343B',
          'LO-0050',
          'LO-0051',
          'LO-0052',
          'LO-0061',
          'LO-8166',
          'LO-8167',
          'KE-9103',
          'KE-9104'
      )
      and p.eq_name in (
          select eqp.eqp_id
          from prism_dev.photo_eqp_info eqp
          where eqp.use_yn = 'Y'
            and eqp.eqp_model_name like 'NXE%'
      )
    group by p.code_occur_time::date
)
select
    d.target_date,
    coalesce(s.source_count, 0) as source_count,
    coalesce(t.target_count, 0) as target_count,
    coalesce(s.source_count, 0) - coalesce(t.target_count, 0) as count_diff,
    case
        when coalesce(s.source_count, 0) = coalesce(t.target_count, 0) then 'Y'
        else 'N'
    end as matched
from dates d
left join source_counts s on s.target_date = d.target_date
left join target_counts t on t.target_date = d.target_date
order by d.target_date;


-- [EUV] 날짜별 원천 vs 타깃 건수 비교 (오늘 포함 최근 7일)
with dates as (
    select current_date - day_offset as target_date
    from generate_series(0, 6) as days(day_offset)
),
source_counts as (
    select
        r.code_occur_time::date as target_date,
        count(*) as source_count
    from mbeat.er_data_raw_euv r
    where r.code_occur_time >= (current_date - 6)::timestamp
      and r.code_occur_time < (current_date + 1)::timestamp
      and lower(r.contents) like '%dose error detected in file:%'
      and lower(r.contents) like '%root cause%'
      and r.eq_name in (
          select eqp.eqp_id
          from prism_dev.photo_eqp_info eqp
          where eqp.use_yn = 'Y'
            and eqp.eqp_model_name like 'NXE%'
      )
    group by r.code_occur_time::date
),
target_counts as (
    select
        p.code_occur_time::date as target_date,
        count(*) as target_count
    from prism_common.er_dose_euv_parsed p
    where p.code_occur_time >= (current_date - 6)::timestamp
      and p.code_occur_time < (current_date + 1)::timestamp
      and p.eq_name in (
          select eqp.eqp_id
          from prism_dev.photo_eqp_info eqp
          where eqp.use_yn = 'Y'
            and eqp.eqp_model_name like 'NXE%'
      )
    group by p.code_occur_time::date
)
select
    d.target_date,
    coalesce(s.source_count, 0) as source_count,
    coalesce(t.target_count, 0) as target_count,
    coalesce(s.source_count, 0) - coalesce(t.target_count, 0) as count_diff,
    case
        when coalesce(s.source_count, 0) = coalesce(t.target_count, 0) then 'Y'
        else 'N'
    end as matched
from dates d
left join source_counts s on s.target_date = d.target_date
left join target_counts t on t.target_date = d.target_date
order by d.target_date;
