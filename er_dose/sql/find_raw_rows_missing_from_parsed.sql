select
    r.er_date,
    r.er_index,
    r.er_line,
    r.eq_name,
    r.code,
    r.code_occur_time,
    r.belong,
    r."type",
    r.title,
    substring(r.contents from 'exposure_handle\s*[:=]\s*([0-9]+)') as exposure_handle_raw,
    r.contents
from mbeat.er_data_raw r
left join prism_common.er_dose_raw_parsed p
    on p.er_date = r.er_date
   and p.er_index = r.er_index
   and p.er_line = r.er_line
   and p.eq_name = r.eq_name
   and p.code = r.code
   and p.code_occur_time = r.code_occur_time
where r.code_occur_time >= :start_time
  and r.code_occur_time < :end_time
  and r.code in (
      'DW-3411', 'DW-3425', 'DW-343A', 'DW-343B',
      'LO-0061', 'LO-8166', 'LO-8167', 'KE-9103', 'KE-9104'
  )
  and p.code_occur_time is null
order by r.eq_name, r.code_occur_time, r.er_date, r.er_index;
