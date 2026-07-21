from __future__ import annotations


def active_nxe_eq_filter(column_name: str) -> str:
    return f"""{column_name} in (
                  select eqp.eqp_id
                  from prism_dev.photo_eqp_info eqp
                  where eqp.use_yn = 'Y'
                    and eqp.eqp_model_name like 'NXE%'
              )"""
