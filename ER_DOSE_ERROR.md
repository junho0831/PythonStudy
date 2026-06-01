# ER Dose Error Parsing

`er_dose`는 `mbeat.er_data_raw`의 Dose Error RAW 로그를 파싱해서 `mbeat.er_dose_error_parsed`에 적재하는 배치다.

## 데이터 흐름

```text
mbeat.er_data_raw
  -> er_dose batch
  -> mbeat.er_dose_error_parsed
```

Root cause는 이 배치와 별도 흐름이다.

```text
mbeat.er_data_raw_euv
  -> contents root cause 파싱
  -> mbeat.er_dose_error_root_cause
```

`mbeat.er_dose_error_parsed`와 `mbeat.er_dose_error_root_cause`는 서로 조인하거나 매칭하지 않는다.

## 테이블 역할

- `mbeat.er_data_raw`: Dose Error 파싱 대상 RAW. `er_date`, `er_index`가 있다.
- `mbeat.er_dose_error_parsed`: `er_data_raw` 파싱 결과. 현재 배치가 적재하는 대상이다.
- `mbeat.er_data_raw_euv`: Root cause source description 후보 RAW. `contents`에 `dose error detected in file`, `root cause`, `exposure id`, 각종 EUV 지표가 들어온다. `er_date`, `er_index`가 없다.
- `mbeat.er_dose_error_root_cause`: FE 조회용 root cause 결과 테이블. `er_data_raw_euv.contents`를 파싱한 구조화 컬럼과 원문을 저장하며, `er_dose_error_parsed`와 무관하다.

DDL:

- [create_er_dose_error_parsed.sql](er_dose/sql/create_er_dose_error_parsed.sql)
- [alter_er_dose_error_parsed_preserve_source_fields.sql](er_dose/sql/alter_er_dose_error_parsed_preserve_source_fields.sql)
- [create_er_dose_error_root_cause.sql](er_dose/sql/create_er_dose_error_root_cause.sql)
- [alter_er_dose_error_root_cause_add_euv_metrics.sql](er_dose/sql/alter_er_dose_error_root_cause_add_euv_metrics.sql)
- [create_er_data_raw_euv.sql](er_dose/sql/create_er_data_raw_euv.sql)

기존 운영 `mbeat.er_dose_error_parsed`가 repo DDL과 다르게 만들어진 경우를 대비해
`alter_er_dose_error_parsed_preserve_source_fields.sql`는 아래 원천 기본 컬럼도 `add column if not exists`로 보강한다.

- `er_date`
- `er_index`
- `er_line`
- `eq_name`
- `code`
- `code_occur_time`
- `belong`
- `type`
- `title`
- `contents`

## ERD

```mermaid
erDiagram
    ER_DATA_RAW ||--o{ ER_DOSE_ERROR_PARSED : "parse"
    ER_DATA_RAW_EUV ||--o{ ER_DOSE_ERROR_ROOT_CAUSE : "source description"

    ER_DATA_RAW {
        int4 er_date
        int4 er_index
        varchar er_line
        varchar eq_name
        varchar code
        timestamp code_occur_time
        varchar belong
        varchar type
        varchar title
        varchar contents
    }

    ER_DATA_RAW_EUV {
        varchar(20) er_line
        varchar(20) eq_name
        varchar(10) er_type
        varchar(20) code
        timestamp code_occur_time
        varchar(12) belong
        varchar(8) type
        varchar title
        varchar contents
        varchar(20) reason_code
        varchar task
        varchar compile_script
    }

    ER_DOSE_ERROR_PARSED {
        int4 er_date
        int4 er_index
        varchar(20) er_line
        varchar(20) eq_name
        varchar(20) code
        timestamp(6) code_occur_time PK
        varchar(12) belong
        varchar(8) type
        varchar title
        varchar contents
        bigint exposure_handle
        bigint source_exposure_id
        bigint action_handle
        integer wafer_seq
        integer shot_seq
        integer field_seq
        numeric(12,7) dose_error
        numeric(12,7) dose_warn_level
        numeric(12,7) de_err
        numeric(12,7) de_warn_lvl
        bigint eset
        integer freq
        integer n_slit
        boolean mb_enabled
        text function_name
        text result_type
        timestamp created_at
    }

    ER_DOSE_ERROR_ROOT_CAUSE {
        varchar(20) er_line
        varchar(20) eq_name
        varchar(10) er_type
        varchar(20) code
        timestamp(6) code_occur_time PARTITION
        varchar(12) belong
        varchar(8) type
        varchar title
        varchar contents
        varchar(20) reason_code
        varchar task
        varchar compile_script
        bigint source_exposure_id
        timestamp(6) source_code_occur_time
        numeric(12,7) dose_error
        text source_file_name
        text root_cause_code
        text root_cause_message
        numeric(12,7) exposure_length
        numeric(12,7) duty_cycle
        numeric(12,7) min_dose_error
        numeric(12,7) max_dose_error
        numeric(12,7) on_drop_euv_energy
        numeric(12,7) on_drop_pp_energy
        numeric(12,7) on_drop_mp_energy
        numeric(12,7) on_drop_pp_dlgc1
        numeric(12,7) on_drop_mp_dlgc1
        numeric(12,7) bi_cell_y_3sigma
        numeric(12,7) fdsc_y_error
        numeric(12,7) fdsc_y_3sigma
        numeric(12,7) max_cross_interval
        numeric(12,7) xint_3sigma
        numeric(12,7) euv_3sigma
        integer pulses_euv_lt_0_6dt_tot
        integer fed_pulses
        numeric(12,7) l2dx_maxce
        numeric(12,7) l2dy_maxce
        numeric(12,7) sensitivity_at_l2dx_maxce
        numeric(12,7) sensitivity_at_l2dy_maxce
        numeric(12,7) dose_margin
        numeric(12,7) l2dx_qc_etdc_3sigma
        numeric(12,7) l2dx_qc_etdc_median
        numeric(12,7) l2dy_qc_etdc_3sigma
        numeric(12,7) l2dy_qc_etdc_median
        numeric(12,7) rbdy_peak_frequency_hf
        numeric(12,7) rbdy_peak_frequency_lf
        numeric(12,7) rbdy_peak_frequency_mf
        numeric(12,7) rbdy_peak_power_hf
        numeric(12,7) rbdy_qc_etdc_3sigma
        numeric(12,7) rbdy_total_power_lf
        numeric(12,7) rbdy_total_power_mf
        text software_version
    }
```

`varchar` 길이와 `numeric` 정밀도는 이 repo의 DDL 기준이다. `mbeat.er_data_raw`는 기존 원천 테이블이므로 배치가 읽는 컬럼만 표시한다.
`mbeat.er_dose_error_parsed`와 `mbeat.er_dose_error_root_cause`는 모두 `code_occur_time` 기준 range partition을 사용한다.

## 배치 동작

`ERDoseBatch.run()`은 다음만 수행한다.

1. 기간에 해당하는 `er_dose_error_parsed` 일별 파티션 생성
2. `mbeat.er_data_raw`에서 Dose Error 후보를 server-side cursor로 chunk 조회
3. RAW contents 파싱
4. `mbeat.er_dose_error_parsed`에 `ON CONFLICT DO NOTHING` bulk insert

bulk insert는 대용량 성능을 우선해 `RETURNING` 결과를 받지 않는다. 따라서 실행 summary의 `inserted`는 실제 신규 insert 건수가 아니라 insert 시도 row 수이며, conflict skip 건수는 별도 집계하지 않는다.

배치는 `mbeat.er_data_raw_euv`와 `mbeat.er_dose_error_root_cause`를 조회하거나 적재하지 않는다.

중복 기준은 운영 테이블 PK인 `code_occur_time`이다. 하루 수천만 건 단위 입력을 전제로 하므로 일반 운영에서는 delete 후 재적재나 `DO UPDATE`를 사용하지 않는다.

같은 `code_occur_time`에 여러 원천 row가 존재하면 현재 PK 제약상 먼저 적재된 row만 남고 나머지는 conflict skip된다.

## Airflow 운영

- `er_dose_near_realtime`: 10분마다 실행하고 `data_interval_start/end - 10분` 구간 처리
- `er_dose_backfill_hourly`: 매시간 최근 1일을 10분 chunk로 재처리
- `er_dose_backfill_daily`: 매일 03시 최근 3일을 10분 chunk로 재처리

세 DAG 모두 `max_active_runs=1`로 둔다. 배치 실행 시간이 스케줄 주기를 넘어도 담당 조회 구간은 Airflow `data_interval` 기준으로 고정된다.

## Root Cause 파싱 대상

`mbeat.er_data_raw_euv.contents`는 아래와 같은 줄 단위 포맷을 파싱 대상으로 한다.

```text
dose error detected in file: adecetdcdata_fdd_lc_eei_scanner_dose_error_event_20260504_180529_3502+0900.zip.
root cause : plasma oscillations
exposure id : 25415
time : 2026-05-04t18:05:29.297624+09:00
min. dose error : -2.02 [perc]
max. dose error : 0.74 [perc]
...
software version : 2.0 [nxe3400 mv 250w]
```

파서는 `source_file_name`, `source_exposure_id`, `source_code_occur_time`, `root_cause_message`, `root_cause_code`를 추출한다.
`dose_error`는 `min_dose_error`와 `max_dose_error` 중 절대값이 큰 대표값으로 저장한다. 예시에서는 `-2.02`가 저장된다.

측정값은 조회/필터링을 위해 `exposure_length`, `duty_cycle`, `on_drop_*`, `fdsc_*`, `l2d*`, `rbdy_*`, `software_version` 등 개별 컬럼에 저장하고, 원문은 `contents`에 보존한다.

## 파싱 대상

`mbeat.er_data_raw`에서 아래 조건에 해당하는 로그를 조회한다.

- `code ilike 'dw-%'`
- 또는 `contents`에 `dose evaluation`, `de_err`, `dwdc_eval_determine_dose_performance_result` 포함

주요 파싱 필드:

- 원천 보존: `er_date`, `er_index`, `er_line`, `eq_name`, `code`, `code_occur_time`, `belong`, `type`, `title`, `contents`
- exposure/action: `exposure_handle`, `source_exposure_id`, `action_handle`
- sequence: `wafer_seq`, `shot_seq`, `field_seq`
- dose: `dose_error`, `dose_warn_level`, `de_err`, `de_warn_lvl`
- dose context: `eset`, `freq`, `n_slit`, `mb_enabled`, `function_name`, `result_type`

필드가 없으면 nullable 컬럼은 `NULL`로 저장한다.

## 실행

DB 접속은 `--dsn`, `ER_DOSE_DB_DSN`, `DATABASE_URL` 순서로 사용한다.

```bash
python -m er_dose.run_er_dose_batch \
  --start-time 2026-04-13T00:00:00 \
  --end-time 2026-04-14T00:00:00 \
  --limit 1000
```
