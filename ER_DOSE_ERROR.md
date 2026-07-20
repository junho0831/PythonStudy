# ER Dose Error Parsing

`er_dose`는 `mbeat.er_data_raw`의 Dose Error RAW 로그를 파싱해서 `prism_common.er_dose_raw_parsed`에 적재하는 배치다.

## 데이터 흐름

```text
mbeat.er_data_raw
  -> er_dose batch
  -> prism_common.er_dose_raw_parsed
```

Root cause는 이 배치와 별도 흐름이다.

```text
mbeat.er_data_raw_euv
  -> contents root cause 파싱
  -> prism_common.er_dose_euv_parsed
```

`prism_common.er_dose_raw_parsed`와 `prism_common.er_dose_euv_parsed`는 서로 조인하거나 매칭하지 않는다.

## 테이블 역할

- `mbeat.er_data_raw`: Dose Error 파싱 대상 RAW. `er_date`, `er_index`가 있다.
- `prism_common.er_dose_raw_parsed`: `er_data_raw` 파싱 결과. 현재 배치가 적재하는 대상이다.
- `mbeat.er_data_raw_euv`: Root cause source description 후보 RAW. `contents`에 `dose error detected in file`, `root cause`, `exposure id`, 각종 EUV 지표가 들어온다. `er_date`, `er_index`가 없다.
- `prism_common.er_dose_euv_parsed`: FE 조회용 root cause 결과 테이블. `er_data_raw_euv.contents`를 파싱한 구조화 컬럼과 원문을 저장하며, `er_dose_raw_parsed`와 무관하다.

DDL:

- [Parsed 테이블 생성](er_dose/sql/create_er_dose_raw_parsed.sql)
- [RAW Parsed 스키마 마이그레이션](er_dose/sql/migrate_er_dose_raw_parsed_schema.sql)
- [EUV Parsed 테이블 생성](er_dose/sql/create_er_dose_euv_parsed.sql)
- [EUV Parsed 스키마 마이그레이션](er_dose/sql/migrate_er_dose_euv_parsed_schema.sql)
- [EUV Parsed 컬럼 rename 마이그레이션](er_dose/sql/rename_er_dose_euv_parsed_columns.sql)
- [RAW EUV 테이블 생성](er_dose/sql/create_er_data_raw_euv.sql)

## ERD

```mermaid
erDiagram
    ER_DATA_RAW ||--o{ ER_DOSE_RAW_PARSED : "parse"
    ER_DATA_RAW_EUV ||--o{ ER_DOSE_EUV_PARSED : "source description"

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
        varchar er_line
        varchar eq_name
        varchar er_type
        varchar code
        timestamp code_occur_time
        varchar belong
        varchar type
        varchar title
        varchar contents
        varchar reason_code
        varchar task
        varchar compile_script
    }

    ER_DOSE_RAW_PARSED {
        varchar eq_name
        varchar code
        timestamp code_occur_time PK
        varchar title
        varchar contents
        bigint exposure_handle
        bigint action_handle
        varchar lot_id
        varchar lot_name
        integer lot_seq
        integer wafer_seq
        numeric de_err
        integer n_slit
        timestamp created_at
    }

    ER_DOSE_EUV_PARSED {
        varchar eq_name
        varchar er_type
        varchar code
        timestamp code_occur_time PK
        varchar title
        varchar contents
        varchar reason_code
        varchar task
        varchar compile_script
        bigint exposure_id
        timestamp time
        text dose_error_detected_in_file
        text root_cause_code
        text root_cause
        numeric exposure_length
        numeric duty_cycle
        numeric min_dose_error
        numeric max_dose_error
        numeric on_drop_euv_energy
        numeric on_drop_pp_energy
        numeric on_drop_mp_energy
        numeric on_drop_pp_dlgc_1
        numeric on_drop_mp_dlgc_1
        numeric bi_cell_y_3sigma
        numeric fdsc_y_error
        numeric fdsc_y_3sigma
        numeric max_cross_interval
        numeric xint_3sigma
        numeric euv_3sigma
        integer pulses_euv_0_6dt_tot
        integer fed_pulses
        numeric l2dx_maxce
        numeric l2dy_maxce
        numeric sensitivity_at_l2dx_maxce
        numeric sensitivity_at_l2dy_maxce
        numeric dose_margin
        numeric l2dx_qc_etdc_3sigma
        numeric l2dx_qc_etdc_median
        numeric l2dy_qc_etdc_3sigma
        numeric l2dy_qc_etdc_median
        numeric rbdy_peak_frequency_hf
        numeric rbdy_peak_frequency_lf
        numeric rbdy_peak_frequency_mf
        numeric rbdy_peak_power_hf
        numeric rbdy_qc_etdc_3sigma
        numeric rbdy_total_power_lf
        numeric rbdy_total_power_mf
        text software_version
    }
```

Mermaid ERD는 렌더링 호환성을 위해 타입 표기를 단순화했다. 실제 `varchar` 길이와 `numeric` 정밀도는 이 repo의 DDL 기준이다. `mbeat.er_data_raw`는 기존 원천 테이블이므로 배치가 읽는 컬럼만 표시한다.
`prism_common.er_dose_raw_parsed`와 `prism_common.er_dose_euv_parsed`는 실제 DB에서는 모두 `code_occur_time` 기준 range partition을 사용한다.

## 배치 동작

`ERDoseProcessor.run()`은 명시적으로 날짜 또는 시간 범위를 받은 경우에 다음을 수행한다.

1. 기간에 해당하는 `er_dose_raw_parsed` 일별 파티션을 대상으로 처리
2. `mbeat.er_data_raw`에서 Dose Error 후보를 `chunk` 단위로 조회
3. 각 `chunk`의 RAW contents 파싱
   - 파싱 중 `lot_seq`나 `wafer_seq`가 없을 경우, 동일 `eq_name`에서 이전에 파싱된 가장 최근 값을 사용한다. 이는 chunk의 경계를 넘어 유지된다.
4. 각 `chunk`를 `prism_common.er_dose_raw_parsed`에 `COPY` append insert

환경변수 기반 기본 실행에서 target date와 `ER_DOSE_START_TIME`, `ER_DOSE_END_TIME`가 모두 없으면 raw/euv 배치는 최근 4일 lookback 모드로 동작한다.

1. 실행일 기준 `오늘 포함 최근 4일`을 날짜 오름차순으로 순회
2. 각 날짜에 대해 원천 raw 건수와 타겟 parsed 건수를 비교
3. 건수가 같으면 해당 날짜는 스킵
4. 건수가 다르면 해당 날짜의 parsed 파티션을 `TRUNCATE`
5. 원천 raw를 해당 날짜 처음부터 다시 조회해 chunk 단위로 파싱 후 insert

`ER_DOSE_EUV_TARGET_DATE`가 있으면 해당 날짜 1일만 같은 방식으로 count 비교 후 필요 시 재적재한다. `ER_DOSE_START_TIME`/`ER_DOSE_END_TIME`으로 시간 범위를 직접 지정하면 count 비교 없이 해당 범위를 처리한다.

`ER_DOSE_EUV` 배치는 `mbeat.er_data_raw_euv`를 기간 조건으로 `chunk` 조회하고, root cause 형식의 `contents`만 파싱해 `prism_common.er_dose_euv_parsed`에 적재한다. EUV source count도 parsed count와 맞추기 위해 `contents`에 `dose error detected in file:`과 `root cause`가 있는 row만 계산한다. EUV parsed 결과에는 `eq_name`, `er_type`, `code`, `code_occur_time`, `title`, `contents`, `reason_code`, `task`, `compile_script`와 root cause 파싱 컬럼만 저장한다.
RAW와 EUV 모두 대용량 처리를 위해 전체 결과를 한 번에 메모리로 올리지 않고 `read chunk -> parse -> insert` 방식으로 반복 처리한다.
또한, 데이터베이스 드라이버 단의 메모리 팽창을 방지하기 위해 SQLAlchemy 서버사이드 커서(`stream_results=True`, `max_row_buffer=chunk_size`)를 활성화하여 스트리밍 조회를 수행한다. 다만 실제 메모리 사용량은 `chunk` 크기와 raw `contents` 크기에 영향을 받기 때문에 운영 환경에서 조정이 필요할 수 있다.
RAW와 EUV 모두 조회 SQL에서 `prism_dev.photo_eqp_info`의 `use_yn = 'Y'`이고 `eqp_model_name like 'NXE%'`인 `eqp_id`를 서브쿼리로 조회해 `eq_name` 필터로 사용한다. RAW의 이전 `lot_seq`, `wafer_seq` 상태 조회에도 같은 조건을 적용한다.

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

파서는 `dose_error_detected_in_file`, `exposure_id`, `time`, `root_cause`를 원문 라벨 기반 snake_case 컬럼으로 저장한다.
`root_cause_code`는 `root_cause`의 snake_case 파생값으로 저장한다.
컬럼명 정규화는 소문자 기준으로 공백, `.`, `-`, `<`, `=`를 모두 `_`로 치환하는 규칙을 따른다.

측정값은 조회/필터링을 위해 `exposure_length`, `duty_cycle`, `on_drop_*`, `fdsc_*`, `l2d*`, `rbdy_*`, `software_version` 등 개별 컬럼에 저장하고, 원문은 `contents`에 보존한다.

## 파싱 대상

`mbeat.er_data_raw`에서 아래 조건에 해당하는 로그를 조회한다.

- `eq_name` 값이 `prism_dev.photo_eqp_info`에서 `use_yn = 'Y'`이고 `eqp_model_name like 'NXE%'`인 `eqp_id` 목록에 포함
- `code` 값이 아래 목록에 원본 형식 그대로 포함
  - `DW-3411`
  - `DW-3425`
  - `DW-343A`
  - `DW-343B`
  - `LO-0050`
  - `LO-0061`
  - `LO-8166`
  - `LO-8167`
  - `KE-9103`
  - `KE-9104`

즉 `code`는 하이픈 제거, 대소문자 변환 같은 정규화 없이 DB 원본 값 그대로 비교한다.

RAW parsed 저장 필드:

- 원천 기반 컬럼: `eq_name`, `code`, `code_occur_time`, `title`, `contents`
- 파싱 컬럼: `exposure_handle`, `action_handle`, `lot_id`, `lot_name`, `lot_seq`, `wafer_seq`, `de_err`, `n_slit`

필드가 없으면 nullable 컬럼은 `NULL`로 저장한다.

## 실행

현재 RAW 배치 parser 이름은 `ER_DOSE_RAW` 이다.
RAW 날짜 변수는 `ER_DOSE_RAW_TARGET_DATE` 를 사용한다.
EUV 날짜 변수는 `ER_DOSE_EUV_TARGET_DATE` 를 사용한다.

DB 접속은 `--dsn`, 프로젝트 루트 `er_dose.properties`, `ER_DOSE_DB_DSN`, `DATABASE_URL` 순서로 사용한다.
기본 `chunk` 크기는 `ER_DOSE_RAW` 및 `ER_DOSE_EUV` 배치 모두 `30000`이며 `--chunk-size`로 조정할 수 있다.
RAW 기본 실행은 최근 4일 lookback 모드이며, `--lookback-days` 또는 환경변수 기반 실행의 `ER_DOSE_LOOKBACK_DAYS`로 일수를 바꿀 수 있다.

```bash
python -m er_dose.run_er_dose_batch \
  --parser ER_DOSE_RAW \
  --chunk-size 30000 \
  --dsn 'postgresql://user:password@host:5432/dbname'
```

```bash
python -m er_dose.run_er_dose_batch \
  --date 2026-04-13 \
  --parser ER_DOSE_RAW \
  --chunk-size 30000 \
  --dsn 'postgresql://user:password@host:5432/dbname'
```

```bash
python -m er_dose.run_er_dose_batch \
  --date 2026-04-13 \
  --parser ER_DOSE_EUV \
  --chunk-size 30000 \
  --dsn 'postgresql://user:password@host:5432/dbname'
```

기존 시간 범위 직접 지정 방식도 계속 지원한다.

```bash
python -m er_dose.run_er_dose_batch \
  --start-time 2026-04-13T00:00:00 \
  --end-time 2026-04-14T00:00:00 \
  --chunk-size 30000 \
  --dsn 'postgresql://user:password@host:5432/dbname'
```
