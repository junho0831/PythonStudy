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
- `mbeat.er_data_raw_euv`: Root cause source description 후보 RAW. `contents`에 `dose error detected in file`, root cause 라벨, exposure ID 라벨, 각종 EUV 지표가 들어온다. `er_date`, `er_index`가 없다.
- `prism_common.er_dose_euv_parsed`: FE 조회용 root cause 결과 테이블. `er_data_raw_euv.contents`를 파싱한 구조화 컬럼과 원문을 저장하며, `er_dose_raw_parsed`와 무관하다.

DDL:

- [Parsed 테이블 생성](er_dose/sql/create_er_dose_raw_parsed.sql)
- [EUV Parsed 테이블 생성](er_dose/sql/create_er_dose_euv_parsed.sql)
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
        int4 er_date
        int4 er_index
        varchar er_line
        varchar eq_name
        varchar code
        timestamp code_occur_time PK
        varchar belong
        varchar type
        varchar title
        varchar contents
        bigint exposure_handle
        bigint action_handle
        integer wafer_id
        numeric de_err
        integer n_slit
        timestamp created_at
    }

    ER_DOSE_EUV_PARSED {
        varchar er_line
        varchar eq_name
        varchar er_type
        varchar code
        timestamp code_occur_time PK
        varchar belong
        varchar type
        varchar title
        varchar contents
        varchar reason_code
        varchar task
        varchar compile_script
        bigint source_exposure_id
        timestamp source_code_occur_time
        numeric dose_error
        text source_file_name
        text root_cause_code
        text root_cause_message
        numeric exposure_length
        numeric duty_cycle
        numeric min_dose_error
        numeric max_dose_error
        numeric on_drop_euv_energy
        numeric on_drop_pp_energy
        numeric on_drop_mp_energy
        numeric on_drop_pp_dlgc1
        numeric on_drop_mp_dlgc1
        numeric bi_cell_y_3sigma
        numeric fdsc_y_error
        numeric fdsc_y_3sigma
        numeric max_cross_interval
        numeric xint_3sigma
        numeric euv_3sigma
        integer pulses_euv_lt_0_6dt_tot
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

`ERDoseProcessor.run()`은 다음만 수행한다.

1. 기간에 해당하는 `er_dose_raw_parsed` 일별 파티션 생성
2. `mbeat.er_data_raw`에서 Dose Error 후보를 `chunk` 단위로 조회
3. 각 `chunk`의 RAW contents 파싱
   - 파싱 중 `wafer_id`나 `wafer_seq`가 없을 경우, 동일 `eq_name`에서 이전에 파싱된 가장 최근 값을 사용한다. 이는 chunk의 경계를 넘어 유지된다.
4. 각 `chunk`를 `prism_common.er_dose_raw_parsed`에 `COPY` append insert

`ER_DOSE_EUV` 배치는 `mbeat.er_data_raw_euv`를 기간 조건으로 `chunk` 조회하고, root cause 형식의 `contents`만 파싱해 `prism_common.er_dose_euv_parsed`에 적재한다.
RAW와 EUV 모두 대용량 처리를 위해 전체 결과를 한 번에 메모리로 올리지 않고 `read chunk -> parse -> insert` 방식으로 반복 처리한다.

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

DB 문자열에 `\n`이 실제 줄바꿈이 아닌 두 문자로 저장된 경우에도 먼저 줄바꿈으로 정규화한다. `\t` 같은 다른 이스케이프 문자열은 별도로 변환하지 않는다.

### 파싱 시나리오

1. 빈 `contents`는 파싱하지 않는다.
2. `\n` 문자열을 실제 줄바꿈으로 정규화한다.
3. 아래 두 조건을 모두 만족하는 행만 root cause 대상으로 인정한다.
   - `Dose error detected in file:` 라벨이 있다. 대소문자는 구분하지 않는다.
   - `Root cause:` 또는 `Root clause:` 라벨이 있다. `Root clause`는 실제 원천 데이터의 라벨 변형을 지원하기 위한 명시적 alias다.
4. 각 줄의 라벨을 기준으로 값을 추출한다. 라벨 앞뒤 공백과 대소문자는 무시한다.
5. 숫자 필드는 `Decimal` 또는 정수로 변환하고 `[s]`, `[perc]` 같은 단위 문자열은 저장하지 않는다.
6. 개별 필드가 없거나 변환할 수 없으면 그 필드만 `NULL`로 둔다. 다른 필드의 파싱과 행 적재는 계속한다.
7. 파싱 결과에 원본 `er_line`, `eq_name`, `code`, `code_occur_time`, `contents` 등을 합쳐 `code_occur_time` 날짜의 `prism_common.er_dose_euv_parsed` 파티션에 적재한다.

### 식별 필드 매핑

| 원문 라벨 | 저장 컬럼 | 처리 규칙 |
| --- | --- | --- |
| `Dose error detected in file` | `source_file_name` | 문장 마지막 구분용 `.`은 제거하고 실제 파일 확장자 `.zip`은 유지한다. |
| `Root cause`, `Root clause` | `root_cause_message` | 원문 메시지를 저장한다. |
| `Root cause`, `Root clause` | `root_cause_code` | 메시지를 소문자 snake case로 변환한다. 예: `Low dose margin` → `low_dose_margin`. |
| `Exposure ID`, `Exposesue I D` | `source_exposure_id` | 정수로 변환한다. `Exposesue I D`는 확인된 원천 라벨 오타에 대한 명시적 alias다. |
| `Time` | `source_code_occur_time` | ISO 8601 timestamp로 변환하며 timezone offset을 유지한다. |
| `Min. dose error` | `min_dose_error` | 단위를 제외한 숫자를 저장한다. |
| `Max. dose error` | `max_dose_error` | 단위를 제외한 숫자를 저장한다. |

`dose_error`는 `min_dose_error`와 `max_dose_error` 중 절대값이 큰 대표값으로 저장한다. 절대값이 같으면 `min_dose_error`가 선택된다. 위 예시에서는 `-2.02`가 저장된다.

### 측정값 처리

측정값은 조회와 필터링을 위해 `exposure_length`, `duty_cycle`, `on_drop_*`, `fdsc_*`, `l2d*`, `rbdy_*` 등 개별 `numeric` 컬럼에 저장한다. 예를 들어 `0.2338 [s]`는 `0.2338`, `99.62 [perc]`는 `99.62`로 저장한다.

`pulses_euv_lt_0_6dt_tot`와 `fed_pulses`는 정수 컬럼이다. 원문이 `3` 또는 `3.0`이면 정수 `3`으로 저장하지만, `3.5`처럼 정수로 표현할 수 없는 값은 `NULL`로 처리한다. `software_version`은 숫자로 변환하지 않고 라벨 뒤의 문자열 전체를 저장한다.

파싱 여부와 관계없이 원천 데이터 자체는 `mbeat.er_data_raw_euv`에 남는다. 파싱 대상이 된 행은 원문도 `prism_common.er_dose_euv_parsed.contents`에 그대로 보존한다.

## 파싱 대상

`mbeat.er_data_raw`에서 아래 조건에 해당하는 로그를 조회한다.

- `code` 값이 아래 목록에 원본 형식 그대로 포함
  - `DW-3411`
  - `DW-3425`
  - `DW-343A`
  - `DW-343B`
  - `LO-0061`
  - `LO-8166`
  - `LO-8167`
  - `KE-9103`
  - `KE-9104`

즉 `code`는 하이픈 제거, 대소문자 변환 같은 정규화 없이 DB 원본 값 그대로 비교한다.

주요 파싱 필드:

- 원천 보존: `er_date`, `er_index`, `er_line`, `eq_name`, `code`, `code_occur_time`, `belong`, `type`, `title`, `contents`
- 추가 컬럼: `exposure_handle`, `action_handle`, `wafer_id`, `wafer_seq`, `de_err`, `n_slit`

필드가 없으면 nullable 컬럼은 `NULL`로 저장한다.

## 실행

현재 RAW 배치 parser 이름은 `ER_DOSE_RAW` 이다.
RAW 날짜 변수는 `ER_DOSE_RAW_TARGET_DATE` 를 사용한다.
EUV 날짜 변수는 `ER_DOSE_EUV_TARGET_DATE` 를 사용한다.

DB 접속은 `--dsn`, 프로젝트 루트 `er_dose.properties`, `ER_DOSE_DB_DSN`, `DATABASE_URL` 순서로 사용한다.
기본 `chunk` 크기는 `10000`이며 `--chunk-size`로 조정할 수 있다.

```bash
python -m er_dose.run_er_dose_batch \
  --date 2026-04-13 \
  --parser ER_DOSE_RAW \
  --chunk-size 10000 \
  --dsn 'postgresql://user:password@host:5432/dbname'
```

```bash
python -m er_dose.run_er_dose_batch \
  --date 2026-04-13 \
  --parser ER_DOSE_EUV \
  --chunk-size 10000 \
  --dsn 'postgresql://user:password@host:5432/dbname'
```

기존 시간 범위 직접 지정 방식도 계속 지원한다.

```bash
python -m er_dose.run_er_dose_batch \
  --start-time 2026-04-13T00:00:00 \
  --end-time 2026-04-14T00:00:00 \
  --chunk-size 10000 \
  --dsn 'postgresql://user:password@host:5432/dbname'
```
