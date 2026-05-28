# ER Dose Error Parsing

## 개요

`er_dose`는 PostgreSQL `mbeat.er_data_raw`에 저장된 ER RAW 로그를 읽어 Dose Error 관련 필드를 파싱하고, `mbeat.er_dose_error_parsed`에 정규화해 적재하는 모듈이다.

기존 `ftp_batch`와 분리되어 있으며, 1차 구현 범위는 `dw-xxxx` dose evaluation warning 로그 파싱이다.

## 테이블

DDL은 [create_er_dose_error_parsed.sql](/Users/parkjunho/PycharmProjects/PythonStudy/er_dose/sql/create_er_dose_error_parsed.sql)에 있다.

핵심 정책:

- RAW 원문은 `mbeat.er_data_raw.contents`에 유지한다.
- parsed 테이블에도 `raw_contents`를 저장한다.
- `raw_id`는 `er_data_raw.id`를 저장하며 `UNIQUE`로 관리한다.
- 재실행 시 이미 적재된 `raw_id`는 조회 단계에서 제외하고, insert 단계에서도 `ON CONFLICT (raw_id) DO NOTHING`으로 보호한다.

## 실행

DB 접속은 `--dsn`, `ER_DOSE_DB_DSN`, `DATABASE_URL` 순서로 사용한다.

```bash
python -m er_dose.run_er_dose_batch \
  --start-time 2026-04-13T00:00:00 \
  --end-time 2026-04-14T00:00:00 \
  --limit 1000
```

## 파싱 규칙

대상은 `dw-xxxx` 코드이며, contents 안에 `dose evaluation`, `de_err`, `dwdc_eval_determine_dose_performance_result` 중 하나가 있는 로그다.

추출 필드:

- `exposure_handle`
- `action_handle`
- `dose_error`
- `dose_warn_level`
- `de_err`
- `de_warn_lvl`
- `eset`
- `freq`
- `n_slit`
- `mb_enabled`
- `function_name`
- `result_type`

필드가 없으면 nullable 컬럼은 `NULL`로 저장한다. `raw_id`, `code_occur_time`, `raw_contents`는 필수로 본다.

## 현재 범위 밖

- `kd-xxxx`, `er-xxxx`, `pl-xxxx` 전용 파서
- Scanner ER / Source ER ±5분 매칭
- Shot sequence 복원
- Wafer summary
- Root Cause / Yield 분석용 집계 테이블
