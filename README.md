# FTP Batch Pipeline

## 과제 선정 배경

EUV 설비 내부 오염이 확산되면 particle이 reticle 또는 reticle stage(RS)에 유입될 수 있습니다. 이로 인해 reticle을 scanner에 loading하기 전 수행하는 RBI(Reticle Backside Inspection) 검사에서 fail이 발생하고, reticle loading이 진행되지 못하는 문제가 발생합니다.

따라서 reticle backside 오염과 관련된 검사 텍스트 및 이미지를 안정적으로 수집하고, 발생 시점 기준으로 연결해 분석 가능한 형태로 보관할 수 있는 시스템이 필요합니다.

## 과제 목표

RUBI 텍스트와 RUIP 이미지를 수집 및 매칭하여 reticle backside 오염 분석 데이터를 구축합니다. 특히 RBI fail 원인 particle의 발생 시점과 관련 이미지를 추적할 수 있도록 텍스트 검사 결과와 이미지 데이터를 1:1로 연결하고, 변환된 PNG 결과물을 분석 서버 경로에 업로드하는 배치 파이프라인을 구현합니다.

## 기술적 선택지 및 선택 사유

본 시스템은 reticle 오염 원인 분석 알고리즘 자체가 아니라, 분석에 필요한 검사 텍스트와 이미지 evidence를 정합성 있게 연결하는 데이터 파이프라인입니다. 구현 시 선택할 수 있었던 주요 기술적 분기점과 현재 선택은 다음과 같습니다.

| 분기점 | 선택 가능 방식 | 현재 선택 | 선택 사유 |
| --- | --- | --- | --- |
| 처리 기준 | 이미지 기준, 텍스트 기준, 텍스트+이미지 통합 처리 | `COMBINED` 텍스트 기준 통합 처리 | RBI 검사 결과 텍스트가 기준 데이터이고, 이미지는 분석 근거 자료로 연결되는 구조가 적합합니다. |
| 매칭 방식 | 파일명 기준, 시간 기준, reticle ID/검사 ID 기준, 수동 매칭 | 파일명 `prefix`와 timestamp 기반 자동 매칭 | 현재 입력 데이터에서 공통으로 확보 가능한 식별자가 파일명 prefix와 생성 시각입니다. |
| 매칭 시간 범위 | 동일 시각만 허용, 1분 이내, 5분 이내, 10분 이상, 가장 가까운 파일 무조건 매칭 | 5분 이내 최근접 이미지 1개 | 너무 좁으면 정상 데이터가 누락되고, 너무 넓으면 잘못된 이미지가 연결될 수 있어 5분을 기준으로 둡니다. |
| 매칭 실패 처리 | 전체 skip, 텍스트만 저장, 미매칭 상태 저장, 에러 후 재시도 | `COMBINED`에서는 텍스트만 저장 | 텍스트 검사 결과는 분석의 기준 데이터이므로 이미지가 없어도 보존합니다. |
| 중복 처리 | 매번 변환/업로드, 기존 결과 skip, 해시 비교, DB 상태값 기준 재처리 | 서버 PNG 존재 시 skip, 로컬 PNG 존재 시 재사용 | 반복 실행 시 불필요한 이미지 변환과 업로드 비용을 줄입니다. |
| 완료 기준 | DB 저장 성공, PNG 변환 성공, 서버 업로드 성공, 원본 삭제 성공 | 서버 업로드 성공 후 DB 저장 및 원본 삭제 | 분석 서버에 결과 이미지가 없는 상태에서 완료 처리되는 문제를 줄입니다. |
| 원본 파일 처리 | 원본 유지, 성공 후 삭제, archive 이동, 실패 파일 별도 보관 | 처리 성공 후 FTP 원본 삭제 | 중복 처리를 줄이고 입력 경로를 정리합니다. |
| 날짜 범위 | 입력일만 처리, 전날+입력일 처리, 전후 N일 처리, 마지막 처리 시점 이후 처리 | 전날+입력일 2일 처리 | 자정 전후에 생성된 텍스트와 이미지의 매칭 누락을 줄입니다. |

## 개요

이 프로젝트는 FTP 서버의 `RUBI` 텍스트와 `RUIP` 이미지를 수집해 처리하는 reticle 오염 분석용 배치입니다.

- `RUBI`: 텍스트 파일을 파싱해 SQLite에 벌크 인서트합니다.
- `RUPI`: 이미지와 텍스트를 매칭해 PNG로 변환하고 FTP `rbi/ruip/.../*.png`로 업로드합니다.
- `COMBINED`: 텍스트를 기준으로 이미지까지 같이 처리하는 통합 모드입니다.

현재 운영 기본값은 `COMBINED` 기준입니다. 입력일 하나를 받으면 내부적으로 `전날 + 입력일` 두 날짜를 함께 처리합니다.

## 디렉토리 구조

```text
ftp_batch/
├── app/
│   └── batch_runner.py
├── common/
│   ├── date_utils.py
│   └── path_utils.py
├── config/
│   └── local_test_settings.py
├── infra/
│   ├── db_manager.py
│   └── ftp_scanner.py
├── matching/
│   └── image_text_matcher.py
└── processors/
    ├── rubi_processor.py
    └── rupi_processor.py

batch_main/
└── main.py
airflow_modules/
└── ftp_batch_jobs.py
dags/
└── ftp_batch_hourly_dag.py

main.py
test.py
init_db.py
local_ftp_server.py
IMAGE_TEXT_MATCHING.md
```

## 처리 흐름

### RUBI

1. `/RUBI/<YYYYMMDD>` 스캔
2. 파일별 다운로드
3. 텍스트 파싱 후 `DataFrame` 생성
4. SQLite 벌크 인서트
5. commit 성공 시 FTP 원본 삭제

### RUPI

1. `/RUIP/<YYYYMMDD>` 이미지 스캔
2. `/RUBI/<YYYYMMDD>` 텍스트 후보 조회
3. 이미지 기준으로 가까운 텍스트 매칭
4. 로컬 PNG 준비
5. 마지막에 업로드 큐를 순차 업로드

### COMBINED

1. `전날 + 입력일`의 `/RUBI`와 `/RUIP`를 모두 먼저 다운로드
2. 텍스트 파일을 시간순으로 파싱
3. 텍스트보다 늦지 않고 5분 이내인 가장 가까운 이미지 1개를 1:1 매칭
4. 매칭 이미지가 있으면 PNG 준비 후 업로드 큐에 등록
5. 매칭 없는 텍스트는 텍스트만 DB 저장
6. 마지막 업로드 단계에서 성공한 항목만 DB finalize 후 텍스트/이미지 FTP 원본 삭제

## 주요 구성 요소

### `BatchRunner`

위치: [/Users/parkjunho/PycharmProjects/PythonStudy/ftp_batch/app/batch_runner.py](/Users/parkjunho/PycharmProjects/PythonStudy/ftp_batch/app/batch_runner.py)

- `RUBI`, `RUPI`, `COMBINED` 실행 분기
- `전날 + 입력일` 두 날짜 처리
- `COMBINED`에서 로컬 PNG 3일 보관 cleanup
- 업로드 큐 제어

### `FTPScanner`

위치: [/Users/parkjunho/PycharmProjects/PythonStudy/ftp_batch/infra/ftp_scanner.py](/Users/parkjunho/PycharmProjects/PythonStudy/ftp_batch/infra/ftp_scanner.py)

- 날짜 폴더 스캔
- 다운로드/업로드
- 원격 존재 여부와 크기 검증
- FTP 원본 삭제

현재 `scan()`은 날짜 폴더 바로 아래 파일만 조회하고 재귀 스캔은 하지 않습니다.

### `DBManager`

위치: [/Users/parkjunho/PycharmProjects/PythonStudy/ftp_batch/infra/db_manager.py](/Users/parkjunho/PycharmProjects/PythonStudy/ftp_batch/infra/db_manager.py)

- `fetch_df(query, params=None, connection=None)`
- `bulk_insert_df(table_name, df, connection=None)`
- `execute(query, params=None, connection=None)`
- `transaction()`

조회는 `pandas.read_sql_query`, 인서트는 `DataFrame -> sqlite3.executemany` 기반 벌크 인서트입니다.

### `RubiProcessor`

위치: [/Users/parkjunho/PycharmProjects/PythonStudy/ftp_batch/processors/rubi_processor.py](/Users/parkjunho/PycharmProjects/PythonStudy/ftp_batch/processors/rubi_processor.py)

- 텍스트 디코딩
- 라인 파싱
- `rubi_ingest`용 `DataFrame` 생성

### `RupiProcessor`

위치: [/Users/parkjunho/PycharmProjects/PythonStudy/ftp_batch/processors/rupi_processor.py](/Users/parkjunho/PycharmProjects/PythonStudy/ftp_batch/processors/rupi_processor.py)

- TIF -> PNG 변환
- `rupi_ingest` upsert/finalize
- 로컬 PNG 경로 생성

### `image_text_matcher`

위치: [/Users/parkjunho/PycharmProjects/PythonStudy/ftp_batch/matching/image_text_matcher.py](/Users/parkjunho/PycharmProjects/PythonStudy/ftp_batch/matching/image_text_matcher.py)

- 파일명에서 `prefix`, timestamp 추출
- 텍스트 기준 최근접 이미지 선택
- 이미지 기준 최근접 텍스트 선택
- `RUIP -> rbi/ruip/.../*.png` 경로 변환

## 실행 방법

### 공통 main 실행

운영용 공통 진입점은 [main.py](/Users/parkjunho/PycharmProjects/PythonStudy/main.py) 입니다. 실제 분기 로직은 [batch_main/main.py](/Users/parkjunho/PycharmProjects/PythonStudy/batch_main/main.py)에 두고, 루트 `main.py`는 얇은 실행 래퍼로 유지합니다.

`BATCH_TARGET` 값에 따라 RBI 배치와 ER Dose 배치를 분기합니다.

먼저 프로젝트 루트로 이동합니다.

```bash
cd /Users/parkjunho/PycharmProjects/PythonStudy
```

`RBI` 실행:

```bash
BATCH_TARGET=RBI \
RBI_INPUT_DATE=2026-05-31 \
RBI_PARSER=COMBINED \
.venv/bin/python main.py
```

`ER_DOSE` 실행:

```bash
BATCH_TARGET=ER_DOSE \
ER_DOSE_START_TIME=2026-05-31T00:00:00 \
ER_DOSE_END_TIME=2026-06-01T00:00:00 \
ER_DOSE_LIMIT=1000 \
ER_DOSE_DB_DSN='postgresql://user:password@host:5432/dbname' \
.venv/bin/python main.py
```

필수 환경변수:

- `BATCH_TARGET`: `RBI` 또는 `ER_DOSE`
- `RBI_INPUT_DATE`: RBI 기준 날짜, `YYYY-MM-DD`
- `ER_DOSE_START_TIME`: ER Dose 조회 시작 시각
- `ER_DOSE_END_TIME`: ER Dose 조회 종료 시각
- `ER_DOSE_DB_DSN` 또는 `DATABASE_URL`: PostgreSQL DSN

선택 환경변수:

- `RBI_PARSER`: `RUBI`, `RUPI`, `COMBINED`, 기본값 `COMBINED`
- `ER_DOSE_LIMIT`: 최대 처리 row 수
- `INPUT_DATE`: `RBI_INPUT_DATE` 대체값
- `START_TIME`: `ER_DOSE_START_TIME` 대체값
- `END_TIME`: `ER_DOSE_END_TIME` 대체값

실행 규칙:

- `BATCH_TARGET=RBI`이면 FTP 기반 RBI 배치를 실행합니다.
- `BATCH_TARGET=ER_DOSE`이면 ER RAW 로그 파싱 배치를 실행합니다.
- `BATCH_TARGET=ER_DOES`도 `ER_DOSE`와 동일하게 처리합니다.
- 환경변수 없이 `main.py`를 실행하면 `BATCH_TARGET` 필수 오류가 발생합니다.

### DB 초기화

```bash
/Users/parkjunho/PycharmProjects/PythonStudy/.venv/bin/python \
  /Users/parkjunho/PycharmProjects/PythonStudy/init_db.py
```

### 로컬 FTP 서버

```bash
/Users/parkjunho/PycharmProjects/PythonStudy/.venv/bin/python \
  /Users/parkjunho/PycharmProjects/PythonStudy/local_ftp_server.py
```

### 배치 실행

`RUBI`

```bash
/Users/parkjunho/PycharmProjects/PythonStudy/.venv/bin/python \
  /Users/parkjunho/PycharmProjects/PythonStudy/test.py \
  --input-date 2026-04-11 \
  --parser RUBI
```

`RUPI`

```bash
/Users/parkjunho/PycharmProjects/PythonStudy/.venv/bin/python \
  /Users/parkjunho/PycharmProjects/PythonStudy/test.py \
  --input-date 2026-04-11 \
  --parser RUPI
```

`COMBINED`

```bash
/Users/parkjunho/PycharmProjects/PythonStudy/.venv/bin/python \
  /Users/parkjunho/PycharmProjects/PythonStudy/test.py \
  --input-date 2026-04-11 \
  --parser COMBINED
```

## Airflow

- DAG: [/Users/parkjunho/PycharmProjects/PythonStudy/dags/ftp_batch_hourly_dag.py](/Users/parkjunho/PycharmProjects/PythonStudy/dags/ftp_batch_hourly_dag.py)
- 래퍼: [/Users/parkjunho/PycharmProjects/PythonStudy/airflow_modules/ftp_batch_jobs.py](/Users/parkjunho/PycharmProjects/PythonStudy/airflow_modules/ftp_batch_jobs.py)
- 스케줄: 매시간 정각
- 기본 실행 모드: `COMBINED`

## 설정

위치: [/Users/parkjunho/PycharmProjects/PythonStudy/ftp_batch/config/local_test_settings.py](/Users/parkjunho/PycharmProjects/PythonStudy/ftp_batch/config/local_test_settings.py)

주요 값:

- `CLIENT_FTP_ROOT_PATH = "/RUIP"`
- `TEXT_FTP_ROOT_PATH = "/RUBI"`
- `SERVER_FTP_ROOT_PATH = "/rbi"`
- `LOCAL_WORK_DIR`
- `LOCAL_DB_PATH`
- `RUPI_SCALE_PERCENT`

## 데이터 정합성 / 로컬 보관 정책

- `RUBI`는 DB commit 성공 전에는 FTP 원본을 삭제하지 않습니다.
- `COMBINED`에서 매칭된 항목은 업로드 성공 후 DB commit, 그 다음 FTP 원본 삭제 순서로 처리합니다.
- 로컬 raw `txt`/`tif`는 scratch 파일이라 처리 후 즉시 삭제합니다.
- 로컬 `png`는 재업로드 캐시로 3일 유지하고, 배치 시작 시 3일 지난 파일만 정리합니다.

## 현재 한계

- `commit 성공 후 FTP delete 실패` 재시도 큐는 아직 없습니다.
- DB는 SQLite 기준입니다.
- `scan()`은 날짜 폴더 바로 아래 파일만 읽고 재귀 스캔은 지원하지 않습니다.

## 이번 범위에 넣지 않은 것

- `.env` 기반 설정 로딩
- `ON CONFLICT DO NOTHING` 기반 중복 처리 정책 변경
- FTP 재귀 디렉토리 삭제 유틸
- `00:00 ~ 01:59` 전날만 처리하는 시간 분기
- PostgreSQL 전환

자세한 매칭 규칙은 [/Users/parkjunho/PycharmProjects/PythonStudy/IMAGE_TEXT_MATCHING.md](/Users/parkjunho/PycharmProjects/PythonStudy/IMAGE_TEXT_MATCHING.md) 를 참고하면 됩니다.
