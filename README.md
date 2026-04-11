# FTP Batch Pipeline

## 개요

이 프로젝트는 FTP 서버의 `RUBI` 텍스트와 `RUIP` 이미지를 수집해 처리하는 배치입니다.

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

airflow_modules/
└── ftp_batch_jobs.py
dags/
└── ftp_batch_hourly_dag.py

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

자세한 매칭 규칙은 [/Users/parkjunho/PycharmProjects/PythonStudy/IMAGE_TEXT_MATCHING.md](/Users/parkjunho/PycharmProjects/PythonStudy/IMAGE_TEXT_MATCHING.md) 를 참고하면 됩니다.
