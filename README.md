# Remote Batch Processor

원격 서버의 날짜별 디렉토리를 SSH로 조회해 파일을 처리하는 배치 프로그램이다.

현재 구현의 메인은 `Rubi(txt)` 파이프라인이며, `Rubp(tif)`는 서버 내 처리 확장용 stub만 둔 상태다.

## 핵심 변경 사항

- `Rubi`와 `Rubp`를 역할별로 분리했다.
  - `Rubi`: txt 파일을 원격에서 직접 읽고 파싱 후 DB 적재
  - `Rubp`: tif 파일을 로컬 다운로드하지 않고 서버 내 처리로 확장할 수 있게 분리
- 디렉토리 구조를 도메인 기준으로 재구성했다.
  - `remote_batch/app`: CLI, 실행 흐름
  - `remote_batch/common`: 공통 상수, 모델, 파일명 규칙
  - `remote_batch/infra`: SSH, DB, 공통 CRUD
  - `remote_batch/domains/rubi`: txt 파싱/처리
  - `remote_batch/domains/rubp`: tif stub
- DB 접근은 `SQLAlchemy Engine` 기반으로 정리했다.
- 파싱 결과 적재 전 `pandas DataFrame`으로 정규화한다.
- 반복 SQL은 `CrudClient` 공통 계층으로 모았다.
- 런타임에서 테이블을 자동 생성하지 않도록 변경했다.
  - 테이블은 사전에 한 번만 생성해 두고, 배치는 처리만 수행한다.

## 핵심 로직

## 처리 흐름도

```mermaid
flowchart LR
    subgraph A["1. 수집"]
        A1["배치 시작"] --> A2["최근 3일 폴더 계산"]
        A2 --> A3["SSH/SFTP 접속"]
        A3 --> A4["Rubi txt 목록 조회"]
        A4 --> A5["파일명 datetime 추출"]
    end

    subgraph B["2. 중복 판단"]
        A5 --> B1{"파일명 datetime\n파싱 성공?"}
        B1 -- "아니오" --> B2["WARNING 후 skip"]
        B1 -- "예" --> B3["처리 이력 조회"]
        B3 --> B4{"DONE 또는\n최근 PROCESSING ?"}
        B4 -- "예" --> B5["skip"]
        B4 -- "아니오" --> B6["PROCESSING 확보"]
    end

    subgraph C["3. 처리"]
        B6 --> C1["원격 txt 읽기"]
        C1 --> C2["utf-8 -> cp949 -> replace"]
        C2 --> C3["parse_text()"]
        C3 --> C4["pandas 정규화"]
        C4 --> C5["rubi_parsed_data upsert"]
    end

    subgraph D["4. 상태 갱신"]
        C5 --> D1["DONE 업데이트"]
        C1 -. "예외" .-> D2["FAIL 업데이트"]
        C3 -. "예외" .-> D2
        C4 -. "예외" .-> D2
        C5 -. "예외" .-> D2
    end

    A3 --> E1["Rubp tif 처리 훅"]
    E1 --> E2["현재는 stub"]
```

### 1. 최근 N일 날짜 폴더 조회

자정 경계(`23:30`, `00:00`, `00:30`) 누락을 막기 위해 오늘 폴더만 보지 않고 최근 `N일` 폴더를 같이 본다.

- 기본값: `3일`
- 예: `20260319`, `20260318`, `20260317`

관련 코드:

- [`build_recent_date_dirs()`](/Users/parkjunho/PycharmProjects/PythonStudy/remote_batch/app/config.py#L49)
- [`run_batch()`](/Users/parkjunho/PycharmProjects/PythonStudy/remote_batch/app/runner.py#L13)

### 2. 파일명 datetime 기준 처리

파일 시스템 `mtime`가 아니라 파일명 안의 `YYYYMMDD_HHMMSS`를 기준으로 판단한다.

예:

- `sw3qaG_20260912_181429.txt`
- `image_20260318_233000.tif`

이유:

- 복사 시각과 실제 생성 시각이 다를 수 있음
- 자정 경계에서 폴더만 기준으로 보면 누락될 수 있음
- 파일명 시각이 업무 데이터 기준에 더 가까움

관련 코드:

- [`extract_file_datetime()`](/Users/parkjunho/PycharmProjects/PythonStudy/remote_batch/common/file_rules.py#L7)
- [`list_remote_files()`](/Users/parkjunho/PycharmProjects/PythonStudy/remote_batch/infra/ssh.py#L28)

### 3. Rubi(txt)는 원격에서 직접 읽음

txt 파일은 로컬로 다운로드하지 않는다.

- `SFTP`로 파일 목록 조회
- `sftp.open()`으로 원격에서 직접 읽기
- `utf-8` 우선
- 실패 시 `cp949` fallback
- 그래도 실패하면 `errors="replace"`로 진행하고 warning 로그 남김

관련 코드:

- [`list_remote_files()`](/Users/parkjunho/PycharmProjects/PythonStudy/remote_batch/infra/ssh.py#L28)
- [`read_remote_text_file()`](/Users/parkjunho/PycharmProjects/PythonStudy/remote_batch/infra/ssh.py#L64)
- [`process_rubi_file()`](/Users/parkjunho/PycharmProjects/PythonStudy/remote_batch/domains/rubi/service.py#L19)

### 4. 중복 방지와 재실행 안전성

중복 제거는 디렉토리 재조회가 아니라 `file_processing_history` 테이블로 관리한다.

상태:

- `PROCESSING`
- `DONE`
- `FAIL`

규칙:

- `DONE` 파일은 재처리하지 않음
- `FAIL` 파일은 재처리 가능
- `PROCESSING` 상태가 오래 남은 파일은 재처리 전략을 붙일 수 있게 `timeout` 기준으로 재진입 가능

관련 코드:

- [`acquire_processing_slot()`](/Users/parkjunho/PycharmProjects/PythonStudy/remote_batch/infra/db.py#L19)
- [`mark_history_done()`](/Users/parkjunho/PycharmProjects/PythonStudy/remote_batch/infra/db.py#L130)
- [`mark_history_fail()`](/Users/parkjunho/PycharmProjects/PythonStudy/remote_batch/infra/db.py#L146)

### 5. 파싱 결과 적재

현재 txt 포맷은 확정되지 않았으므로 샘플 파서를 넣었다.

- `key=value`
- `csv 비슷한 콤마 구분`
- 그 외 raw line

파싱 결과는 `list[dict]`로 만들고, 적재 전 `pandas DataFrame`으로 정규화한 뒤 JSONB로 upsert 한다.

관련 코드:

- [`parse_text()`](/Users/parkjunho/PycharmProjects/PythonStudy/remote_batch/domains/rubi/parser.py#L4)
- [`normalize_parsed_records()`](/Users/parkjunho/PycharmProjects/PythonStudy/remote_batch/infra/db.py#L87)
- [`insert_parsed_data()`](/Users/parkjunho/PycharmProjects/PythonStudy/remote_batch/infra/db.py#L103)

### 6. 공통 CRUD 적용

반복되는 DB 작업은 `CrudClient` 공통 계층으로 정리했다.

제공 기능:

- `fetch_one`
- `fetch_all`
- `insert`
- `update`
- `delete`
- `upsert`

`file_processing_history`, `rubi_parsed_data` 적재/갱신은 이 공통 계층을 통해 수행한다.

관련 코드:

- [`CrudClient`](/Users/parkjunho/PycharmProjects/PythonStudy/remote_batch/infra/crud.py#L23)
- [`remote_batch/infra/db.py`](/Users/parkjunho/PycharmProjects/PythonStudy/remote_batch/infra/db.py#L19)

### 7. Rubp(tif)는 현재 stub

현재는 실제 tif 처리 전체를 구현하지 않았다.

- 로컬 다운로드 대신 서버 내 처리 방향 유지
- 나중에 서버 명령 실행이나 서버 내 이미지 처리 로직을 붙일 수 있게 분리

관련 코드:

- [`process_tif_stub()`](/Users/parkjunho/PycharmProjects/PythonStudy/remote_batch/domains/rubp/service.py#L8)

## 실행 진입점

실행 파일:

- [`remote_batch_processor.py`](/Users/parkjunho/PycharmProjects/PythonStudy/remote_batch_processor.py#L1)

예시:

```bash
python3 remote_batch_processor.py \
  --ssh-host your-host \
  --ssh-username your-user \
  --ssh-key-file /path/to/key \
  --db-dsn 'postgresql://user:pass@host:5432/dbname' \
  --rubi-base-dir /data/Rubi \
  --rubp-base-dir /data/Rubp \
  --days-back 3
```

## 사전 준비

배치는 테이블을 자동 생성하지 않는다.

즉 아래 테이블은 사전에 한 번 만들어져 있어야 한다.

- `file_processing_history`
- `rubi_parsed_data`

테이블 상세 설명과 생성 SQL은 [TABLES.md](/Users/parkjunho/PycharmProjects/PythonStudy/TABLES.md) 참고.

의존성:

- `paramiko`
- `sqlalchemy`
- `psycopg2-binary`
- `pandas`
