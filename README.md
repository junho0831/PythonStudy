# Remote Batch Processor

원격 서버의 날짜별 디렉토리를 SSH/SFTP로 조회해 파일을 처리하는 배치 프로그램이다.

현재 구현의 메인은 `Rubi(txt)` 파이프라인이며, `Rubp(tif)`도 날짜 폴더를 스캔하고 처리 이력 테이블을 거쳐 `png` 변환까지 수행하는 상태다.

## 핵심 변경 사항

- `Rubi`와 `Rubp`를 역할별로 분리했다.
  - `Rubi`: txt 파일을 원격에서 직접 읽고 파싱 후 DB 적재
  - `Rubp`: tif 파일 목록을 조회하고, 처리 이력을 남기면서 SFTP 기반 PNG 변환 처리로 확장할 수 있게 분리
- 디렉토리 구조를 도메인 기준으로 재구성했다.
  - `remote_batch/app`: CLI, 실행 흐름
  - `remote_batch/common`: 공통 상수, 모델, 파일명 규칙
  - `remote_batch/infra`: SSH/SFTP, DB, 공통 CRUD
  - `remote_batch/domains/rubi`: txt 파싱/처리
  - `remote_batch/domains/rubp`: tif -> png 변환 처리
- DB 접근은 `SQLAlchemy Engine` 기반으로 정리했다.
- 파싱 결과 적재 전 `pandas DataFrame`으로 정규화한다.
- 반복 SQL은 `CrudClient` 공통 계층으로 모았다.
- 런타임에서 테이블을 자동 생성하지 않도록 변경했다.
  - 테이블은 사전에 한 번만 생성해 두고, 배치는 처리만 수행한다.

## 핵심 로직

본 프로그램은 `Rubi(txt)`와 `Rubp(tif)` 두 가지 도메인 데이터를 처리합니다. 

1. **파일 스캔**: `io_mode` 설정에 따라 로컬 디렉토리 또는 SSH/SFTP를 통해 원격 디렉토리의 최근 N일치 폴더를 스캔합니다.
2. **이력 제어**: DB의 `file_processing_history` 테이블을 조회하여 이미 성공(`DONE`)했거나 현재 진행 중(`PROCESSING`)인 파일은 건너뜁니다.
3. **도메인 작업**:
   - **Rubi**: 텍스트 파일을 읽어 파싱한 후 결과를 DB에 저장합니다.
   - **Rubp**: TIF 파일을 SFTP로 읽어 설정된 비율로 리사이즈한 후 PNG 형식으로 저장합니다. 원격 모드에서는 로컬에서 변환한 뒤 SFTP로 다시 업로드합니다.
4. **상태 업데이트**: 작업 성공 시 `DONE`, 예외 발생 시 `FAIL`로 이력을 업데이트합니다.

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

    A3 --> E1["Rubp tif 목록 조회"]
    E1 --> E2["처리 이력 조회"]
    E2 --> E3["SFTP download -> 축소 -> png -> upload"]
    E3 --> E4["DONE 또는 FAIL 갱신"]
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
- `sw3qaG_20260318_233000.tif`

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

중복 제거는 디렉토리 재조회가 아니라 `file_processing_history` 테이블로 관리한다. 이 규칙은 `Rubi`와 `Rubp`에 공통 적용된다.

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

### 7. Rubp(tif)는 이력 관리 후 png 변환

현재는 `Rubp`를 실제로 `tif -> 해상도 축소 -> png 변환`까지 처리한다.

- 로컬 모드에서는 `OpenCV`로 직접 변환
- 원격 모드에서는 `SFTP`로 tif를 읽어 로컬에서 변환한 뒤 결과 png를 다시 업로드
- `.tif` 파일을 최근 N일 폴더에서 조회
- `file_processing_history` 기준으로 `DONE/FAIL/PROCESSING` 상태 관리
- 기본 출력 확장자는 `.png`
- 기본 축소 비율은 `50%`
- 결과는 `RUBP_OUTPUT_BASE_DIR/YYYYMMDD/*.png` 경로에 저장

관련 코드:

- [`run_batch()`](/Users/parkjunho/PycharmProjects/PythonStudy/remote_batch/app/runner.py#L13)
- [`process_rubp_file()`](/Users/parkjunho/PycharmProjects/PythonStudy/remote_batch/domains/rubp/service.py#L19)
- [`remote_batch/domains/rubp/service.py`](/Users/parkjunho/PycharmProjects/PythonStudy/remote_batch/domains/rubp/service.py)

## 실행 진입점

실행 파일:

- [`remote_batch_processor.py`](/Users/parkjunho/PycharmProjects/PythonStudy/remote_batch_processor.py#L1)

터미널 인자 없이 로컬에서 실행하려면 아래 프로퍼티 파일에 값을 넣으면 된다.

- [`local.properties`](/Users/parkjunho/PycharmProjects/PythonStudy/remote_batch/app/local.properties)

현재 로컬 테스트 기준 기본값:

```properties
IO_MODE=local
SSH_HOST=127.0.0.1
SSH_PORT=22
SSH_USERNAME=parkjunho
SSH_PASSWORD=
SSH_KEY_FILE=/Users/parkjunho/.ssh/id_rsa
DB_HOST=127.0.0.1
DB_PORT=5432
DB_USER=parkjunho
DB_PASSWORD=
DB_NAME=pythonstudy_demo
RUBI_BASE_DIR=/Users/parkjunho/PycharmProjects/PythonStudy/local_remote_data/Rubi
RUBP_BASE_DIR=/Users/parkjunho/PycharmProjects/PythonStudy/local_remote_data/Rubp
RUBP_OUTPUT_BASE_DIR=/Users/parkjunho/PycharmProjects/PythonStudy/local_remote_data/Rubp_png
RUBP_SCALE_PERCENT=50
DAYS_BACK=3
PROCESSING_TIMEOUT_MINUTES=120
LOG_LEVEL=INFO
```

`DB_DSN` 한 줄로도 줄 수 있지만, 현재 기본 설정은 위처럼 분리된 DB 프로퍼티를 조합해 사용한다.

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

## 실행 결과 해석

로컬 테스트 기준으로 기대 결과는 아래와 같다.

### 첫 실행

- 최근 3일 폴더의 txt 파일을 찾음
- 각 파일을 읽고 파싱 후 DB 적재
- 로그 예:
  - `Rubi txt 처리 완료: ... (3건)`

### 같은 파일로 다시 실행

- 이미 `file_processing_history`에 `DONE`으로 기록된 파일은 재처리하지 않음
- 로그 예:
  - `처리 이력에 DONE 또는 최근 PROCESSING 상태가 있어 skip: ...`

즉 아래처럼 나오면 정상이다.

```text
I/O 모드: local (SSH 미사용)
Rubi 대상 txt 파일 수: 3
처리 이력에 DONE 또는 최근 PROCESSING 상태가 있어 skip: .../sw3qaG_20260316_193738.txt
처리 이력에 DONE 또는 최근 PROCESSING 상태가 있어 skip: .../sw3qaG_20260317_193738.txt
처리 이력에 DONE 또는 최근 PROCESSING 상태가 있어 skip: .../sw3qaG_20260318_193738.txt
Rubp 대상 tif 파일 수: 1
Rubp tif 처리 완료: .../sw3qaG_20260318_233000.tif -> .../sw3qaG_20260318_233000.png
```

### 다시 처리 로그를 보고 싶을 때

- 새 파일명을 가진 txt를 날짜 폴더에 추가하면 그 파일만 새로 처리된다
- 예:
  - `sw3qaG_20260318_194600.txt`

파일명 datetime이 다르면 신규 파일로 판단하고 다시 `PROCESSING -> DONE` 흐름을 탄다.

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
