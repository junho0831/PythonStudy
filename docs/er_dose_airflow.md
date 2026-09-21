# ER Dose 날짜별 DAG Run과 장애 복구

## Airflow 화면과 실행 구조

DAG는 `er_dose_daily` 하나이고 태스크 ID는 네 개로 고정합니다.

```text
start → parse_er_data_euv → parse_er_data_raw → end
```

RAW 태스크 마지막에 수율 → 원인 → RAW 설비 건수 → EUV 설비 건수 순서로 집계합니다. 내부 스레드는 없습니다. 연결은 DAG 파일 맨 아래의 `>>` 한 줄입니다.

한 Run이 날짜 하루만 처리합니다. 10일 처리 시 같은 DAG에 Run 10개가 생기며, Grid에서는 Run별 열과 고정 태스크 행으로 실행 결과를 확인합니다. 예를 들어 1월 12일 데이터를 처리하는 Run은 `manual__2026-01-12`이고 logical date는 `2026-01-12T00:00:00+00:00`입니다. UI 시간대를 UTC 또는 Asia/Seoul로 두면 처리 날짜와 같은 달력 날짜로 보입니다. 다른 시간대에서는 표시 일자가 달라질 수 있으므로 날짜가 들어간 Run ID도 확인할 수 있습니다.

태스크 이름에 날짜를 붙이지 않고 Run 자체에 날짜를 붙입니다. `start`는 logical date와 실제 처리 날짜가 다르거나 logical date가 UTC 자정이 아니면 실패합니다. 임의의 현재 시각으로 UI에서 Trigger하고 과거 `target_date`만 conf에 넣는 방식은 사용하지 않습니다.

## 10일 실행과 날짜 순서

Airflow 환경의 명령 `airflow_modules.er_dose_runs`가 날짜별 Run을 생성하고 성공을 기다린 뒤 다음 날짜를 생성합니다. 추가 관리 DAG는 없습니다. DAG는 `schedule=None`, `max_active_runs=1`입니다. 시간순서 보장은 동시 실행 제한뿐 아니라 실행 명령의 성공 대기로 처리합니다. 한 날짜가 실패하거나 하루 동안 완료되지 않으면 다음 날짜를 제출하지 않고 종료합니다.

```bash
# 10일 전부터 어제까지: 신규 날짜 실행, 기존 성공 날짜는 유지
python -m airflow_modules.er_dose_runs

# 매일 최근 10일의 원천 건수를 다시 비교할 때
python -m airflow_modules.er_dose_runs --recheck-successful

# 지정 기간 또는 하루만 처리
python -m airflow_modules.er_dose_runs --start-date 2026-01-02 --end-date 2026-01-11
python -m airflow_modules.er_dose_runs --start-date 2026-01-12
```

기본 기준일은 명령 시작 시 서울 날짜입니다. `--reference-date 2026-01-12`이면 1월 2일~11일, 총 10일입니다. 시작일만 지정하면 하루만 처리하고, 역전된 기간 또는 10일 초과는 오류입니다. 날짜가 바뀌어도 같은 명령 실행 중 확정된 범위는 변하지 않습니다.

날짜별 logical date가 고유하므로 기존 Run이 있으면 재사용합니다. 기본 모드는 이미 성공한 날짜를 다시 실행하지 않습니다. `--recheck-successful`은 성공한 날짜 Run의 태스크를 Clear하여 건수 비교부터 다시 실행합니다. 해당 날짜의 기존 실행 상태가 재설정되며 새 열이 추가되는 것은 아닙니다. 실패한 Run은 자동으로 Clear하지 않습니다.

매일 자동 점검하려면 기존 운영 실행 스케줄에서 위 명령을 호출해야 합니다. 이 저장소에서 운영 스케줄 등록은 수행하지 않았습니다. 실행 명령은 Airflow scheduler/메타데이터 DB에 접근 가능한 하나의 Linux 호스트에서 실행하며 파일 잠금으로 그 호스트의 중복 실행을 차단합니다. 서로 다른 호스트에서 동시에 제출하거나 별도로 수동 제출한 Run 사이의 순서는 보장하지 않습니다. DAG는 unpause되어 있어야 합니다. 대기는 DAG worker 태스크가 아닌 이 명령 프로세스에서 합니다.

## XCom과 건수 비교

날짜 범위는 실행 명령이 정하고 DAG는 Run의 날짜 한 개만 사용합니다.

`start`는 XCom `date`에 `%Y-%m-%d` 날짜를, `ER_DOSE_COMMANDS`에 전체 SSH 명령 템플릿을 저장합니다. 명령은 Variable `ER_DOSE_EUV_COMMAND`·`ER_DOSE_RAW_COMMAND`에서 읽고 `{target_date}`만 치환합니다. 기존 `--parser` 값을 새로 붙이지 않습니다.

EUV 태스크가 RAW·EUV 원천/parsed 건수를 비교하고, 불일치하며 parsed가 있으면 기존 DISTINCT 비교도 수행합니다. 필요한 EUV 파싱을 실행하고 RAW 태스크로 넘어갑니다. RAW 태스크는 필요한 RAW 파싱과 후속 집계를 실행합니다. 둘 다 재처리가 불필요하면 파싱·집계를 생략하고 Run을 성공으로 끝냅니다. 선택된 파싱만 SSHHook으로 Linux 서버에 전달합니다.

## 장애 복구

실패 태스크는 5분 간격으로 최대 3회 재시도합니다. Airflow가 재시도 시 XCom을 비우므로 날짜·계획·명령·완료 단계를 `er_dose_progress_<Run 식별자 해시>` 임시 Variable에 저장합니다. EUV·RAW는 같은 Run의 기록을 공유합니다. 성공한 단계를 건너뛰어 이어서 처리하고 RAW·집계까지 성공하면 기록을 삭제합니다.

실패 Run의 실패 태스크와 막힌 후속 태스크만 Clear하고 실행 명령을 다시 호출하면 그 날짜 완료를 기다린 뒤 다음 날짜로 진행합니다. 성공한 EUV와 `start` XCom은 유지합니다. 해당 날짜를 처음부터 재처리하려면 진행 Variable을 삭제하고 Run 전체를 Clear합니다. 성공 후 재점검은 `--recheck-successful`을 사용합니다.

외부 작업 완료와 진행 기록 저장은 별개이므로 기록 직전 worker가 종료되면 해당 단계가 반복될 수 있습니다. 파서는 날짜 파티션 재적재, 집계는 DELETE → INSERT로 복구합니다. DELETE·INSERT는 별도 메서드·별도 커밋이며 통계 저장은 `bulk_insert_df`입니다. PostgreSQL 9.4 호환 쿼리와 기존 필터를 유지합니다.

## 프로젝트 경계와 배포

Airflow와 파서는 별도 프로젝트입니다. Airflow는 자체 DB 연결과 쿼리로 날짜·건수 비교·집계를 처리하고 Linux 파서는 전달받은 날짜만 파싱합니다. 서로의 Python 모듈과 DB 설정을 직접 가져오지 않습니다.

Airflow에는 `dags/er_dose_daily_dag.py`와 `airflow_modules`의 `er_dose_dates`, `er_dose_jobs`, `er_dose_process`, `er_dose_runs`, `er_dose_repository`, `er_dose_db`, `__init__.py`를 배포합니다. 예전 `er_dose_range_dag.py`는 배포 경로에서 제거합니다. 태스크 구조와 logical date 규칙이 바뀌므로 전환 전에 기존 실행 중인 Run을 마무리합니다.

- SSH Connection: `er_dose_parser`
- Airflow 업무 DB Connection: `er_dose_db`
- 파서는 원격 서버의 기존 DB 설정 사용

격리 Airflow 2.6.3에서 DAG 로딩·실제 Run 생성 및 날짜·중복 방지·실패 시 후속 날짜 차단·단계별 재시도를 검증합니다. 테스트의 scheduler 완료 상태와 업무 DB·SSH는 모의 처리합니다. 운영 UI 렌더링과 운영 서버 실행은 확인하지 않았습니다.
