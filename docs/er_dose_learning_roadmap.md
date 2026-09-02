# ER Dose 프로젝트 기반 기술 학습 로드맵

## 목적

이 문서는 ER Dose 대용량 배치에서 실제로 발생한 메모리 증가, 긴 처리 시간, 부분 적재, 재실행 판단 문제를 학습 과제로 연결한다.

현재 프로젝트에서는 Kafka나 Redis 같은 별도 인프라를 먼저 도입하기보다 다음 역량을 우선해서 학습하는 편이 효과적이다.

1. PostgreSQL 대량 조회·적재와 실행계획 분석
2. Airflow 배치의 멱등성과 실패 복구
3. 실제 PostgreSQL을 사용하는 통합 테스트
4. Python 및 Linux 성능 분석
5. Repository, 트랜잭션 경계와 기술 의사결정 기록

기존 성능 측정 결과와 처리 구조는 [Database Streaming Optimization](./db_streaming_optimization.md)을 기준 자료로 사용한다.

## 1. PostgreSQL 대량 처리

### 추천 자료

- [PostgreSQL EXPLAIN 사용법](https://www.postgresql.org/docs/current/using-explain.html)
- [PostgreSQL 대량 데이터 적재](https://www.postgresql.org/docs/current/populate.html)
- [PostgreSQL COPY](https://www.postgresql.org/docs/current/sql-copy.html)
- [PostgreSQL 테이블 파티셔닝](https://www.postgresql.org/docs/current/ddl-partitioning.html)
- [PostgreSQL 트랜잭션 격리](https://www.postgresql.org/docs/current/transaction-iso.html)
- [PostgreSQL MVCC](https://www.postgresql.org/docs/current/mvcc-intro.html)
- [PostgreSQL WAL](https://www.postgresql.org/docs/current/wal-intro.html)
- [pg_stat_statements](https://www.postgresql.org/docs/current/pgstatstatements.html)
- [PostgreSQL 명령 진행률 확인](https://www.postgresql.org/docs/current/progress-reporting.html)

### 집중해서 볼 내용

- `EXPLAIN ANALYZE`의 예상 행 수와 실제 행 수 차이
- `Buffers`, `temp read/written`, `Sort Method`, `loops`
- 날짜 조건이 원천 파티션을 제대로 제거하는지 나타내는 partition pruning
- `COPY`와 일반 `INSERT`의 처리 방식 차이
- 청크별 커밋과 날짜 전체 트랜잭션의 성능·복구 범위 차이
- 인덱스를 유지한 채 대량 적재할 때 발생하는 쓰기 비용
- 임시 테이블에 적재·검증한 뒤 파티션을 교체하는 방식
- `pg_stat_statements`의 실행시간, 블록 읽기, 임시 블록, WAL 발생량
- 실행 중인 `COPY`를 `pg_stat_progress_copy`로 확인하는 방법

### 프로젝트 실습

1. `docs/db_streaming_optimization.md`의 RAW 조회 SQL에 `EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, SUMMARY ON)`을 적용한다.
2. source count, target count, 설비별 count, 일별 summary 쿼리의 실행계획을 각각 저장한다.
3. 대상 날짜 파티션만 읽는지, 정렬이 디스크로 내려가는지, 예상 행 수가 실제 값과 크게 다른지 비교한다.
4. 동일한 테스트 데이터로 청크별 커밋과 날짜 단위 커밋의 시간 및 실패 결과를 비교한다.
5. 가능하면 임시 테이블 적재 후 검증·파티션 교체 방식을 별도 실험한다.

`EXPLAIN ANALYZE`는 쿼리를 실제로 실행한다. 운영 환경에서는 부하가 낮은 시간에 수행하고, `pg_stat_statements` 활성화는 DB 관리자와 먼저 협의한다.

## 2. Airflow 멱등성과 실패 복구

### 추천 자료

- [Airflow Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [Airflow DAG Run과 Data Interval](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dag-run.html)
- [Airflow Backfill](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/backfill.html)
- [Airflow Metrics](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/metrics.html)

### 집중해서 볼 내용

- Airflow task를 하나의 트랜잭션처럼 취급하는 이유
- 같은 날짜를 여러 번 실행해도 최종 결과가 같아야 하는 멱등성
- 현재 시간이 아니라 명시적인 data interval을 기준으로 읽고 쓰는 방식
- retry, clear, backfill 실행 시 데이터가 중복되거나 일부만 남지 않도록 하는 방법
- 태스크 실행시간, 실패, 재시도, 지연을 지표로 남기는 방법

### 프로젝트 실습

다음 실패 시나리오를 같은 날짜 데이터로 반복 검증한다.

| 시나리오 | 검증할 결과 |
| --- | --- |
| 정상 실행 후 같은 날짜 재실행 | 최종 건수와 내용이 동일함 |
| 세 번째 청크 적재 전에 실패 | 불완전한 결과가 사용자에게 노출되는지 확인 |
| 세 번째 청크 적재 후 실패 | 재실행 후 중복과 누락이 없는지 확인 |
| summary 갱신 전 실패 | parsed와 summary의 불일치 복구 여부 확인 |
| 동일 날짜 동시 실행 | 중복 재적재 또는 truncate 충돌 여부 확인 |

Airflow 문서는 최신 버전을 가리킨다. 실제 코드에 적용할 때는 운영 서버의 `airflow version`과 같은 버전의 문서를 선택한다.

## 3. 실제 PostgreSQL 통합 테스트

### 추천 자료

- [Testcontainers Python](https://testcontainers-python.readthedocs.io/en/latest/)
- [Docker 공식 Testcontainers Python 실습](https://docs.docker.com/guides/testcontainers-python-getting-started/)
- [pytest fixture](https://docs.pytest.org/en/stable/how-to/fixtures.html)
- [pytest parameterize](https://docs.pytest.org/en/stable/how-to/parametrize.html)
- [GitHub Actions Python 테스트](https://docs.github.com/en/actions/tutorials/build-and-test-code/python)
- [Python pyproject.toml 작성법](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/)

### 먼저 만들 테스트

1. PostgreSQL 컨테이너에 실제 parent table과 날짜 파티션을 생성한다.
2. 작은 RAW 원천 데이터를 넣고 parser와 `COPY`를 거쳐 parsed 결과를 검증한다.
3. 지정한 청크에서 예외를 발생시킨 뒤 트랜잭션이 어디까지 반영됐는지 확인한다.
4. 같은 날짜를 재실행하고 중복·누락 없이 결과가 동일한지 확인한다.
5. RAW와 EUV에 동일한 재적재 판단 조건을 매개변수화해 검증한다.

FakeDB 단위 테스트는 분기 검증에 유지하고, SQL 문법, 파티션, `COPY`, commit/rollback 동작은 실DB 통합 테스트가 담당하도록 나눈다.

## 4. 성능 분석

### 추천 자료

- [py-spy](https://github.com/benfred/py-spy)
- [Python cProfile](https://docs.python.org/3/library/profile.html)
- [Python tracemalloc](https://docs.python.org/3/library/tracemalloc.html)
- [Python concurrent.futures](https://docs.python.org/3/library/concurrent.futures.html)
- [Brendan Gregg의 USE Method](https://www.brendangregg.com/Articles/The_USE_Method.pdf)
- [Brendan Gregg의 Linux Performance 자료](https://www.brendangregg.com/linuxperf.html)

### 분석 순서

1. 전체 시간을 fetch, parse, insert, summary로 나눠 기록한다.
2. `py-spy`로 Python CPU 사용 구간과 DB 응답 대기 구간을 구분한다.
3. `cProfile`로 parser 함수별 누적시간과 호출 횟수를 확인한다.
4. 메모리가 계속 증가하면 `tracemalloc` snapshot을 청크 전후로 비교한다.
5. 서버에서는 CPU 사용률, run queue, 메모리·swap, 디스크 대기시간, 네트워크 에러를 확인한다.
6. worker 수와 chunk size는 한 번에 하나씩만 변경하고 같은 데이터로 비교한다.

멀티스레드나 멀티프로세스는 병목이 Python CPU인지 DB I/O인지 확인한 뒤 적용한다. worker 수만 늘리고 DB가 포화되면 처리시간은 줄지 않고 경합과 메모리 사용량만 증가할 수 있다.

## 5. 코드 구조와 의사결정

### 추천 자료

- [Cosmic Python Repository Pattern](https://www.cosmicpython.com/book/chapter_02_repository)
- [Cosmic Python Unit of Work Pattern](https://www.cosmicpython.com/book/chapter_06_uow.html)
- [Architecture Decision Records](https://adr.github.io/)
- [Martin Fowler의 Architecture Decision Record](https://martinfowler.com/bliki/ArchitectureDecisionRecord.html)

### 프로젝트에 적용할 기준

- Repository는 SQL과 영속성 처리를 담당한다.
- Processor는 날짜, 청크, 파싱·적재 순서와 같은 처리 흐름을 담당한다.
- 여러 Repository 작업을 하나의 원자적 작업으로 묶어야 할 때만 Unit of Work를 검토한다.
- 한 줄을 줄이기 위한 추상화보다 실패 범위와 테스트 가능성을 명확히 만드는 추상화를 우선한다.
- 성능 구조를 변경할 때는 선택 이유, 대안, 실측 결과와 되돌릴 조건을 ADR로 남긴다.

우선 기록할 ADR 후보는 다음과 같다.

1. 적재 worker를 1개로 제한한 이유
2. 원천·대상 count가 다를 때만 `DISTINCT`를 실행하는 이유
3. 청크별 commit과 날짜 단위 commit 중 어떤 방식을 선택했는지
4. 부분 적재 노출을 막기 위해 임시 파티션 교체를 도입할지

## 권장 학습 순서

| 단계 | 학습 및 실습 | 결과물 |
| --- | --- | --- |
| 1 | `EXPLAIN`, partition pruning, `pg_stat_statements` | 실제 쿼리 실행계획 분석 문서 |
| 2 | Airflow 멱등성 및 실패 복구 | 실패 지점별 재실행 테스트 표 |
| 3 | Testcontainers와 pytest | 실제 PostgreSQL 통합 테스트 |
| 4 | `py-spy`, `cProfile`, USE Method | flame graph와 병목 분석 결과 |
| 5 | Repository, Unit of Work, ADR | 트랜잭션 경계 ADR |

가장 먼저 수행할 과제는 RAW 하루치 조회 실행계획 분석과 강제 실패 통합 테스트다. 이 두 결과가 있어야 다음 최적화가 parser, DB query, `COPY`, transaction 중 어디를 대상으로 해야 하는지 근거를 갖고 결정할 수 있다.
