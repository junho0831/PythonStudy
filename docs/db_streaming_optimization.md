# Database Streaming Optimization (`stream_results` & `max_row_buffer`)

이 문서는 대량의 데이터베이스 조회 시 드라이버 및 클라이언트 레벨에서 발생할 수 있는 Out Of Memory(OOM) 현상을 방지하기 위해 적용된 스트리밍 최적화 기술에 대해 다룹니다.

## 1. 배경 및 문제점

기존의 `PostgresDB.select_in_chunks()` 메서드는 `fetchmany(chunk_size)`를 호출하여 데이터를 쪼개서 반환(Generator)하는 구조였습니다.

그러나 SQLAlchemy와 PostgreSQL 드라이버(psycopg2)의 기본 동작은 다음과 같습니다.
1. `conn.execute(text(query))`를 호출하는 즉시, 쿼리의 **모든 매칭 데이터**를 데이터베이스 서버로부터 네트워크를 통해 클라이언트로 전송받습니다.
2. 클라이언트(Python 프로세스)의 메모리에 모든 데이터를 적재합니다.
3. 이후 코드에서 `fetchmany`를 호출할 때는 이미 메모리에 올라와 있는 데이터를 그냥 쪼개서 가져오는 것뿐입니다.

이로 인해 쿼리 대상 데이터의 건수가 수백만 건 이상으로 과도하게 많거나 각 행의 로그 본문(`contents`) 크기가 큰 경우, **클라이언트 드라이버 레벨에서 전체 데이터를 올리다 OOM(Out Of Memory)이 발생**하는 구조적 문제가 존재했습니다.

## 2. 최적화 해결 방안

이를 해결하기 위해 **서버사이드 커서(Server-side Cursor)**를 활용한 진정한 스트리밍 조회를 적용했습니다.

### 적용 옵션
- **`stream_results=True`**: SQLAlchemy에게 PostgreSQL의 서버사이드 커서를 사용하도록 명시합니다. 이 옵션이 활성화되면 데이터를 클라이언트에 한 번에 다 받지 않고 DB 서버에 유지한 채, 점진적으로 스트리밍하여 가져옵니다.
- **`max_row_buffer=chunk_size`**: 네트워크 왕복(Roundtrip) 횟수를 조절하기 위해 클라이언트 버퍼 크기를 지정합니다. 한 번에 가져올 버퍼 개수를 배치 처리 단위(`chunk_size`)와 일치시켜 메모리 효율을 극대화합니다.

### 코드 변경 내역
[er_dose/infra/postgres_db.py](file:///er_dose/infra/postgres_db.py)의 `select_in_chunks` 메서드를 다음과 같이 개선했습니다.

```diff
         with self._connect_sqlalchemy().connect() as conn:
-            result = conn.execute(text(query), params or {})
+            stmt = text(query).execution_options(stream_results=True, max_row_buffer=chunk_size)
+            result = conn.execute(stmt, params or {})
             columns = list(result.keys())
 
             while True:
                 rows = result.fetchmany(chunk_size)
```

## 3. 기대 효과
- **메모리 점유 최적화**: 조회 결과의 총 건수에 관계없이, 항상 `chunk_size`만큼의 데이터만 메모리에 적재되므로 대용량 데이터 로딩 시 OOM 위험이 원천 차단됩니다.
- **성능 밸런스**: `max_row_buffer` 설정을 통해 불필요한 네트워크 오버헤드를 막으면서 대량 스트리밍 처리의 처리량(Throughput)을 보장합니다.

## 4. ER Dose RAW 조회 실행계획 점검

ER Dose RAW 조회 시간이 길면 애플리케이션을 수정하기 전에 PostgreSQL 실행계획으로 원천 파티션 조회와 정렬 비용을 확인합니다. 아래 예시는 `2026-08-13` 파티션 기준이므로 점검할 날짜에 맞게 테이블명과 시간 범위를 함께 변경해야 합니다.

### 4.1 예상 실행계획 확인

`EXPLAIN`은 쿼리를 실제로 실행하지 않으므로 먼저 아래 SQL로 접근 방식과 예상 비용을 확인합니다.

```sql
EXPLAIN (COSTS, VERBOSE)
SELECT
    r.eq_name,
    r.code,
    r.code_occur_time,
    r.title,
    r.contents
FROM mbeat.er_data_raw_1_prt_p20260813 r
WHERE r.code_occur_time >= TIMESTAMP '2026-08-13 00:00:00'
  AND r.code_occur_time <  TIMESTAMP '2026-08-14 00:00:00'
  AND r.code IN (
      'DW-3411', 'DW-3425', 'DW-343A', 'DW-343B',
      'LO-0050', 'LO-0061', 'LO-8166', 'LO-8167',
      'KE-9103', 'KE-9104'
  )
  AND r.eq_name IN (
      SELECT eqp.eqp_id
      FROM prism_dev.photo_eqp_info eqp
      WHERE eqp.use_yn = 'Y'
        AND eqp.eqp_model_name LIKE 'NXE%'
  )
ORDER BY r.code_occur_time, r.eq_name, r.er_date, r.er_index;
```

### 4.2 실제 실행시간과 I/O 확인

실제 DB 처리시간을 측정할 때는 위 SQL의 첫 줄을 다음과 같이 변경합니다.

```sql
EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, SUMMARY ON)
```

`EXPLAIN ANALYZE`는 조회 결과를 변경하지 않지만 쿼리를 끝까지 실제 실행합니다. 하루치 데이터가 약 2천만 건이면 운영 DB에 상당한 읽기 및 정렬 부하를 줄 수 있으므로 부하가 낮은 시간에 실행합니다.

### 4.3 확인 항목

| 실행계획 항목 | 확인 내용 |
| --- | --- |
| `Seq Scan` | 조건에 맞는 행이 파티션 대부분이면 정상일 수 있습니다. 일부 행만 필요함에도 전체 스캔한다면 인덱스 후보를 검토합니다. |
| `Rows Removed by Filter` | 읽은 행 중 조건에서 제거된 행이 많을수록 불필요한 스캔 비중이 큽니다. |
| `Sort Method` | `external merge`와 `Disk`가 표시되면 `ORDER BY` 정렬이 메모리를 초과해 임시 디스크를 사용한 것입니다. |
| `Buffers: shared hit/read` | `read`가 크면 실제 디스크 읽기 비중이 높고, `hit`가 크면 캐시된 페이지를 주로 사용한 것입니다. |
| `temp read/written` | 값이 크면 정렬이나 해시 연산이 임시 파일을 많이 사용한 것입니다. |
| `rows`와 `actual rows` | 예상 행 수와 실제 행 수 차이가 크면 통계가 부정확할 수 있으므로 해당 파티션의 `ANALYZE` 상태를 확인합니다. |
| `Execution Time` | DB 내부 실행시간입니다. 배치의 `fetch_seconds`보다 충분히 짧다면 네트워크 전송이나 Python 데이터프레임 변환 비용도 별도로 확인합니다. |

현재 파티션의 인덱스는 다음 SQL로 확인합니다.

```sql
SELECT indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'mbeat'
  AND tablename = 'er_data_raw_1_prt_p20260813';
```

실행계획에서 디스크 정렬이 확인되더라도 바로 인덱스를 추가하지 않습니다. 하루치 대상 행 비율, 인덱스 크기와 쓰기 비용, `ORDER BY` 제거 가능성을 함께 비교한 뒤 결정합니다.

## 5. 참조 문서 (References)
본 최적화를 위해 아래 공식 문서들을 참고 및 인용하였습니다.
1. **SQLAlchemy 공식 문서 - Streaming Results**:
   - [SQLAlchemy Execution Options (`stream_results` & `max_row_buffer`)](https://docs.sqlalchemy.org/en/20/core/connections.html#sqlalchemy.engine.Connection.execution_options.params.stream_results)
2. **PostgreSQL / psycopg2 공식 문서 - Server-side Cursors**:
   - [psycopg2 Server-side Cursors](https://www.psycopg.org/docs/usage.html#server-side-cursors)
3. **프로젝트 연관 코드**:
   - [er_dose/infra/postgres_db.py](file:///home/junho/IdeaProjects/PythonStudy/er_dose/infra/postgres_db.py#L125-L144)
