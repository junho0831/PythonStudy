# 2026-04-13 Airflow 학습/환경 세팅 업무일지

## 오늘 작업

### 1. Airflow 개발환경 세팅

- PyCharm에서 `.venv` 가상환경을 사용하는 방식으로 정리했다.
- Airflow 2.6.3 설치 방식과 주의점을 확인했다.
- `requirements.txt` 기반 설치 흐름을 다시 확인했다.
- PyCharm에서 사용하는 Interpreter를 `.venv`와 맞춰야 한다는 점을 정리했다.

### 2. IDE import / 인터프리터 인식 점검

- PyCharm에서 `airflow` import 빨간줄이 나는 원인을 확인했다.
- 설치된 가상환경과 PyCharm Interpreter가 다를 때 IDE 인식 문제가 생길 수 있다는 점을 확인했다.
- 캐시 이슈가 있을 때 `Invalidate Caches / Restart`로 점검하는 방법을 정리했다.

### 3. DAG 의존성 구문 확인

- Airflow DAG에서 `>>` 연산자가 task 실행 순서를 정의한다는 점을 다시 확인했다.
- task dependency 구문이 잘못 이어진 경우 문법 오류나 흐름 오해가 생길 수 있다는 점을 점검했다.
- 검토한 예시 흐름은 다음과 같다.

```python
start >> ssh_operators["parse_qddv_sensor"] \
      >> ssh_operators["parse_dpi_sensor"] \
      >> ssh_operators["parse_kpi_sensor"] \
      >> end
```

- 위 예시는 “검토한 의존성 패턴”이며, 현재 저장소의 실제 DAG 구조와 동일하다고 보지는 않는다.

### 4. 스케줄 및 동시 실행 제어 확인

- 1시간 주기 실행 시 이전 실행이 끝나지 않은 경우의 동작을 개념적으로 확인했다.
- `max_active_runs=1`로 중복 실행을 막는 방식을 정리했다.
- Airflow에서 스케줄링만 보는 것이 아니라 실행 겹침 제어까지 같이 봐야 한다는 점을 정리했다.

## 핵심 확인 사항

- Airflow 개발환경에서는 가상환경과 IDE Interpreter가 맞아야 import 인식 문제가 줄어든다.
- DAG에서 `>>`는 task 실행 순서를 정의하는 핵심 구문이다.
- 1시간 주기 배치에서는 `max_active_runs=1` 같은 실행 겹침 제어가 중요하다.

## 실행 제어 포인트

- 스케줄 주기만 정하는 것으로는 운영이 끝나지 않는다.
- 이전 실행이 끝나지 않았을 때 다음 실행을 어떻게 다룰지 함께 정해야 한다.
- DAG 문법 오류보다 더 중요한 것은 실제 실행 순서가 의도대로 연결돼 있는지 확인하는 것이다.

## 한 줄 요약

Airflow 개발환경 세팅, IDE 인식 이슈, DAG 의존성 구문, 그리고 `max_active_runs=1` 중심의 실행 제어 개념을 정리했다.

> 메모: 이 문서는 현재 repo 구현 기록이 아니라 Airflow 학습/환경 세팅 성격의 별도 업무일지다.
