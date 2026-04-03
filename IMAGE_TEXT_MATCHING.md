# 이미지-텍스트 매칭 설계

## 개요

이 문서는 `RUPI` 이미지 파일과 `RUBI` 텍스트 파일을 파일명/생성 순서 기반으로 매칭하는 설계를 정리한 문서다.

현재 저장소의 `RUPI` 경로에는 이 매칭 로직이 반영돼 있다. 이 문서는 현재 구현과 설계 의도를 함께 설명하는 문서다.

## 배경

이미지 파일과 텍스트 파일의 파일명은 아래 규칙을 따른다.

```text
이름명_날짜_시간
```

예:

```text
ixsbscs0v3_20260329_024000.tif
ixsbscs0v3_20260329_024500.txt
```

## 핵심 조건

생성 순서는 다음으로 고정한다.

- 이미지가 먼저 생성된다.
- 텍스트가 나중에 생성된다.

매칭 조건은 다음으로 고정한다.

1. `prefix`가 같아야 한다.
2. `text_ts >= image_ts` 여야 한다.
3. 시간 차이는 `5분 이하`여야 한다.
4. 후보가 여러 개면 `가장 가까운 텍스트`를 선택한다.

수식으로 쓰면 다음과 같다.

```text
min(text_ts - image_ts)
where text_ts >= image_ts
and (text_ts - image_ts) <= 5분
```

## 경로 처리 원칙

루트 경로 구조는 다음을 전제로 한다.

- 이미지: `.../RUIP/...`
- 텍스트: `.../RUBI/...`

즉 중간 디렉토리 이름만 다르고, 날짜 디렉토리와 파일명 규칙은 동일하다고 본다.

이미지 경로를 텍스트 디렉토리 경로로 바꾸는 기준 함수는 아래와 같다.

```python
def image_to_text_dir(image_path):
    parts = ["RUBI" if x == "RUIP" else x for x in Path(image_path).parts]
    return Path(*parts).parent
```

## 기준 코드 예시

```python
from pathlib import Path
from datetime import datetime, timedelta


def extract_info(path):
    name = Path(path).stem
    prefix, date, time = name.split("_")
    ts = datetime.strptime(date + time, "%Y%m%d%H%M%S")
    return prefix, ts


def match(image_path):
    img_prefix, img_ts = extract_info(image_path)

    text_dir = image_to_text_dir(image_path)
    text_paths = list(text_dir.glob("*.txt"))

    best = None
    best_diff = None

    for txt_path in text_paths:
        txt_prefix, txt_ts = extract_info(txt_path)

        if txt_prefix != img_prefix:
            continue

        if txt_ts >= img_ts:
            diff = txt_ts - img_ts

            if diff <= timedelta(minutes=5):
                if best_diff is None or diff < best_diff:
                    best = txt_path
                    best_diff = diff

    return best
```

## 처리 전략

문제는 이미지와 텍스트 생성 순서가 항상 깔끔하게 맞물리지 않을 수 있다는 점이다.

- 이미지 처리 시점에 아직 텍스트가 없을 수 있다.
- 배치가 도는 중 텍스트가 뒤늦게 추가될 수 있다.

이 문서의 설계 기준은 다음과 같다.

1. 이미지 파일은 먼저 DB에 insert 한다.
2. insert 후 바로 매칭을 시도한다.
3. 매칭되는 텍스트가 없으면 방금 insert 한 이미지를 delete 한다.
4. 매칭되는 텍스트가 있으면 이미지 변환까지 먼저 준비한다.
5. 준비된 이미지들은 마지막 업로드 단계에서 한 번에 업로드한다.
6. 업로드 성공 항목만 최종 결과 경로를 update 한다.

예시 흐름:

```python
image_seq = insert_image(...)

matched_text = match(image_path)

if not matched_text:
    delete_image(image_seq)
    return

process_image(...)
queue_upload(...)
finalize_upload(...)
```

## 설계 의도

이 전략의 의도는 다음과 같다.

- 별도 상태값(`PENDING`)을 두지 않는다.
- 유효한 매칭이 없는 이미지는 남기지 않는다.
- 최종적으로 유효한 데이터만 DB에 남긴다.
- 업로드 실패 시에는 row 를 남기고 로컬 PNG를 재업로드 캐시로 유지한다.

즉 전략은 아래 한 줄로 요약된다.

```text
무조건 insert -> 매칭 실패 시 delete
```

## 알고리즘

이 매칭은 전형적인 `Lower Bound / Ceiling Search` 문제로 볼 수 있다.

의미:

- `image_ts` 이후에 생성된 텍스트 중
- 조건 범위(`<= 5분`) 안에 있고
- 가장 가까운 텍스트를 찾는다

현재 가정된 데이터 규모에서는 단순 순회로 충분하다.

- 텍스트 후보 약 `190개`
- 이미지 `1개` 기준 비교

즉 현재 기준으로 `O(n)` 순회가 가장 단순하고 충분히 빠르다.

## DB 설계 관련 메모

- 복합키 인덱스를 쓰는 경우 앞 컬럼부터 조건이 걸려야 인덱스를 잘 탄다.
- 단일 컬럼으로 유니크 식별이 가능하면 복합키가 꼭 필요하지는 않다.
- 이 매칭은 단순 조인보다 유추 규칙이 섞여 있으므로, 최종 매칭 판단은 DB보다는 애플리케이션 코드에서 하는 쪽이 더 적합하다.

## 구현 시 권장 역할 분리

구현 시에는 아래 역할 분리를 권장한다.

- 파일 후보 조회: DB 또는 파일 목록
- 후보 축소: 날짜, prefix 기준 1차 필터
- 최종 매칭 판단: 파이썬 애플리케이션 로직
- 결과 저장: 매칭된 image/text 관계만 DB update

## 최종 요약

한 줄 요약:

```text
이미지가 먼저 생성되고 텍스트가 나중에 생성된다는 전제를 두고,
이미지 기준으로 5분 이내에서 가장 가까운 텍스트를 매칭하며,
매칭 실패 시 이미지는 삭제하고 매칭 성공 시에만 후속 처리한다.
```
