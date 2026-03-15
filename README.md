# PythonStudy

## 16-bit TIFF 스토리지 압축 작업

`test.py`의 `compress_raw_image_for_storage()`는 대용량 원본 이미지를 스토리지 보관용 TIFF로 다시 저장하기 위한 함수입니다.

이번 작업에서 다음 조건을 만족하도록 구현을 정리했습니다.

- 16-bit depth 유지
- Alpha 채널 유지
- TIFF 무손실 압축 사용
- 해상도 축소 지원

## 변경 사항

기존 Pillow 중심 처리 대신 OpenCV 기반으로 `compress_raw_image_for_storage()`를 구성했습니다.

- 입력 로드: `cv2.IMREAD_UNCHANGED` / `cv2.imdecode(..., cv2.IMREAD_UNCHANGED)`
- 리사이즈: 축소에 적합한 `cv2.INTER_AREA`
- 저장: TIFF 전용 출력과 `LZW` 또는 `DEFLATE` 무손실 압축

## 왜 OpenCV를 사용했는가

16-bit 이미지, 특히 Alpha 채널이 포함된 다채널 원본을 다룰 때 Pillow만으로는 기대한 형태를 안정적으로 유지하기 어렵습니다. 현재 구현은 OpenCV로 원본 채널 구조를 그대로 읽고, 축소 후 TIFF 무손실 압축으로 저장하는 흐름을 사용합니다.

## 사용 방법

```python
from test import compress_raw_image_for_storage

compress_raw_image_for_storage(
    "raw_equipment_output.tif",
    "compressed_storage.tif",
    scale_factor=0.5,
    compression_type="LZW",
)

compress_raw_image_for_storage(
    b"...raw tiff bytes...",
    "compressed_storage_deflate.tiff",
    scale_factor=0.5,
    compression_type="DEFLATE",
)
```

CLI로 바로 실행할 수도 있습니다.

```bash
python3 test.py raw_equipment_output.tif compressed_storage.tif --scale-factor 0.5 --compression-type LZW
```

## 파라미터

- `input_data`: `bytes`, `str`, `Path`
- `output_path`: `.tif` 또는 `.tiff` 경로만 허용
- `scale_factor`: 0보다 큰 축소 비율
- `compression_type`: `"LZW"` 또는 `"DEFLATE"`

## 주의 사항

- 출력은 TIFF만 지원합니다. PNG/JPEG 등 다른 포맷으로는 저장하지 않습니다.
- OpenCV가 설치되어 있지 않으면 함수 호출 시 예외가 발생합니다.
- NumPy와 Pillow도 함께 필요합니다.

## 의존성

현재 프로젝트 의존성은 `pyproject.toml`에 선언되어 있습니다.

- `numpy`
- `opencv-python`
- `pillow`

설치 예시:

```bash
pip install -e .
```

또는

```bash
pip install numpy opencv-python pillow
```
