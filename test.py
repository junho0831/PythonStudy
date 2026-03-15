from __future__ import annotations

import argparse
import io
import os
from pathlib import Path
from typing import TYPE_CHECKING, Sequence, Union

if TYPE_CHECKING:
    import cv2
    import numpy as np
    from PIL import Image


def _require_cv2() -> "cv2":
    try:
        import cv2
    except ImportError as exc:
        raise ImportError(
            "OpenCV(cv2)가 필요합니다. `pip install opencv-python`으로 설치하세요."
        ) from exc
    return cv2


def _require_pillow() -> "Image":
    try:
        from PIL import Image
    except ImportError as exc:
        raise ImportError(
            "Pillow가 필요합니다. `pip install pillow`로 설치하세요."
        ) from exc
    return Image


def _require_numpy() -> "np":
    try:
        import numpy as np
    except ImportError as exc:
        raise ImportError(
            "NumPy가 필요합니다. `pip install numpy`로 설치하세요."
        ) from exc
    return np


def load_image(input_data: Union[bytes, str, Path]) -> "Image.Image":
    """바이트 데이터 또는 파일 경로에서 이미지를 로드합니다. (PIL 전용)"""
    Image = _require_pillow()
    if isinstance(input_data, bytes):
        return Image.open(io.BytesIO(input_data))
    elif isinstance(input_data, (str, Path)):
        if not os.path.exists(input_data):
            raise FileNotFoundError(f"파일을 찾을 수 없습니다: {input_data}")
        return Image.open(input_data)
    else:
        raise ValueError("지원하지 않는 입력 형식입니다. bytes, str, Path 중 하나여야 합니다.")


def compress_raw_image_for_storage(input_data: Union[bytes, str, Path], 
                                   output_path: Union[str, Path], 
                                   scale_factor: float = 0.5,
                                   compression_type: str = "LZW") -> None:
    """
    서버로 들어온 20MB 등 대용량 16-bit 원본 이미지의 해상도를 줄이고 
    TIFF 무손실 압축(LZW 또는 Deflate)을 적용하여 저장공간을 최적화합니다.
    16-bit 심도(Depth)와 Alpha 채널은 그대로 유지됩니다.
    (주의: 16-bit 알파 채널을 완벽하게 유지하기 위해 OpenCV를 사용합니다.)
    
    :param input_data: 원본 이미지 (bytes 또는 파일 경로)
    :param output_path: 압축 후 저장할 TIFF 파일 경로 (.tif 또는 .tiff)
    :param scale_factor: 축소할 비율 (예: 0.5는 50% 축소)
    :param compression_type: "LZW" 또는 "DEFLATE"
    """
    cv2 = _require_cv2()
    np = _require_numpy()
    output_path = Path(output_path)

    if scale_factor <= 0:
        raise ValueError("scale_factor는 0보다 커야 합니다.")

    if output_path.suffix.lower() not in {".tif", ".tiff"}:
        raise ValueError(
            "output_path는 .tif 또는 .tiff여야 합니다. "
            "TIFF LZW/DEFLATE 무손실 압축만 지원합니다."
        )

    compression_key = compression_type.upper()
    compression_map = {
        "LZW": getattr(cv2, "IMWRITE_TIFF_COMPRESSION_LZW", 5),
        "DEFLATE": getattr(cv2, "IMWRITE_TIFF_COMPRESSION_DEFLATE", 8),
    }
    if compression_key not in compression_map:
        raise ValueError("compression_type는 'LZW' 또는 'DEFLATE'여야 합니다.")

    # 1. OpenCV로 이미지 읽기 (16-bit 및 Alpha 채널 유지를 위해 IMREAD_UNCHANGED 사용)
    if isinstance(input_data, bytes):
        np_arr = np.frombuffer(input_data, np.uint8)
        img_array = cv2.imdecode(np_arr, cv2.IMREAD_UNCHANGED)
    elif isinstance(input_data, (str, Path)):
        if not os.path.exists(input_data):
            raise FileNotFoundError(f"파일을 찾을 수 없습니다: {input_data}")
        img_array = cv2.imread(str(input_data), cv2.IMREAD_UNCHANGED)
    else:
        raise ValueError("지원하지 않는 입력 형식입니다. bytes, str, Path 중 하나여야 합니다.")

    if img_array is None:
        raise ValueError("이미지를 디코딩할 수 없습니다.")

    # 2. 해상도 축소 (비율에 맞게, 보간법은 축소에 적합한 INTER_AREA 사용)
    height, width = img_array.shape[:2]
    new_width = max(1, round(width * scale_factor))
    new_height = max(1, round(height * scale_factor))
    resized_array = cv2.resize(img_array, (new_width, new_height), interpolation=cv2.INTER_AREA)
    
    # 3. TIFF 무손실 압축 설정
    compression_params = [cv2.IMWRITE_TIFF_COMPRESSION, compression_map[compression_key]]
    
    # 4. 저장
    output_path.parent.mkdir(parents=True, exist_ok=True)
    write_ok = cv2.imwrite(str(output_path), resized_array, compression_params)
    if not write_ok:
        raise OSError(f"이미지를 저장하지 못했습니다: {output_path}")


def process_semiconductor_blob(input_data: Union[bytes, str, Path], 
                               max_size: tuple = (1024, 1024), 
                               quality: int = 80,
                               alpha_weight: float = 1.0) -> bytes:
    """
    16-bit TIFF 또는 BLOB 데이터를 받아 밝기를 최적화하고 가벼운 WebP BLOB으로 반환합니다.
    밝기 정보를 투명도 채널로 사용하여 결함은 흰색으로, 배경은 투명하게 만듭니다.
    
    :param input_data: 원본 이미지 (bytes 또는 파일 경로)
    :param max_size: 축소할 해상도
    :param quality: WebP 압축 품질
    :param alpha_weight: 투명도 조절 비율 (0.0 ~ 1.0+)
    """
    Image = _require_pillow()
    np = _require_numpy()
    with load_image(input_data) as img:
        img_array = np.array(img)
        
        # 2. 16-bit 데이터인 경우, 정규화 후 밝기를 Alpha 채널로 사용
        if img_array.dtype in (np.uint16, np.int32):
            min_val = img_array.min()
            max_val = img_array.max()
            
            # 정규화된 8-bit 배열 생성
            if max_val > min_val:
                normalized = (img_array - min_val) / (max_val - min_val) * 255.0
                img_array_8bit = normalized.astype(np.uint8)
            else:
                img_array_8bit = img_array.astype(np.uint8)
            
            # 밝기 값 자체를 투명도(Alpha) 채널로 사용 + 투명도 가중치 조절
            alpha_channel = np.clip(img_array_8bit * alpha_weight, 0, 255).astype(np.uint8)

            # RGB 채널은 모두 흰색(255)으로 설정
            rgb_channels = np.full((img_array_8bit.shape[0], img_array_8bit.shape[1], 3), 255, dtype=np.uint8)

            # RGB와 Alpha를 합쳐서 RGBA 이미지 생성
            rgba_array = np.dstack((rgb_channels, alpha_channel))
            img = Image.fromarray(rgba_array, 'RGBA')

        else:
            # 8-bit 이미지 등 다른 경우는 RGBA로 변환
            img = img.convert("RGBA")
            if alpha_weight != 1.0:
                alpha = img.split()[3]
                alpha = alpha.point(lambda p: int(min(p * alpha_weight, 255)))
                img.putalpha(alpha)
        
        # 3. 해상도 리사이징 (WebP용 렌더링 최적화)
        img.thumbnail(max_size, Image.Resampling.LANCZOS)
        
        # 4. WebP로 압축하여 새로운 메모리 버퍼에 저장
        out_buffer = io.BytesIO()
        img.save(out_buffer, format="WebP", quality=quality)
        
        return out_buffer.getvalue()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="16-bit TIFF 이미지를 축소하고 무손실 TIFF 압축으로 저장합니다."
    )
    parser.add_argument("input_path", help="원본 TIFF 파일 경로")
    parser.add_argument("output_path", help="저장할 TIFF 파일 경로")
    parser.add_argument(
        "--scale-factor",
        type=float,
        default=0.5,
        help="해상도 축소 비율 (기본값: 0.5)",
    )
    parser.add_argument(
        "--compression-type",
        choices=("LZW", "DEFLATE"),
        default="LZW",
        help="TIFF 무손실 압축 방식 (기본값: LZW)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    compress_raw_image_for_storage(
        input_data=args.input_path,
        output_path=args.output_path,
        scale_factor=args.scale_factor,
        compression_type=args.compression_type,
    )
    print(f"완료: {args.output_path}")
    return 0

# 사용 예시 1: 서버에 대용량 이미지가 들어왔을 때 스토리지 저장용 (용량 최적화)
# compress_raw_image_for_storage("raw_equipment_output.tif", "compressed_storage.tif", scale_factor=0.5, compression_type="LZW")
# compress_raw_image_for_storage(blob_bytes, "compressed_storage_deflate.tiff", scale_factor=0.5, compression_type="DEFLATE")

# 사용 예시 2: 프론트엔드 서빙용 투명 WebP 변환 (입력: 파일 경로 또는 bytes)
# webp_blob = process_semiconductor_blob("compressed_storage.tif", alpha_weight=1.2)
# return Response(webp_blob, mimetype='image/webp')


if __name__ == "__main__":
    raise SystemExit(main())
