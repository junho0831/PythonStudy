from __future__ import annotations

import logging
from pathlib import Path, PurePosixPath

import cv2
import numpy as np
from sqlalchemy.engine import Engine

from remote_batch.infra.db import acquire_processing_slot, mark_history_done, mark_history_fail
from remote_batch.infra.ssh import read_remote_binary_file, write_remote_binary_file

LOGGER = logging.getLogger("remote_batch")


def _build_output_path(file_path: str, output_base_dir: str, *, is_remote: bool) -> str:
    path_cls = PurePosixPath if is_remote else Path
    input_path = path_cls(file_path)
    dated_dir = input_path.parent.name
    return str(path_cls(output_base_dir) / dated_dir / f"{input_path.stem}.png")


def _convert_tif_bytes_to_png_bytes(raw_bytes: bytes, scale_percent: int) -> bytes:
    tif_buffer = np.frombuffer(raw_bytes, dtype=np.uint8)
    image = cv2.imdecode(tif_buffer, cv2.IMREAD_UNCHANGED)
    if image is None:
        raise ValueError("TIF 이미지를 읽지 못했습니다.")
    height, width = image.shape[:2]
    new_width = max(1, round(width * scale_percent / 100))
    new_height = max(1, round(height * scale_percent / 100))
    resized = cv2.resize(image, (new_width, new_height), interpolation=cv2.INTER_AREA)
    success, png_buffer = cv2.imencode(".png", resized, [cv2.IMWRITE_PNG_COMPRESSION, 9])
    if not success:
        raise OSError("PNG 바이너리 인코딩 실패")
    return png_buffer.tobytes()


def _convert_local_tif_to_png(input_path: str, output_path: str, scale_percent: int) -> None:
    png_bytes = _convert_tif_bytes_to_png_bytes(Path(input_path).read_bytes(), scale_percent)
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    Path(output_path).write_bytes(png_bytes)


def _convert_remote_tif_to_png(sftp, input_path: str, output_path: str, scale_percent: int) -> None:
    png_bytes = _convert_tif_bytes_to_png_bytes(
        read_remote_binary_file(sftp, input_path),
        scale_percent,
    )
    write_remote_binary_file(sftp, output_path, png_bytes)


def process_rubp_file(
    *,
    engine: Engine,
    remote_file,
    processing_timeout_minutes: int,
    output_base_dir: str,
    scale_percent: int,
    sftp=None,
) -> None:
    history_id = acquire_processing_slot(engine, remote_file, processing_timeout_minutes)
    if history_id is None:
        LOGGER.info(
            "Rubp 처리 이력에 DONE 또는 최근 PROCESSING 상태가 있어 skip: %s",
            remote_file.file_path,
        )
        return
    output_path = _build_output_path(
        remote_file.file_path,
        output_base_dir,
        is_remote=sftp is not None,
    )
    try:
        if sftp is None:
            _convert_local_tif_to_png(remote_file.file_path, output_path, scale_percent)
        else:
            _convert_remote_tif_to_png(sftp, remote_file.file_path, output_path, scale_percent)
        with engine.begin() as conn:
            mark_history_done(conn, history_id)
        LOGGER.info("Rubp tif 처리 완료: %s -> %s", remote_file.file_path, output_path)
    except Exception as exc:
        LOGGER.exception("Rubp tif 처리 실패: %s", remote_file.file_path)
        with engine.begin() as conn:
            mark_history_fail(conn, history_id, exc)
