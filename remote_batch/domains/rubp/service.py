from __future__ import annotations

import logging
import shlex
from pathlib import Path, PurePosixPath

from sqlalchemy.engine import Engine

from remote_batch.infra.db import acquire_processing_slot, begin_transaction, mark_history_done, mark_history_fail

LOGGER = logging.getLogger("remote_batch")


def _build_output_path(file_path: str, output_base_dir: str, *, is_remote: bool) -> str:
    path_cls = PurePosixPath if is_remote else Path
    input_path = path_cls(file_path)
    dated_dir = input_path.parent.name
    return str(path_cls(output_base_dir) / dated_dir / f"{input_path.stem}.png")


def _convert_local_tif_to_png(input_path: str, output_path: str, scale_percent: int) -> None:
    import cv2

    image = cv2.imread(input_path, cv2.IMREAD_UNCHANGED)
    if image is None:
        raise ValueError(f"TIF 이미지를 읽지 못했습니다: {input_path}")
    height, width = image.shape[:2]
    new_width = max(1, round(width * scale_percent / 100))
    new_height = max(1, round(height * scale_percent / 100))
    resized = cv2.resize(image, (new_width, new_height), interpolation=cv2.INTER_AREA)
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    write_ok = cv2.imwrite(output_path, resized, [cv2.IMWRITE_PNG_COMPRESSION, 9])
    if not write_ok:
        raise OSError(f"PNG 저장 실패: {output_path}")


def _convert_remote_tif_to_png(
    ssh_client,
    input_path: str,
    output_path: str,
    scale_percent: int,
    remote_magick_bin: str,
) -> None:
    output_dir = str(PurePosixPath(output_path).parent)
    quoted_magick = shlex.quote(remote_magick_bin)
    command = (
        f"mkdir -p {shlex.quote(output_dir)} && "
        f"{quoted_magick} {shlex.quote(input_path)} "
        f"-resize {scale_percent}% "
        f"-strip "
        f"-define png:compression-level=9 "
        f"{shlex.quote(output_path)}"
    )
    _stdin, stdout, stderr = ssh_client.exec_command(command)
    exit_code = stdout.channel.recv_exit_status()
    std_out = stdout.read().decode("utf-8", errors="replace").strip()
    std_err = stderr.read().decode("utf-8", errors="replace").strip()
    if exit_code != 0:
        raise RuntimeError(
            f"원격 PNG 변환 실패(exit={exit_code}): {std_err or std_out or command}"
        )
    check_command = f"test -f {shlex.quote(output_path)}"
    _stdin, stdout, stderr = ssh_client.exec_command(check_command)
    exit_code = stdout.channel.recv_exit_status()
    std_err = stderr.read().decode("utf-8", errors="replace").strip()
    if exit_code != 0:
        raise RuntimeError(f"원격 PNG 결과 파일이 없습니다: {output_path} {std_err}".strip())


def process_rubp_file(
    *,
    engine: Engine,
    remote_file,
    processing_timeout_minutes: int,
    output_base_dir: str,
    scale_percent: int,
    remote_magick_bin: str,
    ssh_client=None,
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
        is_remote=ssh_client is not None,
    )
    try:
        if ssh_client is None:
            _convert_local_tif_to_png(remote_file.file_path, output_path, scale_percent)
        else:
            _convert_remote_tif_to_png(
                ssh_client,
                remote_file.file_path,
                output_path,
                scale_percent,
                remote_magick_bin,
            )
        with begin_transaction(engine) as conn:
            mark_history_done(conn, history_id)
        LOGGER.info("Rubp tif 처리 완료: %s -> %s", remote_file.file_path, output_path)
    except Exception as exc:
        LOGGER.exception("Rubp tif 처리 실패: %s", remote_file.file_path)
        with begin_transaction(engine) as conn:
            mark_history_fail(conn, history_id, exc)
