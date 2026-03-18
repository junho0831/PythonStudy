from __future__ import annotations

from collections.abc import Callable
import logging

from sqlalchemy.engine import Engine

from remote_batch.infra.db import acquire_processing_slot, begin_transaction, mark_history_done, mark_history_fail

LOGGER = logging.getLogger("remote_batch")


def process_tif_stub(
    ssh_client,
    remote_file,
) -> None:
    _ = ssh_client
    LOGGER.info(
        "Rubp tif stub. 향후 서버 내 명령 실행으로 확장 예정: %s (%s)",
        remote_file.file_path,
        remote_file.file_datetime.isoformat(),
    )


def process_rubp_file(
    *,
    engine: Engine,
    remote_file,
    processing_timeout_minutes: int,
    process_tif: Callable[[object, object], None],
    ssh_client=None,
) -> None:
    history_id = acquire_processing_slot(engine, remote_file, processing_timeout_minutes)
    if history_id is None:
        LOGGER.info(
            "Rubp 처리 이력에 DONE 또는 최근 PROCESSING 상태가 있어 skip: %s",
            remote_file.file_path,
        )
        return
    try:
        process_tif(ssh_client, remote_file)
        with begin_transaction(engine) as conn:
            mark_history_done(conn, history_id)
        LOGGER.info("Rubp tif 처리 완료: %s", remote_file.file_path)
    except Exception as exc:
        LOGGER.exception("Rubp tif 처리 실패: %s", remote_file.file_path)
        with begin_transaction(engine) as conn:
            mark_history_fail(conn, history_id, exc)
