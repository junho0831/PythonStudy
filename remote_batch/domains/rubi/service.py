from __future__ import annotations

from collections.abc import Callable
import logging

from sqlalchemy.engine import Engine

from remote_batch.domains.rubi.parser import parse_text
from remote_batch.infra.db import (
    acquire_processing_slot,
    insert_parsed_data,
    mark_history_done,
    mark_history_fail,
)

LOGGER = logging.getLogger("remote_batch")


def process_rubi_file(
    *,
    engine: Engine,
    remote_file,
    processing_timeout_minutes: int,
    read_text_file: Callable[[str], str],
) -> None:
    history_id = acquire_processing_slot(engine, remote_file, processing_timeout_minutes)
    if history_id is None:
        LOGGER.info(
            "처리 이력에 DONE 또는 최근 PROCESSING 상태가 있어 skip: %s",
            remote_file.file_path,
        )
        return
    try:
        text = read_text_file(remote_file.file_path)
        parsed_records = parse_text(text)
        with engine.begin() as conn:
            insert_parsed_data(conn, history_id, parsed_records)
            mark_history_done(conn, history_id)
        LOGGER.info("Rubi txt 처리 완료: %s (%s건)", remote_file.file_path, len(parsed_records))
    except Exception as exc:
        LOGGER.exception("Rubi txt 처리 실패: %s", remote_file.file_path)
        with engine.begin() as conn:
            mark_history_fail(conn, history_id, exc)
