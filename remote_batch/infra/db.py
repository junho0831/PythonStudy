from __future__ import annotations

import json
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection, Engine

from remote_batch.common.constants import LOCAL_TZ, STATUS_DONE, STATUS_FAIL, STATUS_PROCESSING
from remote_batch.common.models import RemoteFile
from remote_batch.infra.crud import CrudClient


def connect_db(dsn: str) -> Engine:
    return create_engine(dsn, future=True)


def acquire_processing_slot(
    engine: Engine,
    remote_file: RemoteFile,
    processing_timeout_minutes: int,
) -> int | None:
    stale_before = datetime.now(tz=LOCAL_TZ) - timedelta(minutes=processing_timeout_minutes)
    with engine.begin() as conn:
        crud = CrudClient(conn)
        row = crud.fetch_one(
            """
            select id, status, updated_at
            from file_processing_history
            where source_type = :source_type and file_name = :file_name
            for update
            """,
            {
                "source_type": remote_file.source_type,
                "file_name": remote_file.file_name,
            },
        )
        if row is None:
            history_id = crud.insert(
                "file_processing_history",
                {
                    "source_type": remote_file.source_type,
                    "file_name": remote_file.file_name,
                    "file_path": remote_file.file_path,
                    "file_datetime": remote_file.file_datetime,
                    "status": STATUS_PROCESSING,
                },
                returning="id",
            )
            return int(history_id)

        history_id, status_value, updated_at = row
        if status_value == STATUS_DONE:
            return None
        if status_value == STATUS_PROCESSING and updated_at and updated_at >= stale_before:
            return None

        crud.update(
            "file_processing_history",
            {
                "file_path": remote_file.file_path,
                "file_datetime": remote_file.file_datetime,
                "status": STATUS_PROCESSING,
                "error_message": None,
                "processed_at": None,
            },
            where_clause="id = :history_id",
            where_params={"history_id": history_id},
            expression_values={"updated_at": "now()"},
        )
        return int(history_id)


def begin_transaction(engine: Engine):
    return engine.begin()


def is_missing_value(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, (list, dict, tuple, set)):
        return False
    return bool(pd.isna(value))


def normalize_parsed_records(parsed_records: list[dict]) -> pd.DataFrame:
    frame = pd.DataFrame(parsed_records)
    if frame.empty:
        return pd.DataFrame(columns=["record_index", "parsed_payload"])
    frame.insert(0, "record_index", range(len(frame)))
    frame["parsed_payload"] = frame.apply(
        lambda row: {
            key: value
            for key, value in row.items()
            if key != "record_index" and not is_missing_value(value)
        },
        axis=1,
    )
    return frame[["record_index", "parsed_payload"]]


def insert_parsed_data(
    conn: Connection,
    history_id: int,
    parsed_records: list[dict],
) -> None:
    crud = CrudClient(conn)
    frame = normalize_parsed_records(parsed_records)
    for row in frame.to_dict(orient="records"):
        crud.upsert(
            "rubi_parsed_data",
            {
                "history_id": history_id,
                "record_index": int(row["record_index"]),
                "parsed_payload": json.dumps(row["parsed_payload"], ensure_ascii=False),
            },
            conflict_columns=["history_id", "record_index"],
            update_columns=["parsed_payload"],
            casts={"parsed_payload": "jsonb"},
            update_expression_values={"updated_at": "now()"},
        )
    crud.delete(
        "rubi_parsed_data",
        where_clause="history_id = :history_id and record_index >= :record_count",
        params={"history_id": history_id, "record_count": len(frame)},
    )


def mark_history_done(conn: Connection, history_id: int) -> None:
    CrudClient(conn).update(
        "file_processing_history",
        {
            "status": STATUS_DONE,
            "error_message": None,
        },
        where_clause="id = :history_id",
        where_params={"history_id": history_id},
        expression_values={
            "processed_at": "now()",
            "updated_at": "now()",
        },
    )


def mark_history_fail(
    conn: Connection,
    history_id: int,
    exc: Exception,
) -> None:
    CrudClient(conn).update(
        "file_processing_history",
        {
            "status": STATUS_FAIL,
            "error_message": str(exc)[:4000],
        },
        where_clause="id = :history_id",
        where_params={"history_id": history_id},
        expression_values={"updated_at": "now()"},
    )
