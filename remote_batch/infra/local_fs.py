from __future__ import annotations

import logging
from pathlib import Path

from remote_batch.common.file_rules import extract_file_datetime
from remote_batch.common.models import RemoteFile

LOGGER = logging.getLogger("remote_batch")


def list_local_files(
    base_dir: str,
    folder_names: list[str],
    extension: str,
    source_type: str,
) -> list[RemoteFile]:
    files: list[RemoteFile] = []
    for folder_name in folder_names:
        folder_path = Path(base_dir) / folder_name
        if not folder_path.exists():
            LOGGER.info("로컬 폴더가 없어 건너뜁니다: %s", folder_path)
            continue
        if not folder_path.is_dir():
            LOGGER.warning("로컬 경로가 폴더가 아니어서 건너뜁니다: %s", folder_path)
            continue
        for entry in folder_path.iterdir():
            if not entry.is_file() or entry.suffix.lower() != extension.lower():
                continue
            file_datetime = extract_file_datetime(entry.name)
            if file_datetime is None:
                LOGGER.warning("파일명 datetime 파싱 실패로 skip: %s", entry.name)
                continue
            files.append(
                RemoteFile(
                    source_type=source_type,
                    file_name=entry.name,
                    file_path=str(entry),
                    file_datetime=file_datetime,
                )
            )
    return sorted(files, key=lambda item: (item.file_datetime, item.file_name))


def read_local_text_file(file_path: str) -> str:
    raw_bytes = Path(file_path).read_bytes()
    for encoding in ("utf-8", "cp949"):
        try:
            return raw_bytes.decode(encoding)
        except UnicodeDecodeError:
            continue
    LOGGER.warning("utf-8/cp949 디코딩 실패. replace 모드로 진행합니다: %s", file_path)
    return raw_bytes.decode("utf-8", errors="replace")
