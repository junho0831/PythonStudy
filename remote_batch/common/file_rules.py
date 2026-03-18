from __future__ import annotations

from datetime import datetime

from remote_batch.common.constants import FILE_DATETIME_PATTERN, LOCAL_TZ


def extract_file_datetime(file_name: str) -> datetime | None:
    match = FILE_DATETIME_PATTERN.search(file_name)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y%m%d_%H%M%S").replace(tzinfo=LOCAL_TZ)
    except ValueError:
        return None
