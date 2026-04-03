from __future__ import annotations

import re
from datetime import datetime, timedelta
from pathlib import Path


FILE_NAME_PATTERN = re.compile(r"^(?P<prefix>.+)_(?P<date>\d{8})_(?P<time>\d{6})$")
MAX_MATCH_DIFF = timedelta(minutes=5)


def extract_info(path) -> tuple[str, datetime]:
    stem = Path(path).stem
    match = FILE_NAME_PATTERN.fullmatch(stem)
    if not match:
        raise ValueError(f"지원하지 않는 파일명 형식: {path}")
    prefix = match.group("prefix")
    timestamp = datetime.strptime(
        f"{match.group('date')}{match.group('time')}",
        "%Y%m%d%H%M%S",
    )
    return prefix, timestamp


def find_best_text_match(image_remote_path, text_paths) -> str | None:
    image_prefix, image_ts = extract_info(image_remote_path)
    best_path = None
    best_diff = None

    for text_path in text_paths:
        try:
            text_prefix, text_ts = extract_info(text_path)
        except ValueError:
            continue

        if text_prefix != image_prefix:
            continue
        if text_ts < image_ts:
            continue

        diff = text_ts - image_ts
        if diff > MAX_MATCH_DIFF:
            continue

        if best_diff is None or diff < best_diff:
            best_path = text_path
            best_diff = diff

    return best_path
