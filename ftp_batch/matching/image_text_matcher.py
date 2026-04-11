from __future__ import annotations

import re
from datetime import datetime, timedelta
from pathlib import Path

from ftp_batch.common.path_utils import make_rbi_path


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


def find_nearest_image_for_text(text_path, candidate_images) -> str | None:
    text_prefix, text_ts = extract_info(text_path)
    best_path = None
    best_diff = None

    for image_path in candidate_images:
        try:
            image_prefix, image_ts = extract_info(image_path)
        except ValueError:
            continue

        if image_prefix != text_prefix:
            continue
        if image_ts > text_ts:
            continue

        diff = text_ts - image_ts
        if diff > MAX_MATCH_DIFF:
            continue

        if best_diff is None or diff < best_diff:
            best_path = image_path
            best_diff = diff

    return best_path
