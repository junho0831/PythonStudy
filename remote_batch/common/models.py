from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class RemoteFile:
    source_type: str
    file_name: str
    file_path: str
    file_datetime: datetime
