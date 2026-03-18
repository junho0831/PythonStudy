from __future__ import annotations

import os
import re
from zoneinfo import ZoneInfo

FILE_DATETIME_PATTERN = re.compile(r"(\d{8}_\d{6})")
LOCAL_TZ = ZoneInfo(os.getenv("APP_TIMEZONE", "Asia/Seoul"))
STATUS_DONE = "DONE"
STATUS_FAIL = "FAIL"
STATUS_PROCESSING = "PROCESSING"
SOURCE_RUBI_TXT = "RUBI_TXT"
SOURCE_RUBP_TIF = "RUBP_TIF"
