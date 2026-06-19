from __future__ import annotations

import sys
from pathlib import Path


_VENDOR_PATH = Path(__file__).resolve().parent / ".vendor"

if _VENDOR_PATH.is_dir():
    vendor = str(_VENDOR_PATH)
    if vendor not in sys.path:
        sys.path.insert(0, vendor)
