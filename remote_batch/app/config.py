from __future__ import annotations

import argparse
import logging
import os
from datetime import date, datetime, timedelta
from pathlib import Path

from remote_batch.common.constants import LOCAL_TZ
PROPERTIES_PATH = Path(__file__).with_name("local.properties")


def load_properties(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


LOCAL_PROPERTIES = load_properties(PROPERTIES_PATH)


def get_setting(name: str, env_name: str, default=None):
    env_value = os.getenv(env_name)
    if env_value not in (None, ""):
        return env_value
    local_value = LOCAL_PROPERTIES.get(name, default)
    if local_value not in (None, ""):
        return local_value
    return default


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Rubi txt 원격 배치 프로그램")
    parser.add_argument(
        "--io-mode",
        choices=("auto", "local", "remote"),
        default=get_setting("IO_MODE", "IO_MODE", "auto"),
        help="파일 접근 모드(auto/local/remote). auto는 localhost 설정일 때 local 우선.",
    )
    parser.add_argument("--ssh-host", default=get_setting("SSH_HOST", "SSH_HOST"))
    parser.add_argument("--ssh-port", type=int, default=int(get_setting("SSH_PORT", "SSH_PORT", 22)))
    parser.add_argument("--ssh-username", default=get_setting("SSH_USERNAME", "SSH_USERNAME"))
    parser.add_argument("--ssh-password", default=get_setting("SSH_PASSWORD", "SSH_PASSWORD"))
    parser.add_argument("--ssh-key-file", default=get_setting("SSH_KEY_FILE", "SSH_KEY_FILE"))
    parser.add_argument("--db-dsn", default=get_setting("DB_DSN", "DB_DSN"))
    parser.add_argument("--rubi-base-dir", default=get_setting("RUBI_BASE_DIR", "RUBI_BASE_DIR", "/data/Rubi"))
    parser.add_argument("--rubp-base-dir", default=get_setting("RUBP_BASE_DIR", "RUBP_BASE_DIR", "/data/Rubp"))
    parser.add_argument("--days-back", type=int, default=int(get_setting("DAYS_BACK", "DAYS_BACK", 3)))
    parser.add_argument(
        "--processing-timeout-minutes",
        type=int,
        default=int(get_setting("PROCESSING_TIMEOUT_MINUTES", "PROCESSING_TIMEOUT_MINUTES", 120)),
    )
    parser.add_argument("--log-level", default=get_setting("LOG_LEVEL", "LOG_LEVEL", "INFO"))
    return parser


def validate_args(args: argparse.Namespace) -> None:
    required = {"db_dsn": args.db_dsn}
    if args.io_mode != "local":
        required["ssh_host"] = args.ssh_host
        required["ssh_username"] = args.ssh_username
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise ValueError(f"필수 인자가 비어 있습니다: {', '.join(missing)}")
    if args.days_back < 1:
        raise ValueError("days_back은 1 이상이어야 합니다.")


def build_recent_date_dirs(days_back: int, today: date | None = None) -> list[str]:
    base_date = today or datetime.now(tz=LOCAL_TZ).date()
    return [(base_date - timedelta(days=offset)).strftime("%Y%m%d") for offset in range(days_back)]
