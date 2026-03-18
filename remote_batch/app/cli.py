from __future__ import annotations

from .config import build_parser, build_recent_date_dirs, configure_logging, finalize_args, validate_args
from .runner import run_batch


def main() -> int:
    args = finalize_args(build_parser().parse_args())
    configure_logging(args.log_level)
    validate_args(args)
    run_batch(args, build_recent_date_dirs=build_recent_date_dirs)
    return 0
