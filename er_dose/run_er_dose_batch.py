from __future__ import annotations

import argparse
from datetime import date, datetime

from er_dose.infra.postgres_db import PostgresDB
from er_dose.processor import ERDoseProcessor
from er_dose.repository import ERDoseRepository


def parse_datetime(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid datetime: {value}") from exc


def parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid date: {value}") from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Parse ER Dose Error raw logs into normalized table.")
    parser.add_argument("--date", dest="target_date", type=parse_date, help="target date in YYYY-MM-DD format")
    parser.add_argument(
        "--parser",
        dest="parser_name",
        type=str.upper,
        choices=("ER_DOSE_RAW", "ER_DOSE_EUV"),
        help="parser name; use ER_DOSE_RAW or ER_DOSE_EUV",
    )
    parser.add_argument("--start-time", type=parse_datetime, help="inclusive start time")
    parser.add_argument("--end-time", type=parse_datetime, help="exclusive end time")
    parser.add_argument("--chunk-size", type=int, default=10000, help="raw row chunk size for streaming processing")
    parser.add_argument("--dsn", default=None, help="PostgreSQL DSN. Defaults to ER_DOSE_DB_DSN or DATABASE_URL.")
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)

    if args.parser_name == "ER_DOSE_EUV":
        raise NotImplementedError("ER_DOSE_EUV is not implemented yet")

    if args.target_date is not None:
        target_date = args.target_date
        start_time = None
        end_time = None
    else:
        if args.start_time is None or args.end_time is None:
            raise ValueError("--date or both --start-time and --end-time are required")
        if args.start_time >= args.end_time:
            raise ValueError("--start-time must be earlier than --end-time")
        target_date = None
        start_time = args.start_time
        end_time = args.end_time

    db = PostgresDB(dsn=args.dsn)
    repository = ERDoseRepository(db)
    processor = ERDoseProcessor(repository)
    processor.run(
        start_time=start_time,
        end_time=end_time,
        chunk_size=args.chunk_size,
        target_date=target_date,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
