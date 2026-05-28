from __future__ import annotations

import argparse
from datetime import datetime

from er_dose.batch import ERDoseBatch
from er_dose.infra.postgres_db import PostgresDB


def parse_datetime(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid datetime: {value}") from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Parse ER Dose Error raw logs into normalized table.")
    parser.add_argument("--start-time", required=True, type=parse_datetime, help="inclusive start time")
    parser.add_argument("--end-time", required=True, type=parse_datetime, help="exclusive end time")
    parser.add_argument("--limit", type=int, default=None, help="optional max raw rows to process")
    parser.add_argument("--dsn", default=None, help="PostgreSQL DSN. Defaults to ER_DOSE_DB_DSN or DATABASE_URL.")
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    if args.start_time >= args.end_time:
        raise ValueError("--start-time must be earlier than --end-time")

    db = PostgresDB(dsn=args.dsn)
    batch = ERDoseBatch(db)
    batch.run(start_time=args.start_time, end_time=args.end_time, limit=args.limit)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

