from __future__ import annotations

import sqlite3

from ftp_batch.config.local_test_settings import LOCAL_DB_PATH


def init_db(db_path):
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            create table if not exists rubi_ingest (
                id integer primary key autoincrement,
                source_file text not null,
                line_number integer not null,
                record_type text not null,
                payload_json text not null,
                created_at text not null default current_timestamp
            )
            """
        )
        conn.execute(
            """
            create table if not exists rupi_ingest (
                id integer primary key autoincrement,
                source_file text not null unique,
                prefix text not null,
                image_ts text not null,
                matched_text_file text,
                matched_text_ts text,
                matched_diff_seconds integer,
                output_remote_file text,
                created_at text not null default current_timestamp,
                updated_at text not null default current_timestamp
            )
            """
        )
        conn.commit()


def main():
    init_db(LOCAL_DB_PATH)
    print(f"[INIT] SQLite schema created: {LOCAL_DB_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
