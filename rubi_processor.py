from __future__ import annotations

import json
import sqlite3
from pathlib import Path


class RubiProcessor:
    def __init__(self, db_path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_table()

    def _ensure_table(self):
        with sqlite3.connect(self.db_path) as conn:
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
            conn.commit()

    def read_text(self, local_path):
        raw_bytes = Path(local_path).read_bytes()
        for encoding in ("utf-8", "cp949"):
            try:
                return raw_bytes.decode(encoding)
            except UnicodeDecodeError:
                continue
        return raw_bytes.decode("utf-8", errors="replace")

    def parse_text(self, text):
        parsed_records = []
        for line_number, raw_line in enumerate(text.splitlines(), start=1):
            line = raw_line.strip()
            if not line:
                continue
            if "=" in line:
                key, value = line.split("=", 1)
                parsed_records.append(
                    {
                        "type": "key_value",
                        "line_number": line_number,
                        "key": key.strip(),
                        "value": value.strip(),
                    }
                )
                continue
            if "," in line:
                parsed_records.append(
                    {
                        "type": "csv_like",
                        "line_number": line_number,
                        "columns": [value.strip() for value in line.split(",")],
                    }
                )
                continue
            parsed_records.append(
                {
                    "type": "raw",
                    "line_number": line_number,
                    "text": line,
                }
            )
        return parsed_records

    def store_records(self, source_file, parsed_records):
        with sqlite3.connect(self.db_path) as conn:
            try:
                conn.execute("begin")
                for record in parsed_records:
                    conn.execute(
                        """
                        insert into rubi_ingest (
                            source_file,
                            line_number,
                            record_type,
                            payload_json
                        ) values (?, ?, ?, ?)
                        """,
                        (
                            source_file,
                            record["line_number"],
                            record["type"],
                            json.dumps(record, ensure_ascii=False),
                        ),
                    )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def process(self, local_path, source_file):
        text = self.read_text(local_path)
        parsed_records = self.parse_text(text)
        preview = json.dumps(parsed_records[:3], ensure_ascii=False)
        print(f"[TEXT] {local_path.name} / length={len(text)} / records={len(parsed_records)}")
        print(f"[TEXT] preview={preview}")
        self.store_records(source_file, parsed_records)
        return parsed_records
