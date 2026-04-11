from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from ftp_batch.infra.db_manager import DBManager


class RubiProcessor:
    def __init__(self, db_path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db = DBManager(db_path)

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

    def build_df(self, source_file, parsed_records):
        rows = [
            {
                "source_file": source_file,
                "line_number": record["line_number"],
                "record_type": record["type"],
                "payload_json": json.dumps(record, ensure_ascii=False),
            }
            for record in parsed_records
        ]
        return pd.DataFrame(rows)

    def parse_to_df(self, local_path, source_file):
        text = self.read_text(local_path)
        parsed_records = self.parse_text(text)
        preview = json.dumps(parsed_records[:3], ensure_ascii=False)
        print(f"[TEXT] {local_path.name} / length={len(text)} / records={len(parsed_records)}")
        print(f"[TEXT] preview={preview}")
        return self.build_df(source_file, parsed_records)

    def store_df(self, df, connection=None):
        self.db.bulk_insert_df("rubi_ingest", df, connection=connection)

    def process(self, local_path, source_file, connection=None):
        df = self.parse_to_df(local_path, source_file)
        self.store_df(df, connection=connection)
        return df
