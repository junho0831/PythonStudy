from __future__ import annotations

import json
from pathlib import Path


class RubiProcessor:
    EXTENSIONS = {".txt", ".csv", ".log"}

    def can_process(self, local_path):
        return local_path.suffix.lower() in self.EXTENSIONS

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

    def process(self, local_path):
        text = self.read_text(local_path)
        parsed_records = self.parse_text(text)
        preview = json.dumps(parsed_records[:3], ensure_ascii=False)
        print(f"[TEXT] {local_path.name} / length={len(text)} / records={len(parsed_records)}")
        print(f"[TEXT] preview={preview}")
        return parsed_records
