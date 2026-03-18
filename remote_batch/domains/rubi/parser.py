from __future__ import annotations


def parse_text(text: str) -> list[dict]:
    records: list[dict] = []
    for line_number, raw_line in enumerate(text.splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue
        if "=" in line:
            key, value = line.split("=", 1)
            records.append(
                {
                    "line_number": line_number,
                    "format": "key_value",
                    "key": key.strip(),
                    "value": value.strip(),
                }
            )
        elif "," in line:
            records.append(
                {
                    "line_number": line_number,
                    "format": "csv_like",
                    "columns": [item.strip() for item in line.split(",")],
                }
            )
        else:
            records.append(
                {
                    "line_number": line_number,
                    "format": "raw",
                    "text": line,
                }
            )
    return records
