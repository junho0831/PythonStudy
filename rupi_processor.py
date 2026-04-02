from __future__ import annotations

import sqlite3
from pathlib import Path


class RupiProcessor:
    def __init__(self, db_path, scale_percent=50):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.scale_percent = scale_percent
        self._ensure_table()

    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_table(self):
        with self._connect() as conn:
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

    def get_image_by_source_file(self, source_file):
        with self._connect() as conn:
            row = conn.execute(
                """
                select *
                from rupi_ingest
                where source_file = ?
                """,
                (source_file,),
            ).fetchone()
        return dict(row) if row else None

    def insert_image(self, source_file, prefix, image_ts):
        with self._connect() as conn:
            conn.execute(
                """
                insert or ignore into rupi_ingest (
                    source_file,
                    prefix,
                    image_ts
                ) values (?, ?, ?)
                """,
                (source_file, prefix, image_ts.isoformat()),
            )
            conn.commit()
        row = self.get_image_by_source_file(source_file)
        return row["id"]

    def delete_image(self, image_id):
        with self._connect() as conn:
            conn.execute(
                """
                delete from rupi_ingest
                where id = ?
                """,
                (image_id,),
            )
            conn.commit()

    def update_image_match(
        self,
        image_id,
        matched_text_file,
        matched_text_ts,
        matched_diff_seconds,
        output_remote_file,
    ):
        with self._connect() as conn:
            conn.execute(
                """
                update rupi_ingest
                set matched_text_file = ?,
                    matched_text_ts = ?,
                    matched_diff_seconds = ?,
                    output_remote_file = ?,
                    updated_at = current_timestamp
                where id = ?
                """,
                (
                    matched_text_file,
                    matched_text_ts.isoformat(),
                    matched_diff_seconds,
                    output_remote_file,
                    image_id,
                ),
            )
            conn.commit()

    def build_output_path(self, local_path):
        return Path(local_path).with_suffix(".png")

    def _require_image(self):
        try:
            from PIL import Image
        except ImportError as exc:
            raise RuntimeError("Pillow가 필요합니다. `pip install pillow`로 설치하세요.") from exc
        return Image

    def process(self, local_path):
        Image = self._require_image()
        output_path = self.build_output_path(local_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with Image.open(local_path) as image:
                width, height = image.size
                new_size = (
                    max(1, round(width * self.scale_percent / 100)),
                    max(1, round(height * self.scale_percent / 100)),
                )
                resized = image.resize(new_size)
                resized.save(output_path, format="PNG")
                print(
                    f"[IMAGE] {local_path.name} / size={image.size} / mode={image.mode} "
                    f"-> output={output_path} / resized={new_size}"
                )
        except Exception:
            output_path.unlink(missing_ok=True)
            raise
        return output_path
