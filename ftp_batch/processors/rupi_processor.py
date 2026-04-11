from __future__ import annotations

from pathlib import Path

import pandas as pd

from ftp_batch.infra.db_manager import DBManager


class RupiProcessor:
    def __init__(self, db_path, scale_percent=50):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.scale_percent = scale_percent
        self.db = DBManager(db_path)


    def get_image_by_source_file(self, source_file, connection=None):
        df = self.db.fetch_df(
            """
            select *
            from rupi_ingest
            where source_file = ?
            """,
            params=[source_file],
            connection=connection,
        )
        if df.empty:
            return None
        return df.iloc[0].to_dict()

    def insert_image(self, source_file, prefix, image_ts, connection=None):
        row = self.get_image_by_source_file(source_file, connection=connection)
        if row:
            return row["id"]
        df = pd.DataFrame(
            [
                {
                    "source_file": source_file,
                    "prefix": prefix,
                    "image_ts": image_ts.isoformat(),
                }
            ]
        )
        self.db.bulk_insert_df("rupi_ingest", df, connection=connection)
        row = self.get_image_by_source_file(source_file, connection=connection)
        return row["id"]

    def delete_image(self, image_id, connection=None):
        self.db.execute(
            """
            delete from rupi_ingest
            where id = ?
            """,
            (image_id,),
            connection=connection,
        )

    def update_match_candidate(
        self,
        image_id,
        matched_text_file,
        matched_text_ts,
        matched_diff_seconds,
        connection=None,
    ):
        self.db.execute(
            """
            update rupi_ingest
            set matched_text_file = ?,
                matched_text_ts = ?,
                matched_diff_seconds = ?,
                updated_at = current_timestamp
            where id = ?
            """,
            (
                matched_text_file,
                matched_text_ts.isoformat(),
                matched_diff_seconds,
                image_id,
            ),
            connection=connection,
        )

    def finalize_upload(self, image_id, output_remote_file, connection=None):
        self.db.execute(
            """
            update rupi_ingest
            set output_remote_file = ?,
                updated_at = current_timestamp
            where id = ?
            """,
            (
                output_remote_file,
                image_id,
            ),
            connection=connection,
        )

    def upsert_image_match(
        self,
        *,
        source_file,
        prefix,
        image_ts,
        matched_text_file,
        matched_text_ts,
        matched_diff_seconds,
        output_remote_file,
        connection=None,
    ):
        self.db.execute(
            """
            insert into rupi_ingest (
                source_file,
                prefix,
                image_ts,
                matched_text_file,
                matched_text_ts,
                matched_diff_seconds,
                output_remote_file
            )
            values (?, ?, ?, ?, ?, ?, ?)
            on conflict(source_file) do update set
                prefix = excluded.prefix,
                image_ts = excluded.image_ts,
                matched_text_file = excluded.matched_text_file,
                matched_text_ts = excluded.matched_text_ts,
                matched_diff_seconds = excluded.matched_diff_seconds,
                output_remote_file = excluded.output_remote_file,
                updated_at = current_timestamp
            """,
            (
                source_file,
                prefix,
                image_ts.isoformat(),
                matched_text_file,
                matched_text_ts.isoformat(),
                matched_diff_seconds,
                output_remote_file,
            ),
            connection=connection,
        )

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
