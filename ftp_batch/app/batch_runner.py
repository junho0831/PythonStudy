from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from ftp_batch.common.date_utils import get_target_dates
from ftp_batch.common.path_utils import build_local_path, make_rbi_path
from ftp_batch.infra.ftp_scanner import FTPScanner
from ftp_batch.matching.image_text_matcher import MAX_MATCH_DIFF, extract_info, find_best_text_match
from ftp_batch.processors.rubi_processor import RubiProcessor
from ftp_batch.processors.rupi_processor import RupiProcessor


@dataclass
class RunStats:
    processed: int = 0
    skipped: int = 0
    errors: int = 0

    def add(self, other: "RunStats") -> None:
        self.processed += other.processed
        self.skipped += other.skipped
        self.errors += other.errors


@dataclass(frozen=True)
class TextWorkItem:
    remote_path: str
    local_path: Path
    prefix: str
    text_ts: datetime


@dataclass
class ImageCandidate:
    remote_path: str
    local_path: Path
    prefix: str
    image_ts: datetime


@dataclass(frozen=True)
class RupiUploadTask:
    image_id: int
    remote_file: str
    local_path: Path
    output_path: Path
    remote_output_path: str


@dataclass(frozen=True)
class CombinedUploadTask:
    text_remote_path: str
    image_remote_path: str
    output_path: Path
    remote_output_path: str
    rubi_df: Any
    prefix: str
    image_ts: datetime
    matched_text_ts: datetime
    matched_diff_seconds: int


class BatchRunner:
    PNG_RETENTION_DAYS = 3

    def __init__(
        self,
        input_date,
        parser_name,
        *,
        client_host,
        client_port,
        client_username,
        client_password,
        client_root_path,
        text_root_path,
        server_host,
        server_port,
        server_username,
        server_password,
        server_root_path,
        db_path,
        work_dir="/tmp/ftp_work",
        scale_percent=50,
        passive=True,
    ):
        self.input_date = input_date
        self.target_dates = get_target_dates(input_date)
        self.parser_name = self._normalize_parser(parser_name)
        self.image_root_path = client_root_path.rstrip("/")
        self.text_root_path = text_root_path.rstrip("/")
        self.server_root_path = server_root_path.rstrip("/")
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(parents=True, exist_ok=True)

        self.text_scanner = None
        self.image_scanner = None
        self.server_scanner = None

        if self.parser_name in {"rubi", "rupi", "combined"}:
            self.text_scanner = FTPScanner(
                host=client_host,
                port=client_port,
                username=client_username,
                password=client_password,
                root_path=self.text_root_path,
                passive=passive,
            )
        if self.parser_name in {"rupi", "combined"}:
            self.image_scanner = FTPScanner(
                host=client_host,
                port=client_port,
                username=client_username,
                password=client_password,
                root_path=self.image_root_path,
                passive=passive,
            )
            self.server_scanner = FTPScanner(
                host=server_host,
                port=server_port,
                username=server_username,
                password=server_password,
                root_path=self.server_root_path,
                passive=passive,
            )

        self.rubi_processor = RubiProcessor(db_path=db_path) if self.parser_name in {"rubi", "combined"} else None
        self.rupi_processor = (
            RupiProcessor(db_path=db_path, scale_percent=scale_percent)
            if self.parser_name in {"rupi", "combined"}
            else None
        )

    def _normalize_parser(self, parser_name):
        normalized = parser_name.lower().strip()
        if normalized == "rubi":
            return "rubi"
        if normalized in {"rupi", "rubp"}:
            return "rupi"
        if normalized in {"combined", "pair"}:
            return "combined"
        raise ValueError(f"지원하지 않는 parser_name: {parser_name}")

    @staticmethod
    def _safe_unlink(path: Path | None) -> None:
        if path is None:
            return
        path.unlink(missing_ok=True)

    def _cleanup_stale_png_cache(self):
        threshold = datetime.now() - timedelta(days=self.PNG_RETENTION_DAYS)
        removed = 0
        for png_path in self.work_dir.rglob("*.png"):
            modified_at = datetime.fromtimestamp(png_path.stat().st_mtime)
            if modified_at < threshold:
                self._safe_unlink(png_path)
                removed += 1
        if removed:
            print(f"[CACHE] removed_png={removed}, retention_days={self.PNG_RETENTION_DAYS}")

    def _download_all_files(self, scanner, root_path, remote_files, label):
        downloaded = {}
        total = len(remote_files)
        for idx, remote_file in enumerate(remote_files, start=1):
            local_path = build_local_path(self.work_dir, root_path, remote_file)
            self._safe_unlink(local_path)
            try:
                print(f"[{label} DOWNLOAD {idx}/{total}] {remote_file}")
                scanner.download_file(remote_file, local_path)
                downloaded[remote_file] = local_path
            except Exception as exc:
                print(f"[ERROR] {remote_file} / {exc}")
                self._safe_unlink(local_path)
        return downloaded

    def _scan_combined_remote_files(self):
        text_remote_files = []
        image_remote_files = []
        for date_str in self.target_dates:
            date_text_files = self.text_scanner.scan(date_str)
            date_image_files = self.image_scanner.scan(date_str)
            print(
                f"[INFO] parser=combined, input_date={self.input_date}, "
                f"date={date_str}, text_total={len(date_text_files)}, image_total={len(date_image_files)}"
            )
            text_remote_files.extend(date_text_files)
            image_remote_files.extend(date_image_files)
        return text_remote_files, image_remote_files

    def _build_combined_text_items(self, text_local_map):
        text_items: list[TextWorkItem] = []
        errors = 0
        for remote_path, local_path in text_local_map.items():
            try:
                prefix, text_ts = extract_info(remote_path)
            except ValueError as exc:
                errors += 1
                print(f"[ERROR] {remote_path} / {exc}")
                self._safe_unlink(local_path)
                continue
            text_items.append(
                TextWorkItem(
                    remote_path=remote_path,
                    local_path=local_path,
                    prefix=prefix,
                    text_ts=text_ts,
                )
            )
        text_items.sort(key=lambda item: item.text_ts)
        return text_items, errors

    def _build_combined_image_candidates(self, image_local_map):
        image_candidates_by_prefix: dict[str, list[ImageCandidate]] = defaultdict(list)
        errors = 0
        for remote_path, local_path in image_local_map.items():
            try:
                prefix, image_ts = extract_info(remote_path)
            except ValueError as exc:
                errors += 1
                print(f"[ERROR] {remote_path} / {exc}")
                self._safe_unlink(local_path)
                continue
            image_candidates_by_prefix[prefix].append(
                ImageCandidate(
                    remote_path=remote_path,
                    local_path=local_path,
                    prefix=prefix,
                    image_ts=image_ts,
                )
            )
        for candidates in image_candidates_by_prefix.values():
            candidates.sort(key=lambda item: item.image_ts)
        return image_candidates_by_prefix, errors

    def _delete_text_source(self, remote_file):
        self.text_scanner.delete_file(remote_file)

    def _delete_matched_sources(self, text_remote_file, image_remote_file):
        self.text_scanner.delete_file(text_remote_file)
        self.image_scanner.delete_file(image_remote_file)

    def _pop_nearest_image(self, text_item: TextWorkItem, image_candidates_by_prefix):
        candidates = image_candidates_by_prefix.get(text_item.prefix, [])
        if not candidates:
            return None

        best_index = None
        best_diff = None
        for index, item in enumerate(candidates):
            if item.image_ts > text_item.text_ts:
                break
            diff = text_item.text_ts - item.image_ts
            if diff > MAX_MATCH_DIFF:
                continue
            if best_diff is None or diff < best_diff:
                best_index = index
                best_diff = diff

        if best_index is None:
            return None
        return candidates.pop(best_index)

    def _cleanup_unmatched_image_candidates(self, image_candidates_by_prefix):
        for candidates in image_candidates_by_prefix.values():
            for item in candidates:
                self._safe_unlink(item.local_path)

    def _prepare_combined_queue(self, text_items, image_candidates_by_prefix):
        stats = RunStats()
        upload_queue: list[CombinedUploadTask] = []
        total = len(text_items)

        for idx, text_item in enumerate(text_items, start=1):
            text_remote_path = text_item.remote_path
            text_local_path = text_item.local_path
            matched_image = None
            try:
                print(f"[{idx}/{total}] TEXT PARSE: {text_remote_path}")
                rubi_df = self.rubi_processor.parse_to_df(text_local_path, source_file=text_remote_path)
                matched_image = self._pop_nearest_image(text_item, image_candidates_by_prefix)

                if matched_image is None:
                    with self.rubi_processor.db.transaction() as connection:
                        self.rubi_processor.store_df(rubi_df, connection=connection)
                    print(f"[{idx}/{total}] TEXT DELETE: {text_remote_path}")
                    self._delete_text_source(text_remote_path)
                    stats.processed += 1
                    continue

                matched_diff_seconds = int((text_item.text_ts - matched_image.image_ts).total_seconds())
                remote_output_path = make_rbi_path(matched_image.remote_path).as_posix()
                local_png_path = self.rupi_processor.build_output_path(matched_image.local_path)

                if self.server_scanner.file_exists(remote_output_path):
                    with self.rubi_processor.db.transaction() as connection:
                        self.rubi_processor.store_df(rubi_df, connection=connection)
                        self.rupi_processor.upsert_image_match(
                            source_file=matched_image.remote_path,
                            prefix=matched_image.prefix,
                            image_ts=matched_image.image_ts,
                            matched_text_file=text_remote_path,
                            matched_text_ts=text_item.text_ts,
                            matched_diff_seconds=matched_diff_seconds,
                            output_remote_file=remote_output_path,
                            connection=connection,
                        )
                    print(f"[{idx}/{total}] MATCHED DELETE: text={text_remote_path}, image={matched_image.remote_path}")
                    self._delete_matched_sources(text_remote_path, matched_image.remote_path)
                    stats.processed += 1
                    continue

                if local_png_path.exists():
                    print(f"[{idx}/{total}] IMAGE PREPARE (LOCAL PNG REUSE): {local_png_path}")
                else:
                    print(f"[{idx}/{total}] IMAGE PROCESS: {matched_image.remote_path}")
                    local_png_path = self.rupi_processor.process(matched_image.local_path)

                upload_queue.append(
                    CombinedUploadTask(
                        text_remote_path=text_remote_path,
                        image_remote_path=matched_image.remote_path,
                        output_path=local_png_path,
                        remote_output_path=remote_output_path,
                        rubi_df=rubi_df,
                        prefix=matched_image.prefix,
                        image_ts=matched_image.image_ts,
                        matched_text_ts=text_item.text_ts,
                        matched_diff_seconds=matched_diff_seconds,
                    )
                )
            except Exception as exc:
                stats.errors += 1
                print(f"[ERROR] {text_remote_path} / {exc}")
            finally:
                self._safe_unlink(text_local_path)
                if matched_image is not None:
                    self._safe_unlink(matched_image.local_path)

        return upload_queue, stats

    def _flush_combined_upload_queue(self, upload_queue):
        stats = RunStats()
        if upload_queue:
            upload_total = len(upload_queue)
            print(f"[UPLOAD] parser=combined queued={upload_total}")
            for upload_idx, item in enumerate(upload_queue, start=1):
                try:
                    print(f"[{upload_idx}/{upload_total}] SERVER UPLOAD: {item.remote_output_path}")
                    self.server_scanner.upload_file(item.output_path, item.remote_output_path)
                    with self.rubi_processor.db.transaction() as connection:
                        self.rubi_processor.store_df(item.rubi_df, connection=connection)
                        self.rupi_processor.upsert_image_match(
                            source_file=item.image_remote_path,
                            prefix=item.prefix,
                            image_ts=item.image_ts,
                            matched_text_file=item.text_remote_path,
                            matched_text_ts=item.matched_text_ts,
                            matched_diff_seconds=item.matched_diff_seconds,
                            output_remote_file=item.remote_output_path,
                            connection=connection,
                        )
                    print(
                        f"[{upload_idx}/{upload_total}] MATCHED DELETE: "
                        f"text={item.text_remote_path}, image={item.image_remote_path}"
                    )
                    self._delete_matched_sources(item.text_remote_path, item.image_remote_path)
                    stats.processed += 1
                except Exception as exc:
                    stats.errors += 1
                    print(f"[ERROR] {item.text_remote_path} / {exc}")
        return stats

    def _run_rubi(self):
        stats = RunStats()

        for date_str in self.target_dates:
            remote_files = self.text_scanner.scan(date_str)
            total = len(remote_files)
            print(f"[INFO] parser=rubi, input_date={self.input_date}, date={date_str}, total={total}")

            for idx, remote_file in enumerate(remote_files, start=1):
                local_path = build_local_path(self.work_dir, self.text_root_path, remote_file)
                try:
                    print(f"[{idx}/{total}] TEXT DOWNLOAD: {remote_file}")
                    self.text_scanner.download_file(remote_file, local_path)
                    print(f"[{idx}/{total}] TEXT PROCESS")
                    df = self.rubi_processor.parse_to_df(local_path, source_file=remote_file)
                    with self.rubi_processor.db.transaction() as connection:
                        self.rubi_processor.store_df(df, connection=connection)
                    print(f"[{idx}/{total}] TEXT DELETE: {remote_file}")
                    self._delete_text_source(remote_file)
                    stats.processed += 1
                except Exception as exc:
                    stats.errors += 1
                    print(f"[ERROR] {remote_file} / {exc}")
                finally:
                    self._safe_unlink(local_path)

        return stats

    def _prepare_rupi_upload_queue_for_date(self, remote_files, text_files):
        stats = RunStats()
        upload_queue: list[RupiUploadTask] = []
        total = len(remote_files)

        for idx, remote_file in enumerate(remote_files, start=1):
            local_path = build_local_path(self.work_dir, self.image_root_path, remote_file)
            try:
                prefix, image_ts = extract_info(remote_file)
                image_id = self.rupi_processor.insert_image(remote_file, prefix, image_ts)
                matched_text = find_best_text_match(remote_file, text_files)
                if not matched_text:
                    print(f"[{idx}/{total}] MATCH SKIP (TEXT NOT FOUND): {remote_file}")
                    self.rupi_processor.delete_image(image_id)
                    stats.skipped += 1
                    continue

                _, matched_text_ts = extract_info(matched_text)
                matched_diff_seconds = int((matched_text_ts - image_ts).total_seconds())
                self.rupi_processor.update_match_candidate(
                    image_id=image_id,
                    matched_text_file=matched_text,
                    matched_text_ts=matched_text_ts,
                    matched_diff_seconds=matched_diff_seconds,
                )
                output_path = self.rupi_processor.build_output_path(local_path)
                remote_output_path = make_rbi_path(remote_file).as_posix()
                if self.server_scanner.file_exists(remote_output_path):
                    print(f"[{idx}/{total}] PROCESS SKIP (REMOTE PNG EXISTS): {remote_output_path}")
                    self.rupi_processor.finalize_upload(image_id, remote_output_path)
                    stats.processed += 1
                    self._safe_unlink(local_path)
                    continue

                if output_path.exists():
                    print(f"[{idx}/{total}] PROCESS PREPARE (LOCAL PNG REUSE): {output_path}")
                else:
                    print(f"[{idx}/{total}] IMAGE DOWNLOAD: {remote_file}")
                    self.image_scanner.download_file(remote_file, local_path)
                    print(f"[{idx}/{total}] IMAGE PROCESS")
                    output_path = self.rupi_processor.process(local_path)

                upload_queue.append(
                    RupiUploadTask(
                        image_id=image_id,
                        remote_file=remote_file,
                        local_path=local_path,
                        output_path=output_path,
                        remote_output_path=remote_output_path,
                    )
                )
            except Exception as exc:
                stats.errors += 1
                print(f"[ERROR] {remote_file} / {exc}")
                self._safe_unlink(local_path)

        return upload_queue, stats

    def _flush_rupi_upload_queue_for_date(self, date_str, upload_queue):
        stats = RunStats()
        if not upload_queue:
            return stats

        upload_total = len(upload_queue)
        print(f"[UPLOAD] date={date_str} queued={upload_total}")
        for upload_idx, item in enumerate(upload_queue, start=1):
            try:
                print(f"[{upload_idx}/{upload_total}] SERVER UPLOAD: {item.remote_output_path}")
                self.server_scanner.upload_file(item.output_path, item.remote_output_path)
                self.rupi_processor.finalize_upload(item.image_id, item.remote_output_path)
                self._safe_unlink(item.local_path)
                stats.processed += 1
            except Exception as exc:
                stats.errors += 1
                print(f"[ERROR] {item.remote_file} / {exc}")
                self._safe_unlink(item.local_path)
        return stats

    def _run_rupi(self):
        stats = RunStats()

        self._cleanup_stale_png_cache()

        for date_str in self.target_dates:
            remote_files = self.image_scanner.scan(date_str)
            text_files = self.text_scanner.scan(date_str)
            total = len(remote_files)
            print(f"[INFO] parser=rupi, input_date={self.input_date}, date={date_str}, total={total}")
            queue, prepare_stats = self._prepare_rupi_upload_queue_for_date(remote_files, text_files)
            upload_stats = self._flush_rupi_upload_queue_for_date(date_str, queue)
            stats.add(prepare_stats)
            stats.add(upload_stats)

        return stats

    def _run_combined(self):
        stats = RunStats()

        self._cleanup_stale_png_cache()

        text_remote_files, image_remote_files = self._scan_combined_remote_files()
        text_local_map = self._download_all_files(self.text_scanner, self.text_root_path, text_remote_files, "TEXT")
        image_local_map = self._download_all_files(self.image_scanner, self.image_root_path, image_remote_files, "IMAGE")
        text_items, text_errors = self._build_combined_text_items(text_local_map)
        image_candidates_by_prefix, image_errors = self._build_combined_image_candidates(image_local_map)
        upload_queue, prepare_stats = self._prepare_combined_queue(
            text_items,
            image_candidates_by_prefix,
        )
        self._cleanup_unmatched_image_candidates(image_candidates_by_prefix)
        upload_stats = self._flush_combined_upload_queue(upload_queue)

        stats.add(prepare_stats)
        stats.add(upload_stats)
        stats.errors += text_errors + image_errors
        return stats

    def _close_scanners(self):
        if self.text_scanner is not None:
            self.text_scanner.close()
        if self.image_scanner is not None:
            self.image_scanner.close()
        if self.server_scanner is not None:
            self.server_scanner.close()

    def run(self):
        try:
            if self.parser_name == "rubi":
                stats = self._run_rubi()
            elif self.parser_name == "rupi":
                stats = self._run_rupi()
            else:
                stats = self._run_combined()
        finally:
            self._close_scanners()

        print(f"[SUMMARY] processed={stats.processed}, skipped={stats.skipped}, errors={stats.errors}")
