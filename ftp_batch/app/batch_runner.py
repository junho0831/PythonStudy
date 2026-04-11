from __future__ import annotations

from pathlib import Path

from ftp_batch.common.date_utils import get_target_dates
from ftp_batch.common.path_utils import build_local_path, make_rbi_path
from ftp_batch.infra.ftp_scanner import FTPScanner
from ftp_batch.matching.image_text_matcher import extract_info, find_best_text_match
from ftp_batch.processors.rubi_processor import RubiProcessor
from ftp_batch.processors.rupi_processor import RupiProcessor


class BatchRunner:
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
        self.client_root_path = client_root_path.rstrip("/")
        self.text_root_path = text_root_path.rstrip("/")
        self.source_root_path = self.text_root_path if self.parser_name == "rubi" else self.client_root_path
        self.server_root_path = server_root_path.rstrip("/")
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.client_scanner = FTPScanner(
            host=client_host,
            port=client_port,
            username=client_username,
            password=client_password,
            root_path=self.source_root_path,
            passive=passive,
        )
        self.server_scanner = None
        self.text_scanner = None
        if self.parser_name == "rupi":
            self.text_scanner = FTPScanner(
                host=client_host,
                port=client_port,
                username=client_username,
                password=client_password,
                root_path=self.text_root_path,
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
        self.rubi_processor = RubiProcessor(db_path=db_path) if self.parser_name == "rubi" else None
        self.rupi_processor = (
            RupiProcessor(db_path=db_path, scale_percent=scale_percent)
            if self.parser_name == "rupi"
            else None
        )

    def _normalize_parser(self, parser_name):
        normalized = parser_name.lower().strip()
        if normalized == "rubi":
            return "rubi"
        if normalized in {"rupi", "rubp"}:
            return "rupi"
        raise ValueError(f"지원하지 않는 parser_name: {parser_name}")

    def download_local_file(self, remote_file, local_path, idx, total):
        print(f"[{idx}/{total}] CLIENT DOWNLOAD: {remote_file}")
        self.client_scanner.download_file(remote_file, local_path)

    def ensure_local_rupi_source_file(self, remote_file, local_path, idx, total):
        if local_path.exists():
            print(f"[{idx}/{total}] CLIENT DOWNLOAD SKIP (LOCAL TIF EXISTS): {local_path}")
            return
        self.download_local_file(remote_file, local_path, idx, total)

    def upload_rupi_output(self, output_path, remote_output_path, idx, total):
        print(f"[{idx}/{total}] SERVER UPLOAD: {remote_output_path}")
        self.server_scanner.upload_file(output_path, remote_output_path)

    def run(self):
        processed = 0
        skipped = 0
        errors = 0

        try:
            for date_str in self.target_dates:
                remote_files = self.client_scanner.scan(date_str)
                text_files = []
                upload_queue = []
                if self.parser_name == "rupi":
                    text_files = self.text_scanner.scan(date_str)
                total = len(remote_files)
                print(
                    f"[INFO] parser={self.parser_name}, input_date={self.input_date}, "
                    f"date={date_str}, total={total}"
                )

                for idx, remote_file in enumerate(remote_files, start=1):
                    local_path = build_local_path(self.work_dir, self.source_root_path, remote_file)
                    try:
                        if self.parser_name == "rupi":
                            prefix, image_ts = extract_info(remote_file)
                            image_id = self.rupi_processor.insert_image(remote_file, prefix, image_ts)
                            matched_text = find_best_text_match(remote_file, text_files)
                            if not matched_text:
                                print(f"[{idx}/{total}] MATCH SKIP (TEXT NOT FOUND): {remote_file}")
                                self.rupi_processor.delete_image(image_id)
                                skipped += 1
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
                                processed += 1
                                continue
                            if output_path.exists():
                                print(f"[{idx}/{total}] PROCESS PREPARE (LOCAL PNG REUSE): {output_path}")
                            else:
                                self.ensure_local_rupi_source_file(remote_file, local_path, idx, total)
                                print(f"[{idx}/{total}] PROCESS PREPARE")
                                output_path = self.rupi_processor.process(local_path)

                            upload_queue.append(
                                {
                                    "image_id": image_id,
                                    "remote_file": remote_file,
                                    "local_path": local_path,
                                    "output_path": output_path,
                                    "remote_output_path": remote_output_path,
                                }
                            )
                            continue

                        self.download_local_file(remote_file, local_path, idx, total)
                        print(f"[{idx}/{total}] PROCESS")
                        self.rubi_processor.process(local_path, source_file=remote_file)
                        print(f"[{idx}/{total}] CLIENT DELETE: {remote_file}")
                        self.client_scanner.delete_file(remote_file)
                        local_path.unlink(missing_ok=True)
                        processed += 1
                    except Exception as exc:
                        errors += 1
                        print(f"[ERROR] {remote_file} / {exc}")
                        if local_path.exists():
                            local_path.unlink()

                if self.parser_name == "rupi" and upload_queue:
                    upload_total = len(upload_queue)
                    print(f"[UPLOAD] date={date_str} queued={upload_total}")
                    for upload_idx, item in enumerate(upload_queue, start=1):
                        try:
                            self.upload_rupi_output(
                                item["output_path"],
                                item["remote_output_path"],
                                upload_idx,
                                upload_total,
                            )
                            self.rupi_processor.finalize_upload(
                                item["image_id"],
                                item["remote_output_path"],
                            )
                            item["local_path"].unlink(missing_ok=True)
                            processed += 1
                        except Exception as exc:
                            errors += 1
                            print(f"[ERROR] {item['remote_file']} / {exc}")
                            item["local_path"].unlink(missing_ok=True)
        finally:
            self.client_scanner.close()
            if self.text_scanner is not None:
                self.text_scanner.close()
            if self.server_scanner is not None:
                self.server_scanner.close()

        print(f"[SUMMARY] processed={processed}, skipped={skipped}, errors={errors}")
