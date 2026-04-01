from __future__ import annotations

from datetime import datetime
from pathlib import Path, PurePosixPath

from ftp_scanner import FTPScanner
from rubi_processor import RubiProcessor
from rupi_processor import RupiProcessor


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
        self.date_str = self._normalize_date(input_date)
        self.parser_name = self._normalize_parser(parser_name)
        self.client_root_path = client_root_path.rstrip("/")
        self.server_root_path = server_root_path.rstrip("/")
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.client_scanner = FTPScanner(
            host=client_host,
            port=client_port,
            username=client_username,
            password=client_password,
            root_path=self.client_root_path,
            passive=passive,
        )
        self.server_scanner = None
        if self.parser_name == "rupi":
            self.server_scanner = FTPScanner(
                host=server_host,
                port=server_port,
                username=server_username,
                password=server_password,
                root_path=self.server_root_path,
                passive=passive,
            )
        self.rubi_processor = RubiProcessor(db_path=db_path)
        self.rupi_processor = RupiProcessor(scale_percent=scale_percent)

    def _normalize_date(self, input_date):
        try:
            return datetime.strptime(input_date, "%Y-%m-%d").strftime("%Y%m%d")
        except ValueError as exc:
            raise ValueError(f"날짜 형식 오류: {input_date} (YYYY-MM-DD 필요)") from exc

    def _normalize_parser(self, parser_name):
        normalized = parser_name.lower().strip()
        if normalized == "rubi":
            return "rubi"
        if normalized in {"rupi", "rubp"}:
            return "rupi"
        raise ValueError(f"지원하지 않는 parser_name: {parser_name}")

    def build_relative_path(self, remote_path, root_path):
        remote = PurePosixPath(remote_path)
        root = PurePosixPath(root_path)
        try:
            return remote.relative_to(root).as_posix()
        except ValueError:
            return remote.as_posix().lstrip("/")

    def build_local_path(self, remote_path):
        relative = PurePosixPath(self.build_relative_path(remote_path, self.client_root_path))
        return self.work_dir.joinpath(*relative.parts)

    def build_server_remote_path(self, client_remote_path):
        relative = PurePosixPath(self.build_relative_path(client_remote_path, self.client_root_path))
        return (PurePosixPath(self.server_root_path) / relative).with_suffix(".png").as_posix()

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

    def process_file(self, local_path, remote_file):
        if self.parser_name == "rubi":
            self.rubi_processor.process(local_path, source_file=remote_file)
            return True, None
        return True, self.rupi_processor.process(local_path)

    def run(self):
        processed = 0
        skipped = 0
        errors = 0

        try:
            remote_files = self.client_scanner.scan(self.date_str)
            total = len(remote_files)
            print(
                f"[INFO] parser={self.parser_name}, input_date={self.input_date}, "
                f"ftp_date={self.date_str}, client_total={total}, target_total={total}"
            )

            for idx, remote_file in enumerate(remote_files, start=1):
                local_path = self.build_local_path(remote_file)
                output_path = None
                try:
                    if self.parser_name == "rupi":
                        output_path = self.rupi_processor.build_output_path(local_path)
                        remote_output_path = self.build_server_remote_path(remote_file)
                        if self.server_scanner.file_exists(remote_output_path):
                            print(f"[{idx}/{total}] PROCESS SKIP (REMOTE PNG EXISTS): {remote_output_path}")
                            skipped += 1
                            continue
                        if output_path.exists():
                            print(f"[{idx}/{total}] PROCESS SKIP (LOCAL PNG REUSE): {output_path}")
                            self.upload_rupi_output(output_path, remote_output_path, idx, total)
                        else:
                            self.ensure_local_rupi_source_file(remote_file, local_path, idx, total)
                            print(f"[{idx}/{total}] PROCESS")
                            success, output_path = self.process_file(local_path, remote_file)
                            if not success:
                                skipped += 1
                                continue
                            self.upload_rupi_output(output_path, remote_output_path, idx, total)
                        local_path.unlink(missing_ok=True)
                        processed += 1
                        continue

                    self.download_local_file(remote_file, local_path, idx, total)
                    print(f"[{idx}/{total}] PROCESS")
                    success, _ = self.process_file(local_path, remote_file)

                    if not success:
                        skipped += 1
                        continue

                    print(f"[{idx}/{total}] CLIENT DELETE: {remote_file}")
                    self.client_scanner.delete_file(remote_file)
                    local_path.unlink(missing_ok=True)
                    processed += 1
                except Exception as exc:
                    errors += 1
                    print(f"[ERROR] {remote_file} / {exc}")
                    if self.parser_name == "rubi" and local_path.exists():
                        local_path.unlink()
        finally:
            self.client_scanner.close()
            if self.server_scanner is not None:
                self.server_scanner.close()

        print(f"[SUMMARY] processed={processed}, skipped={skipped}, errors={errors}")
