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
        done_file_path,
        work_dir="/tmp/ftp_work",
        scale_percent=50,
        passive=True,
    ):
        self.input_date = input_date
        self.date_str = self._normalize_date(input_date)
        self.parser_name = self._normalize_parser(parser_name)
        self.client_root_path = client_root_path.rstrip("/")
        self.server_root_path = server_root_path.rstrip("/")
        self.done_file_path = Path(done_file_path)
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
        self.server_scanner = FTPScanner(
            host=server_host,
            port=server_port,
            username=server_username,
            password=server_password,
            root_path=self.server_root_path,
            passive=passive,
        )
        self.rubi_processor = RubiProcessor()
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

    def build_local_path(self, remote_path):
        return self.work_dir / remote_path.lstrip("/")

    def build_relative_path(self, remote_path, root_path):
        remote = PurePosixPath(remote_path)
        root = PurePosixPath(root_path)
        try:
            return remote.relative_to(root).as_posix()
        except ValueError:
            return remote.as_posix().lstrip("/")

    def build_file_key(self, remote_path):
        return self.build_relative_path(remote_path, self.client_root_path)

    def build_server_remote_path(self, client_remote_path):
        relative = PurePosixPath(self.build_relative_path(client_remote_path, self.client_root_path))
        return (PurePosixPath(self.server_root_path) / relative.with_suffix(".png")).as_posix()

    def build_done_record(self, file_key):
        return f"{self.parser_name}|{file_key}"

    def load_done_keys(self):
        done_keys = set()
        if not self.done_file_path.exists():
            return done_keys
        for line in self.done_file_path.read_text(encoding="utf-8").splitlines():
            record = line.strip()
            if not record:
                continue
            parser_name, separator, file_key = record.partition("|")
            if parser_name == self.parser_name and separator and file_key:
                done_keys.add(file_key)
        return done_keys

    def mark_done(self, file_key):
        self.done_file_path.parent.mkdir(parents=True, exist_ok=True)
        with self.done_file_path.open("a", encoding="utf-8") as file_obj:
            file_obj.write(f"{self.build_done_record(file_key)}\n")

    def is_target_file(self, remote_path):
        suffix = Path(remote_path).suffix.lower()
        if self.parser_name == "rubi":
            return suffix in self.rubi_processor.EXTENSIONS
        return suffix in self.rupi_processor.EXTENSIONS

    def process_file(self, local_path):
        if self.parser_name == "rubi":
            if self.rubi_processor.can_process(local_path):
                self.rubi_processor.process(local_path)
                return True, None
            print(f"[SKIP] rubi 대상 아님: {local_path.name}")
            return False, None

        if self.rupi_processor.can_process(local_path):
            return True, self.rupi_processor.process(local_path)
        print(f"[SKIP] rupi 대상 아님: {local_path.name}")
        return False, None

    def is_rupi_processed(self, local_path):
        return self.rupi_processor.build_output_path(local_path).exists()

    def ensure_local_file(self, remote_file, local_path, idx, total):
        if local_path.exists():
            print(f"[{idx}/{total}] CLIENT DOWNLOAD SKIP: {local_path}")
            return
        print(f"[{idx}/{total}] CLIENT DOWNLOAD: {remote_file}")
        self.client_scanner.download_file(remote_file, local_path)

    def run(self):
        processed = 0
        skipped = 0
        errors = 0

        try:
            remote_files = [path for path in self.client_scanner.scan(self.date_str) if self.is_target_file(path)]
            if self.parser_name == "rubi":
                source_map = {self.build_file_key(path): path for path in remote_files}
                done_keys = self.load_done_keys()
                target_items = [(file_key, source_map[file_key]) for file_key in sorted(source_map.keys() - done_keys)]
            else:
                done_keys = set()
                target_items = [(self.build_file_key(path), path) for path in remote_files]
            total = len(target_items)
            print(
                f"[INFO] parser={self.parser_name}, input_date={self.input_date}, "
                f"ftp_date={self.date_str}, client_total={len(remote_files)}, "
                f"done_total={len(done_keys)}, target_total={total}"
            )

            for idx, (file_key, remote_file) in enumerate(target_items, start=1):
                local_path = self.build_local_path(remote_file)
                try:
                    self.ensure_local_file(remote_file, local_path, idx, total)
                    if self.parser_name == "rupi" and self.is_rupi_processed(local_path):
                        print(f"[{idx}/{total}] PROCESS SKIP: {self.rupi_processor.build_output_path(local_path)}")
                        skipped += 1
                        continue
                    print(f"[{idx}/{total}] PROCESS")
                    success, output_path = self.process_file(local_path)
                    if success:
                        if self.parser_name == "rubi":
                            self.mark_done(file_key)
                        else:
                            remote_output_path = self.build_server_remote_path(remote_file)
                            print(f"[{idx}/{total}] SERVER UPLOAD: {remote_output_path}")
                            self.server_scanner.upload_file(output_path, remote_output_path)
                        processed += 1
                    else:
                        skipped += 1
                except Exception as exc:
                    errors += 1
                    print(f"[ERROR] {remote_file} / {exc}")
        finally:
            self.client_scanner.close()
            self.server_scanner.close()

        print(f"[SUMMARY] processed={processed}, skipped={skipped}, errors={errors}")
