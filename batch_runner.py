from __future__ import annotations

from datetime import datetime
from pathlib import Path

from ftp_scanner import FTPScanner
from rubi_processor import RubiProcessor
from rupi_processor import RupiProcessor


class BatchRunner:
    def __init__(
        self,
        input_date,
        parser_name,
        *,
        host,
        port,
        username,
        password,
        root_path,
        temp_dir="/tmp/ftp_work",
        output_dir="/tmp/ftp_output",
        scale_percent=50,
        passive=True,
    ):
        self.input_date = input_date
        self.date_str = self._normalize_date(input_date)
        self.parser_name = self._normalize_parser(parser_name)
        self.temp_dir = Path(temp_dir)
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.scanner = FTPScanner(
            host=host,
            port=port,
            username=username,
            password=password,
            root_path=root_path,
            passive=passive,
        )
        self.rubi_processor = RubiProcessor()
        self.rupi_processor = RupiProcessor(output_dir=output_dir, scale_percent=scale_percent)

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
        return self.temp_dir / remote_path.lstrip("/")

    def process_file(self, local_path):
        if self.parser_name == "rubi":
            if self.rubi_processor.can_process(local_path):
                self.rubi_processor.process(local_path)
                return True
            print(f"[SKIP] rubi 대상 아님: {local_path.name}")
            return False

        if self.rupi_processor.can_process(local_path):
            self.rupi_processor.process(local_path)
            return True
        print(f"[SKIP] rupi 대상 아님: {local_path.name}")
        return False

    def cleanup(self, local_path):
        try:
            if local_path.exists():
                local_path.unlink()
        except Exception as exc:
            print(f"[WARN] 삭제 실패: {local_path} / {exc}")

    def run(self):
        processed = 0
        skipped = 0
        errors = 0

        try:
            remote_files = self.scanner.scan(self.date_str)
            total = len(remote_files)
            print(
                f"[INFO] parser={self.parser_name}, input_date={self.input_date}, "
                f"ftp_date={self.date_str}, total={total}"
            )

            for idx, remote_file in enumerate(remote_files, start=1):
                local_path = self.build_local_path(remote_file)
                try:
                    print(f"[{idx}/{total}] DOWNLOAD: {remote_file}")
                    self.scanner.download_file(remote_file, local_path)
                    print(f"[{idx}/{total}] PROCESS")
                    if self.process_file(local_path):
                        processed += 1
                    else:
                        skipped += 1
                except Exception as exc:
                    errors += 1
                    print(f"[ERROR] {remote_file} / {exc}")
                finally:
                    self.cleanup(local_path)
        finally:
            self.scanner.close()

        print(f"[SUMMARY] processed={processed}, skipped={skipped}, errors={errors}")
