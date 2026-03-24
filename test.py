from ftplib import FTP
from pathlib import Path
from datetime import datetime
from PIL import Image


class FTPScanner:
    def __init__(self, host, port, username, password, root_path):
        self.ftp = FTP()
        self.ftp.connect(host, port)
        self.ftp.login(username, password)
            self.root_path = root_path.rstrip("/")

    def scan(self, date_str):
        target_dir = f"{self.root_path}/{date_str}"

        try:
            self.ftp.cwd(target_dir)
        except Exception:
            print(f"[SKIP] 날짜 폴더 없음: {target_dir}")
            return []

        return self._scan_recursive(target_dir)

    def _scan_recursive(self, path):
        results = []

        for name, facts in self.ftp.mlsd(path):
            full_path = f"{path}/{name}"

            if facts.get("type") == "dir":
                results.extend(self._scan_recursive(full_path))
            elif facts.get("type") == "file":
                results.append(full_path)

        return results

    def download_file(self, remote_path, local_path):
        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)

        with open(local_path, "wb") as f:
            self.ftp.retrbinary(f"RETR {remote_path}", f.write)

    def close(self):
        try:
            self.ftp.quit()
        except Exception:
            pass


class Processor:
    def __init__(self, input_date, parser_name):
        self.input_date = input_date

        # ⭐ 날짜 변환: 2026-03-10 → 20260310
        self.date_str = self._normalize_date(input_date)

        self.__parser = parser_name.lower()

        self.temp_dir = Path("/tmp/ftp_work")
        self.temp_dir.mkdir(parents=True, exist_ok=True)

        self.scanner = FTPScanner(
            host="아이피",
            port=21,
            username="mbeat2024",
            password="ftp.utils",
            root_path="/RUIP"
        )

    def _normalize_date(self, input_date):
        try:
            return datetime.strptime(input_date, "%Y-%m-%d").strftime("%Y%m%d")
        except ValueError:
            raise ValueError(f"날짜 형식 오류: {input_date} (YYYY-MM-DD 필요)")

    def build_local_path(self, remote_path):
        return self.temp_dir / remote_path.lstrip("/")

    def process_text(self, local_path):
        with open(local_path, "r", encoding="utf-8", errors="ignore") as f:
            text = f.read()

        print(f"[TEXT] {local_path.name} / length={len(text)}")
        # 👉 rubi 처리 로직

    def process_image(self, local_path):
        with Image.open(local_path) as img:
            print(f"[IMAGE] {local_path.name} / size={img.size} / mode={img.mode}")
        # 👉 rupi 처리 로직

    def process_file(self, local_path):
        ext = local_path.suffix.lower()

        if self.__parser == "rubi":
            if ext in {".txt", ".csv", ".log"}:
                self.process_text(local_path)

        elif self.__parser == "rupi":
            if ext in {".tif", ".tiff"}:
                self.process_image(local_path)

    def cleanup(self, local_path):
        try:
            if local_path.exists():
                local_path.unlink()
        except Exception as e:
            print(f"[WARN] 삭제 실패: {local_path} / {e}")

    def run(self):  # ⭐ 메인 실행
        remote_files = self.scanner.scan(self.date_str)

        total = len(remote_files)
        print(f"[INFO] parser={self.__parser}, input_date={self.input_date}, ftp_date={self.date_str}, total={total}")

        for idx, remote_file in enumerate(remote_files, start=1):
            local_path = self.build_local_path(remote_file)

            try:
                print(f"[{idx}/{total}] DOWNLOAD: {remote_file}")
                self.scanner.download_file(remote_file, local_path)

                print(f"[{idx}/{total}] PROCESS")
                self.process_file(local_path)

            except Exception as e:
                print(f"[ERROR] {remote_file} / {e}")

            finally:
                self.cleanup(local_path)

        self.scanner.close()
