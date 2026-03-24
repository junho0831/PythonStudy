from __future__ import annotations

from ftplib import FTP, all_errors
from pathlib import Path


class FTPScanner:
    def __init__(self, host, port, username, password, root_path, *, passive=True, timeout=10):
        self.root_path = root_path.rstrip("/")
        self.ftp = FTP()
        self.ftp.connect(host, port, timeout=timeout)
        self.ftp.login(username, password)
        self.ftp.set_pasv(passive)

    def scan(self, date_str):
        target_dir = f"{self.root_path}/{date_str}"
        try:
            self.ftp.cwd(target_dir)
        except all_errors:
            print(f"[SKIP] 날짜 폴더 없음: {target_dir}")
            return []
        return self._scan_recursive(target_dir)

    def _scan_recursive(self, path):
        results = []
        try:
            entries = list(self.ftp.mlsd(path))
        except all_errors as exc:
            print(f"[WARN] 디렉토리 조회 실패: {path} / {exc}")
            return results

        for name, facts in entries:
            full_path = f"{path}/{name}"
            if facts.get("type") == "dir":
                results.extend(self._scan_recursive(full_path))
            elif facts.get("type") == "file":
                results.append(full_path)
        return results

    def download_file(self, remote_path, local_path):
        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        with open(local_path, "wb") as file_obj:
            self.ftp.retrbinary(f"RETR {remote_path}", file_obj.write)

    def close(self):
        try:
            self.ftp.quit()
        except Exception:
            pass
