from __future__ import annotations

from io import BytesIO
from ftplib import FTP, all_errors
from pathlib import Path, PurePosixPath


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
        try:
            entries = list(self.ftp.mlsd(target_dir))
        except all_errors as exc:
            print(f"[WARN] 디렉토리 조회 실패: {target_dir} / {exc}")
            return []
        return [
            f"{target_dir}/{name}"
            for name, facts in entries
            if facts.get("type") == "file"
        ]

    def _download_with_validation(self, remote_path, write_chunk):
        expected_size = self.get_file_size(remote_path)
        downloaded_size = 0

        def tracked_write(chunk):
            nonlocal downloaded_size
            downloaded_size += len(chunk)
            write_chunk(chunk)

        self.ftp.retrbinary(f"RETR {remote_path}", tracked_write)

        if expected_size is not None and downloaded_size != expected_size:
            raise IOError(
                f"다운로드 크기 불일치: remote={expected_size}, downloaded={downloaded_size}, path={remote_path}"
            )

    def download_file(self, remote_path, local_path):
        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(local_path, "wb") as file_obj:
                self._download_with_validation(remote_path, file_obj.write)
        except Exception:
            try:
                local_path.unlink()
            except FileNotFoundError:
                pass
            raise

    def download_bytes(self, remote_path):
        buffer = BytesIO()
        self._download_with_validation(remote_path, buffer.write)
        return buffer.getvalue()

    def read_text_file(self, remote_path, *, encoding="utf-8"):
        try:
            return self.download_bytes(remote_path).decode(encoding)
        except all_errors:
            return ""

    def _ensure_remote_dir(self, remote_dir):
        current = PurePosixPath("/")
        for part in PurePosixPath(remote_dir).parts:
            if part in {"", "/"}:
                continue
            current = current / part
            try:
                self.ftp.mkd(current.as_posix())
            except all_errors:
                pass

    def upload_file(self, local_path, remote_path):
        local_path = Path(local_path)
        self._ensure_remote_dir(PurePosixPath(remote_path).parent.as_posix())
        with local_path.open("rb") as file_obj:
            self.ftp.storbinary(f"STOR {remote_path}", file_obj)

    def get_file_size(self, remote_path):
        try:
            size = self.ftp.size(remote_path)
        except all_errors:
            return None
        return int(size) if size is not None else None

    def file_exists(self, remote_path):
        return self.get_file_size(remote_path) is not None

    def delete_file(self, remote_path):
        self.ftp.delete(remote_path)

    def append_text_line(self, remote_path, line, *, encoding="utf-8"):
        self._ensure_remote_dir(PurePosixPath(remote_path).parent.as_posix())
        payload = BytesIO((line.rstrip("\n") + "\n").encode(encoding))
        self.ftp.storbinary(f"APPE {remote_path}", payload)

    def close(self):
        try:
            self.ftp.quit()
        except Exception:
            pass
