from __future__ import annotations

import argparse
import ftplib
import io
import logging
import posixpath

from remote_batch.common.file_rules import extract_file_datetime
from remote_batch.common.models import RemoteFile

LOGGER = logging.getLogger("remote_batch")


def create_ftp_client(args: argparse.Namespace) -> ftplib.FTP:
    client = ftplib.FTP()
    try:
        client.connect(host=args.ftp_host, port=args.ftp_port, timeout=10)
        client.login(user=args.ftp_username, passwd=args.ftp_password)
        client.set_pasv(args.ftp_passive)
    except OSError as exc:
        try:
            client.quit()
        except Exception:
            client.close()
        raise ConnectionError(
            f"FTP 접속 실패: {args.ftp_host}:{args.ftp_port}에 연결할 수 없습니다. "
            "원격 서버 주소/포트 또는 io_mode 설정(local/remote)을 확인하세요."
        ) from exc
    except ftplib.all_errors as exc:
        try:
            client.quit()
        except Exception:
            client.close()
        raise ConnectionError(
            f"FTP 로그인 실패: {args.ftp_host}:{args.ftp_port} user={args.ftp_username}"
        ) from exc
    return client


def list_remote_files(
    ftp: ftplib.FTP,
    base_dir: str,
    folder_names: list[str],
    extension: str,
    source_type: str,
) -> list[RemoteFile]:
    files: list[RemoteFile] = []
    original_dir = ftp.pwd()
    for folder_name in folder_names:
        folder_path = posixpath.join(base_dir, folder_name)
        try:
            ftp.cwd(folder_path)
            entry_names = ftp.nlst()
        except ftplib.error_perm as exc:
            LOGGER.info("원격 폴더가 없어 건너뜁니다: %s (%s)", folder_path, exc)
            continue
        finally:
            ftp.cwd(original_dir)

        for entry_name in entry_names:
            file_name = posixpath.basename(entry_name)
            if not file_name.lower().endswith(extension.lower()):
                continue
            file_path = posixpath.join(folder_path, file_name)
            try:
                ftp.size(file_path)
            except ftplib.all_errors:
                continue
            file_datetime = extract_file_datetime(file_name)
            if file_datetime is None:
                LOGGER.warning("파일명 datetime 파싱 실패로 skip: %s", file_name)
                continue
            files.append(
                RemoteFile(
                    source_type=source_type,
                    file_name=file_name,
                    file_path=file_path,
                    file_datetime=file_datetime,
                )
            )
    return sorted(files, key=lambda item: (item.file_datetime, item.file_name))


def read_remote_text_file(ftp: ftplib.FTP, remote_path: str) -> str:
    raw_bytes = read_remote_binary_file(ftp, remote_path)
    for encoding in ("utf-8", "cp949"):
        try:
            return raw_bytes.decode(encoding)
        except UnicodeDecodeError:
            continue
    LOGGER.warning("utf-8/cp949 디코딩 실패. replace 모드로 진행합니다: %s", remote_path)
    return raw_bytes.decode("utf-8", errors="replace")


def read_remote_binary_file(ftp: ftplib.FTP, remote_path: str) -> bytes:
    buffer = io.BytesIO()
    ftp.retrbinary(f"RETR {remote_path}", buffer.write)
    return buffer.getvalue()


def ensure_remote_dir(ftp: ftplib.FTP, remote_dir: str) -> None:
    current = ""
    for part in posixpath.normpath(remote_dir).split("/"):
        if not part:
            continue
        current = f"{current}/{part}" if current else f"/{part}"
        try:
            ftp.mkd(current)
        except ftplib.error_perm as exc:
            if not str(exc).startswith("550"):
                raise


def write_remote_binary_file(ftp: ftplib.FTP, remote_path: str, content: bytes) -> None:
    ensure_remote_dir(ftp, posixpath.dirname(remote_path))
    with io.BytesIO(content) as buffer:
        ftp.storbinary(f"STOR {remote_path}", buffer)
