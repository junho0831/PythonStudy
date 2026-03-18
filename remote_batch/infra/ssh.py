from __future__ import annotations

import argparse
import logging
import os
import stat
from pathlib import PurePosixPath

import paramiko

from remote_batch.common.file_rules import extract_file_datetime
from remote_batch.common.models import RemoteFile

LOGGER = logging.getLogger("remote_batch")


def create_ssh_client(args: argparse.Namespace) -> paramiko.SSHClient:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect(
            hostname=args.ssh_host,
            port=args.ssh_port,
            username=args.ssh_username,
            password=args.ssh_password or None,
            key_filename=args.ssh_key_file or None,
            look_for_keys=not bool(args.ssh_password),
            timeout=10,
        )
    except paramiko.ssh_exception.NoValidConnectionsError as exc:
        client.close()
        raise ConnectionError(
            f"SSH 접속 실패: {args.ssh_host}:{args.ssh_port}에 연결할 수 없습니다. "
            "원격 서버 주소/포트 또는 io_mode 설정(local/remote)을 확인하세요."
        ) from exc
    return client


def list_remote_files(
    sftp: paramiko.SFTPClient,
    base_dir: str,
    folder_names: list[str],
    extension: str,
    source_type: str,
) -> list[RemoteFile]:
    files: list[RemoteFile] = []
    for folder_name in folder_names:
        folder_path = str(PurePosixPath(base_dir) / folder_name)
        try:
            entries = sftp.listdir_attr(folder_path)
        except FileNotFoundError:
            LOGGER.info("원격 폴더가 없어 건너뜁니다: %s", folder_path)
            continue
        for entry in entries:
            if not stat.S_ISREG(entry.st_mode) or not entry.filename.lower().endswith(extension):
                continue
            file_datetime = extract_file_datetime(entry.filename)
            if file_datetime is None:
                LOGGER.warning("파일명 datetime 파싱 실패로 skip: %s", entry.filename)
                continue
            files.append(
                RemoteFile(
                    source_type=source_type,
                    file_name=entry.filename,
                    file_path=str(PurePosixPath(folder_path) / entry.filename),
                    file_datetime=file_datetime,
                )
            )
    return sorted(files, key=lambda item: (item.file_datetime, item.file_name))


def read_remote_text_file(sftp: paramiko.SFTPClient, remote_path: str) -> str:
    with sftp.open(remote_path, "rb") as remote_file:
        raw_bytes = remote_file.read()
    for encoding in ("utf-8", "cp949"):
        try:
            return raw_bytes.decode(encoding)
        except UnicodeDecodeError:
            continue
    LOGGER.warning("utf-8/cp949 디코딩 실패. replace 모드로 진행합니다: %s", remote_path)
    return raw_bytes.decode("utf-8", errors="replace")


def read_remote_binary_file(sftp: paramiko.SFTPClient, remote_path: str) -> bytes:
    with sftp.open(remote_path, "rb") as remote_file:
        return remote_file.read()


def ensure_remote_dir(sftp: paramiko.SFTPClient, remote_dir: str) -> None:
    current = PurePosixPath("/")
    for part in PurePosixPath(remote_dir).parts:
        if part == "/":
            continue
        current = current / part
        try:
            sftp.stat(str(current))
        except FileNotFoundError:
            sftp.mkdir(str(current))


def write_remote_binary_file(sftp: paramiko.SFTPClient, remote_path: str, content: bytes) -> None:
    ensure_remote_dir(sftp, os.path.dirname(remote_path))
    with sftp.open(remote_path, "wb") as remote_file:
        remote_file.write(content)
