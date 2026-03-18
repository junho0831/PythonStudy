from __future__ import annotations

import logging

import paramiko

LOGGER = logging.getLogger("remote_batch")


def process_tif_stub(
    ssh_client: paramiko.SSHClient,
    remote_file,
) -> None:
    _ = ssh_client
    LOGGER.info(
        "Rubp tif stub. 향후 서버 내 명령 실행으로 확장 예정: %s (%s)",
        remote_file.file_path,
        remote_file.file_datetime.isoformat(),
    )
