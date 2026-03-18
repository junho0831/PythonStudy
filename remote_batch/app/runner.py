from __future__ import annotations

import argparse
import logging
from pathlib import Path

from remote_batch.common.constants import SOURCE_RUBI_TXT, SOURCE_RUBP_TIF
from remote_batch.domains.rubp.service import process_rubp_file, process_tif_stub
from remote_batch.domains.rubi.service import process_rubi_file
from remote_batch.infra.db import connect_db
from remote_batch.infra.local_fs import list_local_files, read_local_text_file
from remote_batch.infra.ssh import create_ssh_client, list_remote_files, read_remote_text_file

LOGGER = logging.getLogger("remote_batch")


def _resolve_io_mode(args: argparse.Namespace) -> str:
    if args.io_mode in ("local", "remote"):
        return args.io_mode
    localhost_aliases = {"127.0.0.1", "localhost", "::1"}
    if args.ssh_host in localhost_aliases and Path(args.rubi_base_dir).exists():
        return "local"
    return "remote"


def run_batch(args: argparse.Namespace, *, build_recent_date_dirs) -> None:
    engine = connect_db(args.db_dsn)
    io_mode = _resolve_io_mode(args)
    folder_names = build_recent_date_dirs(args.days_back)
    ssh_client = None
    try:
        if io_mode == "local":
            LOGGER.info("I/O 모드: local (SSH 미사용)")
            rubi_files = list_local_files(
                base_dir=args.rubi_base_dir,
                folder_names=folder_names,
                extension=".txt",
                source_type=SOURCE_RUBI_TXT,
            )
            LOGGER.info("Rubi 대상 txt 파일 수: %s", len(rubi_files))
            for remote_file in rubi_files:
                process_rubi_file(
                    engine=engine,
                    remote_file=remote_file,
                    processing_timeout_minutes=args.processing_timeout_minutes,
                    read_text_file=read_local_text_file,
                )
            rubp_files = list_local_files(
                base_dir=args.rubp_base_dir,
                folder_names=folder_names,
                extension=".tif",
                source_type=SOURCE_RUBP_TIF,
            )
            LOGGER.info("Rubp 대상 tif 파일 수: %s", len(rubp_files))
            for remote_file in rubp_files:
                process_rubp_file(
                    engine=engine,
                    remote_file=remote_file,
                    processing_timeout_minutes=args.processing_timeout_minutes,
                    process_tif=process_tif_stub,
                    ssh_client=None,
                )
        else:
            LOGGER.info("I/O 모드: remote (SSH 사용)")
            ssh_client = create_ssh_client(args)
            with ssh_client.open_sftp() as sftp:
                rubi_files = list_remote_files(
                    sftp=sftp,
                    base_dir=args.rubi_base_dir,
                    folder_names=folder_names,
                    extension=".txt",
                    source_type=SOURCE_RUBI_TXT,
                )
                LOGGER.info("Rubi 대상 txt 파일 수: %s", len(rubi_files))
                for remote_file in rubi_files:
                    process_rubi_file(
                        engine=engine,
                        remote_file=remote_file,
                        processing_timeout_minutes=args.processing_timeout_minutes,
                        read_text_file=lambda path: read_remote_text_file(sftp, path),
                    )
                rubp_files = list_remote_files(
                    sftp=sftp,
                    base_dir=args.rubp_base_dir,
                    folder_names=folder_names,
                    extension=".tif",
                    source_type=SOURCE_RUBP_TIF,
                )
                LOGGER.info("Rubp 대상 tif 파일 수: %s", len(rubp_files))
                for remote_file in rubp_files:
                    process_rubp_file(
                        engine=engine,
                        remote_file=remote_file,
                        processing_timeout_minutes=args.processing_timeout_minutes,
                        process_tif=process_tif_stub,
                        ssh_client=ssh_client,
                    )
    finally:
        engine.dispose()
        if ssh_client is not None:
            ssh_client.close()
