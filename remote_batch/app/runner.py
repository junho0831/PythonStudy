from __future__ import annotations

import argparse
import logging
from pathlib import Path

from remote_batch.common.constants import SOURCE_RUBI_TXT, SOURCE_RUBP_TIF
from remote_batch.domains.rubp.service import process_rubp_file
from remote_batch.domains.rubi.service import process_rubi_file
from remote_batch.infra.db import connect_db
from remote_batch.infra.ftp import create_ftp_client, list_remote_files, read_remote_text_file
from remote_batch.infra.local_fs import list_local_files, read_local_text_file

LOGGER = logging.getLogger("remote_batch")


def _resolve_io_mode(args: argparse.Namespace) -> str:
    if args.io_mode in ("local", "remote"):
        return args.io_mode
    localhost_aliases = {"127.0.0.1", "localhost", "::1"}
    if args.ftp_host in localhost_aliases and Path(args.rubi_base_dir).exists():
        return "local"
    return "remote"


def run_batch(args: argparse.Namespace, *, build_recent_date_dirs) -> None:
    engine = connect_db(args.db_dsn)
    io_mode = _resolve_io_mode(args)
    folder_names = build_recent_date_dirs(args.days_back)
    ftp_client = None
    try:
        if io_mode == "local":
            LOGGER.info("I/O 모드: local (FTP 미사용)")
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
                    output_base_dir=args.rubp_output_base_dir,
                    scale_percent=args.rubp_scale_percent,
                    ftp=None,
                )
        else:
            LOGGER.info("I/O 모드: remote (FTP 사용)")
            ftp_client = create_ftp_client(args)
            rubi_files = list_remote_files(
                ftp=ftp_client,
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
                    read_text_file=lambda path: read_remote_text_file(ftp_client, path),
                )
            rubp_files = list_remote_files(
                ftp=ftp_client,
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
                    output_base_dir=args.rubp_output_base_dir,
                    scale_percent=args.rubp_scale_percent,
                    ftp=ftp_client,
                )
    finally:
        engine.dispose()
        if ftp_client is not None:
            try:
                ftp_client.quit()
            except Exception:
                ftp_client.close()
