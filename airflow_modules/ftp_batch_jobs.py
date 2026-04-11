from __future__ import annotations

from ftp_batch.app.batch_runner import BatchRunner
from ftp_batch.config.local_test_settings import (
    CLIENT_FTP_HOST,
    CLIENT_FTP_PASSWORD,
    CLIENT_FTP_PORT,
    CLIENT_FTP_ROOT_PATH,
    CLIENT_FTP_USERNAME,
    FTP_PASSIVE_MODE,
    LOCAL_DB_PATH,
    LOCAL_WORK_DIR,
    RUPI_SCALE_PERCENT,
    SERVER_FTP_HOST,
    SERVER_FTP_PASSWORD,
    SERVER_FTP_PORT,
    SERVER_FTP_ROOT_PATH,
    SERVER_FTP_USERNAME,
    TEXT_FTP_ROOT_PATH,
)


def build_runner(input_date: str, parser_name: str) -> BatchRunner:
    return BatchRunner(
        input_date=input_date,
        parser_name=parser_name,
        client_host=CLIENT_FTP_HOST,
        client_port=CLIENT_FTP_PORT,
        client_username=CLIENT_FTP_USERNAME,
        client_password=CLIENT_FTP_PASSWORD,
        client_root_path=CLIENT_FTP_ROOT_PATH,
        text_root_path=TEXT_FTP_ROOT_PATH,
        server_host=SERVER_FTP_HOST,
        server_port=SERVER_FTP_PORT,
        server_username=SERVER_FTP_USERNAME,
        server_password=SERVER_FTP_PASSWORD,
        server_root_path=SERVER_FTP_ROOT_PATH,
        db_path=LOCAL_DB_PATH,
        work_dir=LOCAL_WORK_DIR,
        scale_percent=RUPI_SCALE_PERCENT,
        passive=FTP_PASSIVE_MODE,
    )


def run_batch(input_date: str, parser_name: str) -> None:
    runner = build_runner(input_date=input_date, parser_name=parser_name)
    runner.run()


def run_combined(input_date: str) -> None:
    run_batch(input_date=input_date, parser_name="COMBINED")


def run_rubi(input_date: str) -> None:
    run_batch(input_date=input_date, parser_name="RUBI")


def run_rupi(input_date: str) -> None:
    run_batch(input_date=input_date, parser_name="RUPI")
