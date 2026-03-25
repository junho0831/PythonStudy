import argparse
from batch_runner import BatchRunner
from local_test_settings import (
    LOCAL_FTP_HOST,
    LOCAL_FTP_PASSWORD,
    LOCAL_FTP_PORT,
    LOCAL_FTP_ROOT_PATH,
    LOCAL_FTP_USERNAME,
    LOCAL_OUTPUT_DIR,
    LOCAL_TEMP_DIR,
)


def build_parser():
    parser = argparse.ArgumentParser(description="FTP 날짜 폴더 스캔 및 파일 처리")
    parser.add_argument("--input-date", required=True, help="YYYY-MM-DD 형식")
    parser.add_argument("--parser", required=True, help="rubi 또는 rupi(rubp)")
    parser.add_argument("--host", default=LOCAL_FTP_HOST)
    parser.add_argument("--port", type=int, default=LOCAL_FTP_PORT)
    parser.add_argument("--username", default=LOCAL_FTP_USERNAME)
    parser.add_argument("--password", default=LOCAL_FTP_PASSWORD)
    parser.add_argument("--root-path", default=LOCAL_FTP_ROOT_PATH)
    parser.add_argument("--temp-dir", default=str(LOCAL_TEMP_DIR))
    parser.add_argument("--output-dir", default=str(LOCAL_OUTPUT_DIR))
    parser.add_argument("--scale-percent", type=int, default=50)
    parser.add_argument("--active", action="store_true")
    return parser


def main():
    args = build_parser().parse_args()
    runner = BatchRunner(
        input_date=args.input_date,
        parser_name=args.parser,
        host=args.host,
        port=args.port,
        username=args.username,
        password=args.password,
        root_path=args.root_path,
        temp_dir=args.temp_dir,
        output_dir=args.output_dir,
        scale_percent=args.scale_percent,
        passive=not args.active,
    )
    runner.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
