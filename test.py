import argparse
from batch_runner import BatchRunner


def build_parser():
    parser = argparse.ArgumentParser(description="FTP 날짜 폴더 스캔 및 파일 처리")
    parser.add_argument("--input-date", required=True, help="YYYY-MM-DD 형식")
    parser.add_argument("--parser", required=True, help="rubi 또는 rupi(rubp)")
    parser.add_argument("--host", default="아이피")
    parser.add_argument("--port", type=int, default=21)
    parser.add_argument("--username", default="mbeat2024")
    parser.add_argument("--password", default="ftp.utils")
    parser.add_argument("--root-path", default="/RUIP")
    parser.add_argument("--temp-dir", default="/tmp/ftp_work")
    parser.add_argument("--output-dir", default="/tmp/ftp_output")
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
