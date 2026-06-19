from __future__ import annotations

import argparse
import os

from batch_main.main import Main


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument("--date", dest="input_date", help="batch date in YYYY-MM-DD format")
    parser.add_argument(
        "--parser",
        dest="parser_name",
        type=str.upper,
        choices=("RUBI", "RUPI", "COMBINED", "ER_DOSE_RAW", "ER_DOSE_EUV"),
        help="RUBI, RUPI, COMBINED, ER_DOSE_RAW, or ER_DOSE_EUV",
    )
    return parser


def main(argv=None, env=None) -> int:
    parsed = build_parser().parse_args(argv)
    runtime_env = dict(os.environ if env is None else env)

    if parsed.input_date and parsed.parser_name:
        if parsed.parser_name == "ER_DOSE_RAW":
            runtime_env["BATCH_TARGET"] = "ER_DOSE_RAW"
            runtime_env["ER_DOSE_RAW_TARGET_DATE"] = parsed.input_date
        elif parsed.parser_name == "ER_DOSE_EUV":
            runtime_env["BATCH_TARGET"] = "ER_DOSE_EUV"
            runtime_env["ER_DOSE_EUV_TARGET_DATE"] = parsed.input_date
        else:
            runtime_env["BATCH_TARGET"] = "RBI"
            runtime_env["RBI_INPUT_DATE"] = parsed.input_date
            runtime_env["RBI_PARSER"] = parsed.parser_name

    return Main(env=runtime_env).run()


if __name__ == "__main__":
    raise SystemExit(main())

