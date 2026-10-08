from __future__ import annotations

import argparse
from pathlib import Path

from sales_etl.config import (
    DEFAULT_APP_NAME,
    DEFAULT_BASE_PATH,
    DEFAULT_INPUT_PATH,
    DEFAULT_LOG_DIR,
)
from sales_etl.etl_pipeline import SalesETLPipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the sales ETL pipeline.")
    parser.add_argument(
        "--input-path",
        default=DEFAULT_INPUT_PATH,
        help="Input sales CSV path.",
    )
    parser.add_argument(
        "--base-path",
        default=DEFAULT_BASE_PATH,
        help="Base path for the medallion data lake.",
    )
    parser.add_argument(
        "--log-dir",
        type=Path,
        default=DEFAULT_LOG_DIR,
        help="Directory for pipeline.log.",
    )
    parser.add_argument("--app-name", default=DEFAULT_APP_NAME, help="Spark application name.")
    args = parser.parse_args()

    pipeline = SalesETLPipeline(
        base_path=args.base_path,
        app_name=args.app_name,
        input_path=args.input_path,
        log_dir=args.log_dir,
    )
    try:
        pipeline.run()
    finally:
        pipeline.close()


if __name__ == "__main__":
    main()
