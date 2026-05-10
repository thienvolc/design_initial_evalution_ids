from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.benchmark.runtime.pandas_udf_job import (  # noqa: E402
    PandasUdfBenchmarkOptions,
    run_pandas_udf_benchmark,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Joblib models with Spark pandas UDF")
    parser.add_argument("--config", type=str, default="configs/streaming/detect.yaml")
    parser.add_argument("--input-parquet", type=str, default=None)
    parser.add_argument("--max-rows", type=int, default=0)
    parser.add_argument("--batch-size", type=int, default=50000)
    parser.add_argument("--models", nargs="*", default=None)
    parser.add_argument("--feature-set", choices=["full", "reduced"], default="full")
    parser.add_argument("--output-parquet-dir", type=str, default=None)
    parser.add_argument("--summary-json", type=str, default=None)
    parser.add_argument("--run-tag", type=str, default="")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return run_pandas_udf_benchmark(
        PandasUdfBenchmarkOptions(
            config_path=args.config,
            input_parquet=args.input_parquet,
            max_rows=args.max_rows,
            batch_size=args.batch_size,
            models=tuple(args.models) if args.models else None,
            feature_set=args.feature_set,
            output_parquet_dir=args.output_parquet_dir,
            summary_json=args.summary_json,
            run_tag=args.run_tag,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())

