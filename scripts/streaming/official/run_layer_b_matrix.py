from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.evaluation.matrices.layer_b_matrix import LayerBMatrixOptions, run  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run streaming Layer B model-cost matrix")
    parser.add_argument("--config", type=str, default="configs/streaming/streaming.yaml")
    parser.add_argument("--models", nargs="*", default=["logistic_regression", "random_forest", "gradient_boosting"])
    parser.add_argument("--feature-sets", nargs="*", default=["full", "reduced"])
    parser.add_argument(
        "--model-feature-pairs",
        nargs="*",
        default=[],
        help="Optional explicit pairs in format model:feature_set; if provided, overrides cartesian product of --models and --feature-sets",
    )
    parser.add_argument("--max-rows", type=int, default=1000)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--run-prefix", type=str, default="layerB")
    parser.add_argument("--metrics-timeout-sec", type=int, default=45)
    parser.add_argument("--metrics-idle-sec", type=int, default=5)
    parser.add_argument("--trace-input-parquet", type=str, default="data/gold/splits/test.parquet")
    parser.add_argument("--trace-order-column", type=str, default="event_time")
    parser.add_argument("--trace-rows-per-sec", type=float, default=0.0)
    parser.add_argument(
        "--trace-rate-schedule",
        type=str,
        default="",
        help="Optional step-rate schedule rps:seconds,rps:seconds",
    )
    parser.add_argument(
        "--trace-stream-run-seconds",
        type=int,
        default=0,
        help="Stream runtime for trace mode; 0 means auto from replay profile",
    )
    parser.add_argument("--trace-startup-wait-sec", type=int, default=8)
    parser.add_argument("--warmup-rows", type=int, default=0)
    parser.add_argument("--warmup-rows-per-sec", type=float, default=0.0)
    parser.add_argument(
        "--warmup-rate-schedule",
        type=str,
        default="",
        help="Optional warmup step-rate schedule rps:seconds,rps:seconds",
    )
    parser.add_argument(
        "--warmup-stream-run-seconds",
        type=int,
        default=0,
        help="Warmup stream runtime in trace mode; 0 means auto from warmup replay profile",
    )
    parser.add_argument(
        "--summary-csv",
        type=str,
        default="artifacts/streaming/evaluation/layer_b_summary.csv",
    )
    return parser.parse_args()


def build_options(args: argparse.Namespace) -> LayerBMatrixOptions:
    return LayerBMatrixOptions(
        config=args.config,
        models=tuple(args.models),
        feature_sets=tuple(args.feature_sets),
        model_feature_pairs=tuple(args.model_feature_pairs),
        max_rows=args.max_rows,
        batch_size=args.batch_size,
        repeats=args.repeats,
        run_prefix=args.run_prefix,
        metrics_timeout_sec=args.metrics_timeout_sec,
        metrics_idle_sec=args.metrics_idle_sec,
        trace_input_parquet=args.trace_input_parquet,
        trace_order_column=args.trace_order_column,
        trace_rows_per_sec=args.trace_rows_per_sec,
        trace_rate_schedule=args.trace_rate_schedule,
        trace_stream_run_seconds=args.trace_stream_run_seconds,
        trace_startup_wait_sec=args.trace_startup_wait_sec,
        warmup_rows=args.warmup_rows,
        warmup_rows_per_sec=args.warmup_rows_per_sec,
        warmup_rate_schedule=args.warmup_rate_schedule,
        warmup_stream_run_seconds=args.warmup_stream_run_seconds,
        summary_csv=args.summary_csv,
    )


if __name__ == "__main__":
    raise SystemExit(run(build_options(parse_args())))

