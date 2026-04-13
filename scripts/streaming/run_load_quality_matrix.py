from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.matrices.load_quality_matrix import LoadQualityMatrixOptions, run  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run detection quality under load matrix")
    parser.add_argument("--config", type=str, default="configs/streaming/online.yaml")
    parser.add_argument("--model", type=str, default="logistic_regression")
    parser.add_argument("--feature-set", type=str, default="full")
    parser.add_argument(
        "--load-profiles",
        nargs="*",
        default=["low:100:1000", "medium:400:2000", "high:800:3000"],
        help="Format: name:rows_per_sec:max_rows",
    )
    parser.add_argument(
        "--trace-rate-schedule",
        type=str,
        default="",
        help="Optional single-run schedule: rps:seconds,rps:seconds",
    )
    parser.add_argument(
        "--trace-max-rows",
        type=int,
        default=0,
        help="Total rows for trace schedule mode (required if --trace-rate-schedule is set)",
    )
    parser.add_argument(
        "--trace-profile-name",
        type=str,
        default="trace_step_rate",
        help="Profile name used in summary row for trace schedule mode",
    )
    parser.add_argument(
        "--trace-stream-run-seconds",
        type=int,
        default=0,
        help="Stream runtime for trace schedule mode; 0 means auto from schedule duration + buffer",
    )
    parser.add_argument(
        "--trace-startup-wait-sec",
        type=int,
        default=8,
        help="Seconds to wait after stream start before replay begins in trace schedule mode",
    )
    parser.add_argument("--warmup-rows", type=int, default=0)
    parser.add_argument("--warmup-rows-per-sec", type=float, default=0.0)
    parser.add_argument(
        "--warmup-rate-schedule",
        type=str,
        default="",
        help="Optional warmup schedule rps:seconds,rps:seconds",
    )
    parser.add_argument(
        "--warmup-stream-run-seconds",
        type=int,
        default=0,
        help="Warmup stream runtime; 0 means auto from warmup replay profile",
    )
    parser.add_argument(
        "--warmup-startup-wait-sec",
        type=int,
        default=8,
        help="Seconds to wait after warmup stream start before warmup replay begins",
    )
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--metrics-timeout-sec", type=int, default=90)
    parser.add_argument("--metrics-idle-sec", type=int, default=8)
    parser.add_argument(
        "--summary-csv",
        type=str,
        default="artifacts/streaming/online/load_quality_summary.csv",
    )
    return parser.parse_args()


def build_options(args: argparse.Namespace) -> LoadQualityMatrixOptions:
    return LoadQualityMatrixOptions(
        config=args.config,
        model=args.model,
        feature_set=args.feature_set,
        load_profiles=tuple(args.load_profiles),
        trace_rate_schedule=args.trace_rate_schedule,
        trace_max_rows=args.trace_max_rows,
        trace_profile_name=args.trace_profile_name,
        trace_stream_run_seconds=args.trace_stream_run_seconds,
        trace_startup_wait_sec=args.trace_startup_wait_sec,
        warmup_rows=args.warmup_rows,
        warmup_rows_per_sec=args.warmup_rows_per_sec,
        warmup_rate_schedule=args.warmup_rate_schedule,
        warmup_stream_run_seconds=args.warmup_stream_run_seconds,
        warmup_startup_wait_sec=args.warmup_startup_wait_sec,
        batch_size=args.batch_size,
        metrics_timeout_sec=args.metrics_timeout_sec,
        metrics_idle_sec=args.metrics_idle_sec,
        summary_csv=args.summary_csv,
    )


if __name__ == "__main__":
    raise SystemExit(run(build_options(parse_args())))

