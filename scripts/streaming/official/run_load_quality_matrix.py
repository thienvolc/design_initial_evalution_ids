from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.replay.config import parse_rate_schedule  # noqa: E402
from ids_platform.streaming.evaluation.matrices.load_quality_matrix import LoadQualityMatrixOptions, run  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run detection quality under load matrix")
    parser.add_argument("--config", type=str, default="configs/streaming/streaming.yaml")
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
        default="artifacts/streaming/evaluation/load_quality_summary.csv",
    )
    parser.add_argument(
        "--allow-overwrite-summary",
        action="store_true",
        help="Allow overwriting an existing summary CSV. By default the runner fails fast.",
    )
    return parser.parse_args()


def _format_float(value: float) -> str:
    text = f"{value:.2f}"
    return text.rstrip("0").rstrip(".")


def _validate_args(args: argparse.Namespace) -> None:
    summary_path = PROJECT_ROOT / args.summary_csv
    if summary_path.exists() and not args.allow_overwrite_summary:
        raise SystemExit(
            "Refusing to overwrite existing summary CSV: "
            f"{summary_path}. Use a new --summary-csv path or pass --allow-overwrite-summary."
        )

    trace_schedule_raw = args.trace_rate_schedule.strip()
    if trace_schedule_raw:
        if args.trace_max_rows <= 0:
            raise SystemExit("--trace-max-rows must be > 0 when --trace-rate-schedule is provided.")

        schedule = parse_rate_schedule(
            trace_schedule_raw,
            error_message="trace rate schedule must be rps:seconds,rps:seconds",
        )
        expected_rows = sum(rps * seconds for rps, seconds in schedule)
        if expected_rows <= 0:
            raise SystemExit("The provided --trace-rate-schedule yields zero expected rows.")

        relative_gap = abs(expected_rows - args.trace_max_rows) / expected_rows
        if relative_gap > 0.05:
            print(
                "[warn] trace-rate-schedule and trace-max-rows are materially different: "
                f"the schedule implies about {_format_float(expected_rows)} rows, "
                f"while --trace-max-rows caps replay at {args.trace_max_rows}. "
                "This is allowed, but the replay will stop early at max_rows.",
                flush=True,
            )

    warmup_schedule_raw = args.warmup_rate_schedule.strip()
    if warmup_schedule_raw and args.warmup_rows <= 0:
        raise SystemExit("--warmup-rows must be > 0 when --warmup-rate-schedule is provided.")

    if args.warmup_rows > 0 and args.warmup_rows_per_sec <= 0 and not warmup_schedule_raw:
        # Keep current fallback behavior but make it explicit for the operator.
        print(
            "[warn] warmup_rows is set without warmup rate input; "
            "the matrix will reuse min(main rows_per_sec, 5000.0) for warmup replay.",
            flush=True,
        )


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
    parsed_args = parse_args()
    _validate_args(parsed_args)
    raise SystemExit(run(build_options(parsed_args)))

