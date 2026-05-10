from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.evaluation.matrices.watermark_matrix import WatermarkMatrixOptions, run  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run event-time watermark matrix")
    parser.add_argument("--config", type=str, default="configs/streaming/streaming.yaml")
    parser.add_argument("--model", type=str, default="logistic_regression")
    parser.add_argument("--feature-set", type=str, default="full")
    parser.add_argument("--watermark-delays", nargs="*", default=["0", "10", "30", "60"])
    parser.add_argument("--drop-late-events", action="store_true")
    parser.add_argument("--trace-input-parquet", type=str, default="data/gold/splits/test.parquet")
    parser.add_argument("--trace-order-column", type=str, default="event_time")
    parser.add_argument("--trace-rows-per-sec", type=float, default=0.0)
    parser.add_argument("--trace-rate-schedule", type=str, default="")
    parser.add_argument("--reorder-window-size", type=int, default=0)
    parser.add_argument("--late-event-ratio", type=float, default=0.0)
    parser.add_argument("--late-event-max-sec", type=float, default=0.0)
    parser.add_argument("--random-seed", type=int, default=42)
    parser.add_argument("--max-rows", type=int, default=2000)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--rows-per-sec", type=float, default=0.0)
    parser.add_argument("--warmup-rows", type=int, default=0)
    parser.add_argument("--warmup-rows-per-sec", type=float, default=0.0)
    parser.add_argument("--warmup-rate-schedule", type=str, default="")
    parser.add_argument(
        "--trace-stream-run-seconds",
        type=int,
        default=0,
        help="Stream runtime for trace mode; 0 means auto from replay profile",
    )
    parser.add_argument(
        "--warmup-stream-run-seconds",
        type=int,
        default=0,
        help="Warmup stream runtime in trace mode; 0 means auto from warmup replay profile",
    )
    parser.add_argument("--trace-startup-wait-sec", type=int, default=8)
    parser.add_argument("--metrics-timeout-sec", type=int, default=90)
    parser.add_argument("--metrics-idle-sec", type=int, default=15)
    parser.add_argument(
        "--summary-csv",
        type=str,
        default="artifacts/streaming/evaluation/watermark_summary.csv",
    )
    return parser.parse_args()


def build_options(args: argparse.Namespace) -> WatermarkMatrixOptions:
    return WatermarkMatrixOptions(
        config=args.config,
        model=args.model,
        feature_set=args.feature_set,
        watermark_delays=tuple(args.watermark_delays),
        drop_late_events=args.drop_late_events,
        trace_input_parquet=args.trace_input_parquet,
        trace_order_column=args.trace_order_column,
        trace_rows_per_sec=args.trace_rows_per_sec,
        trace_rate_schedule=args.trace_rate_schedule,
        reorder_window_size=args.reorder_window_size,
        late_event_ratio=args.late_event_ratio,
        late_event_max_sec=args.late_event_max_sec,
        random_seed=args.random_seed,
        max_rows=args.max_rows,
        batch_size=args.batch_size,
        rows_per_sec=args.rows_per_sec,
        warmup_rows=args.warmup_rows,
        warmup_rows_per_sec=args.warmup_rows_per_sec,
        warmup_rate_schedule=args.warmup_rate_schedule,
        trace_stream_run_seconds=args.trace_stream_run_seconds,
        warmup_stream_run_seconds=args.warmup_stream_run_seconds,
        trace_startup_wait_sec=args.trace_startup_wait_sec,
        metrics_timeout_sec=args.metrics_timeout_sec,
        metrics_idle_sec=args.metrics_idle_sec,
        summary_csv=args.summary_csv,
    )


if __name__ == "__main__":
    raise SystemExit(run(build_options(parse_args())))

