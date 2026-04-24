from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.matrices.layer_c_fault_matrix import LayerCFaultMatrixOptions, run  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run online Layer C fault matrix")
    parser.add_argument("--config", type=str, default="configs/streaming/online.yaml")
    parser.add_argument("--model", type=str, default="logistic_regression")
    parser.add_argument("--feature-set", type=str, default="full")
    parser.add_argument("--execution-mode", type=str, default="docker", choices=["host", "docker"])
    parser.add_argument("--python-exe", type=str, default=sys.executable)
    parser.add_argument("--bootstrap-servers", type=str, default="")
    parser.add_argument(
        "--scenarios",
        nargs="*",
        default=["kafka_restart", "spark_process_restart", "producer_restart", "network_slowdown"],
        choices=["kafka_restart", "spark_process_restart", "producer_restart", "network_slowdown"],
    )
    parser.add_argument("--warmup-rows", type=int, default=1000)
    parser.add_argument("--warmup-rows-per-sec", type=float, default=0.0)
    parser.add_argument("--warmup-rate-schedule", type=str, default="")
    parser.add_argument("--fault-delay-sec", type=int, default=0)
    parser.add_argument("--post-fault-rows", type=int, default=1000)
    parser.add_argument("--post-fault-rows-per-sec", type=float, default=0.0)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--trace-input-parquet", type=str, default="data/gold/splits/test.parquet")
    parser.add_argument("--trace-order-column", type=str, default="event_time")
    parser.add_argument("--slowdown-rows-per-sec", type=float, default=50.0)
    parser.add_argument("--producer-restart-pause-sec", type=int, default=5)
    parser.add_argument("--replay-retries", type=int, default=5)
    parser.add_argument("--replay-retry-wait-sec", type=int, default=3)
    parser.add_argument("--stream-run-seconds", type=int, default=180)
    parser.add_argument("--startup-wait-sec", type=int, default=60)
    parser.add_argument("--metrics-timeout-sec", type=int, default=120)
    parser.add_argument("--summary-csv", type=str, default="artifacts/streaming/online/layer_c_summary.csv")
    return parser.parse_args()


def build_options(args: argparse.Namespace) -> LayerCFaultMatrixOptions:
    return LayerCFaultMatrixOptions(
        config=args.config,
        model=args.model,
        feature_set=args.feature_set,
        scenarios=tuple(args.scenarios),
        warmup_rows=args.warmup_rows,
        warmup_rows_per_sec=args.warmup_rows_per_sec,
        warmup_rate_schedule=args.warmup_rate_schedule,
        fault_delay_sec=args.fault_delay_sec,
        post_fault_rows=args.post_fault_rows,
        post_fault_rows_per_sec=args.post_fault_rows_per_sec,
        batch_size=args.batch_size,
        trace_input_parquet=args.trace_input_parquet,
        trace_order_column=args.trace_order_column,
        slowdown_rows_per_sec=args.slowdown_rows_per_sec,
        producer_restart_pause_sec=args.producer_restart_pause_sec,
        replay_retries=args.replay_retries,
        replay_retry_wait_sec=args.replay_retry_wait_sec,
        stream_run_seconds=args.stream_run_seconds,
        startup_wait_sec=args.startup_wait_sec,
        metrics_timeout_sec=args.metrics_timeout_sec,
        execution_mode=args.execution_mode,
        python_executable=args.python_exe,
        bootstrap_servers=args.bootstrap_servers,
        summary_csv=args.summary_csv,
    )


if __name__ == "__main__":
    raise SystemExit(run(build_options(parse_args())))

