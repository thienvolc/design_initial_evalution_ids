from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.replay.runner import ReplayJobOptions, run_replay_job  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay parquet rows to Kafka raw topic")
    parser.add_argument("--config", type=str, default="configs/streaming/online.yaml")
    parser.add_argument("--bootstrap-servers", type=str, default=None)
    parser.add_argument("--topic", type=str, default=None)
    parser.add_argument("--run-tag", type=str, default="")

    parser.add_argument("--input-parquet", type=str, default=None)
    parser.add_argument("--trace-order-column", type=str, default="event_time")
    parser.add_argument("--force-sort-input", action="store_true")
    parser.add_argument("--max-rows", type=int, default=0)

    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--rows-per-sec", type=float, default=0.0)
    parser.add_argument("--rate-schedule", type=str, default="")

    parser.add_argument("--reorder-window-size", type=int, default=0)
    parser.add_argument("--late-event-ratio", type=float, default=0.0)
    parser.add_argument("--late-event-max-sec", type=float, default=0.0)
    parser.add_argument("--random-seed", type=int, default=42)

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return run_replay_job(
        ReplayJobOptions(
            config_path=args.config,
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            run_tag=args.run_tag,

            input_parquet=args.input_parquet,
            trace_order_column=args.trace_order_column,
            force_sort_input=args.force_sort_input,
            max_rows=args.max_rows,

            batch_size=args.batch_size,
            rows_per_sec=args.rows_per_sec,
            rate_schedule=args.rate_schedule,

            reorder_window_size=args.reorder_window_size,
            late_event_ratio=args.late_event_ratio,
            late_event_max_sec=args.late_event_max_sec,
            random_seed=args.random_seed,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
