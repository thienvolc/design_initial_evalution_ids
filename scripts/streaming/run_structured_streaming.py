from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.runtime.structured_streaming_job import (  # noqa: E402
    StructuredStreamingJobOptions,
    run_structured_streaming_job,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Spark Structured Streaming IDS baseline")
    parser.add_argument("--config", type=str, default="configs/streaming/online.yaml")
    parser.add_argument("--model", type=str, default="logistic_regression")
    parser.add_argument("--feature-set", choices=["full", "reduced"], default="full")
    parser.add_argument("--run-tag", type=str, default="")
    parser.add_argument("--input-run-tag", type=str, default="")
    parser.add_argument("--load-profile", type=str, default="")

    parser.add_argument("--override-max-offsets", type=int, default=0)
    parser.add_argument("--override-shuffle-partitions", type=int, default=0)
    parser.add_argument("--override-trigger-interval", type=str, default="")
    parser.add_argument("--override-starting-offsets", type=str, default="")

    parser.add_argument("--override-watermark-delay-sec", type=int, default=-1)
    parser.add_argument("--drop-late-events", action="store_true")

    parser.add_argument("--reset-checkpoint", action="store_true")
    parser.add_argument("--available-now", action="store_true")
    parser.add_argument("--run-seconds", type=int, default=0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return run_structured_streaming_job(
        StructuredStreamingJobOptions(
            config_path=args.config,
            model_name=args.model,
            feature_set=args.feature_set,
            run_tag=args.run_tag,
            input_run_tag=args.input_run_tag,
            load_profile=args.load_profile,

            override_max_offsets=args.override_max_offsets,
            override_shuffle_partitions=args.override_shuffle_partitions,
            override_trigger_interval=args.override_trigger_interval,
            override_starting_offsets=args.override_starting_offsets,

            override_watermark_delay_sec=args.override_watermark_delay_sec,
            drop_late_events=args.drop_late_events,

            reset_checkpoint=args.reset_checkpoint,
            available_now=args.available_now,
            run_seconds=args.run_seconds,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
