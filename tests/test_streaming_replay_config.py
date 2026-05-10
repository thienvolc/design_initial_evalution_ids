from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.replay.config import (
    build_replay_command,
    estimate_stream_runtime,
    parse_rate_schedule,
    schedule_total_seconds,
)


class ReplayConfigTests(unittest.TestCase):
    def test_parse_rate_schedule_parses_multiple_steps(self) -> None:
        schedule = parse_rate_schedule("100:10, 250:20")
        self.assertEqual(schedule, [(100.0, 10.0), (250.0, 20.0)])

    def test_parse_rate_schedule_rejects_invalid_shape(self) -> None:
        with self.assertRaises(ValueError):
            parse_rate_schedule("100")

    def test_schedule_total_seconds_sums_durations(self) -> None:
        self.assertEqual(schedule_total_seconds([(100.0, 10.0), (250.0, 20.0)]), 30.0)

    def test_estimate_stream_runtime_prefers_schedule_then_buffer(self) -> None:
        runtime_seconds = estimate_stream_runtime(
            max_rows=1000,
            trace_rows_per_sec=0.0,
            trace_schedule=[(100.0, 10.0), (250.0, 20.0)],
            override_seconds=0,
            schedule_buffer_seconds=15,
            default_seconds=300,
        )
        self.assertEqual(runtime_seconds, 45)

    def test_build_replay_command_uses_rate_schedule_over_rows_per_sec(self) -> None:
        command = build_replay_command(
            python_exe="python",
            config="configs/streaming/streaming.yaml",
            run_tag="run-1",
            max_rows=1000,
            batch_size=200,
            rows_per_sec=123.0,
            rate_schedule="100:10",
            input_parquet="data/test.parquet",
            trace_order_column="event_time",
        )
        self.assertIn("--rate-schedule", command)
        self.assertNotIn("--rows-per-sec", command)
        self.assertEqual(command[0:2], ["python", "scripts/streaming/official/replay_parquet_to_kafka.py"])

    def test_build_replay_command_includes_force_sort_flag_when_requested(self) -> None:
        command = build_replay_command(
            python_exe="python",
            config="configs/streaming/streaming.yaml",
            run_tag="run-1",
            max_rows=1000,
            batch_size=200,
            force_sort_input=True,
        )
        self.assertIn("--force-sort-input", command)


if __name__ == "__main__":
    unittest.main()

