from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest import mock

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.replay.config import (
    ReplayConfig,
    ReplayRuntimeConfig,
    ReplayRatePlan,
    ReplaySource,
    ReplayTimingConfig,
)
from ids_platform.streaming.replay.runner import run_replay_job


class ReplayRunnerTests(unittest.TestCase):
    def test_run_replay_job_orchestrates_resolve_load_publish(self) -> None:
        source = ReplaySource(table=mock.Mock(), batch_size=5000)
        config = ReplayConfig(
            source=source,
            runtime=ReplayRuntimeConfig(
                run_tag="run-1",
                bootstrap_servers="kafka:29092",
                topic="ids.raw.flows",
            ),
            rate=ReplayRatePlan(rows_per_sec=0.0, schedule=()),
            timing=ReplayTimingConfig(
                random_seed=42,
                trace_order_column="event_time",
                reorder_window_size=0,
                late_event_ratio=0.0,
                late_event_max_sec=0.0,
            ),
        )

        with (
            mock.patch("ids_platform.streaming.replay.runner.ReplayPublisher") as publisher_cls,
            mock.patch("ids_platform.streaming.replay.runner.ReplayRecordBuilder") as builder_cls,
        ):
            config.source.table.to_batches.return_value = []
            builder_cls.return_value.build_input_sentinel_record.return_value = {"flow_id": "sentinel"}
            result = run_replay_job(config)

        self.assertEqual(result, 0)
        publisher_cls.assert_called_once_with(config.runtime)
        builder_cls.assert_called_once_with("run-1", phase="measure")


if __name__ == "__main__":
    unittest.main()
