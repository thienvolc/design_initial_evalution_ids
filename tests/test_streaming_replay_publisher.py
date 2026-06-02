from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.replay.config import ReplayRuntimeConfig
from ids_platform.streaming.replay.publisher import ReplayPublisher, ReplayRecordBuilder


class FakeProducer:
    def __init__(self, *, remaining_messages: int = 0) -> None:
        self.remaining_messages = remaining_messages
        self.produced: list[dict] = []

    def produce(self, **kwargs) -> None:
        self.produced.append(kwargs)

    def flush(self) -> int:
        return self.remaining_messages


class ReplayPublisherTests(unittest.TestCase):
    def test_build_replay_record_adds_replay_metadata(self) -> None:
        record = ReplayRecordBuilder("run-1").build_replay_record(
            pd.Series({"flow_id": "flow-3", "event_time": "2026-01-01T00:00:00Z"}),
            row_index=3,
        )

        self.assertEqual(record["flow_id"], "flow-3")
        self.assertEqual(record["replay_run_tag"], "run-1")
        self.assertEqual(record["replay_row_index"], 3)
        self.assertEqual(record["benchmark_phase"], "measure")
        self.assertIn("source_ingest_epoch_ms", record)

    def test_publisher_raises_when_flush_leaves_messages(self) -> None:
        with mock.patch(
            "ids_platform.streaming.replay.publisher.create_producer",
            return_value=FakeProducer(remaining_messages=1),
        ):
            publisher = ReplayPublisher(
                ReplayRuntimeConfig(
                    run_tag="run-1",
                    bootstrap_servers="kafka:29092",
                    topic="ids.raw.flows",
                )
            )

        with self.assertRaises(RuntimeError):
            publisher.flush_or_raise("test phase")


if __name__ == "__main__":
    unittest.main()
