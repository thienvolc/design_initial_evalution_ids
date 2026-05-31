from __future__ import annotations

import random
import sys
import unittest
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.replay.config import ReplayTimingConfig
from ids_platform.streaming.replay.perturbation import ReplayPerturbation


class ReplayPerturbationTests(unittest.TestCase):
    def test_noop_perturbation_returns_frame_unchanged(self) -> None:
        frame = pd.DataFrame(
            {
                "flow_id": ["a", "b"],
                "event_time": ["2026-01-01T00:00:00Z", "2026-01-01T00:00:01Z"],
            }
        )

        result = ReplayPerturbation(
            ReplayTimingConfig(
                random_seed=42,
                trace_order_column="event_time",
                reorder_window_size=0,
                late_event_ratio=0.0,
                late_event_max_sec=0.0,
            )
        ).apply(frame, chunk_index=1, rng=random.Random(42))

        self.assertTrue(result.equals(frame))

    def test_lateness_moves_event_time_back(self) -> None:
        frame = pd.DataFrame(
            {
                "flow_id": ["a"],
                "event_time": ["2026-01-01T00:00:10Z"],
            }
        )

        result = ReplayPerturbation(
            ReplayTimingConfig(
                random_seed=42,
                trace_order_column="event_time",
                reorder_window_size=0,
                late_event_ratio=1.0,
                late_event_max_sec=1.0,
            )
        ).apply(
            frame,
            chunk_index=1,
            rng=random.Random(42),
        )

        self.assertLess(pd.Timestamp(result.loc[0, "event_time"]), pd.Timestamp(frame.loc[0, "event_time"]))


if __name__ == "__main__":
    unittest.main()
