from __future__ import annotations

import sys
import unittest
import uuid
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.replay.config import (
    RateStep,
    ReplayRatePlan,
    ReplaySourceFactory,
)


class ReplayConfigTests(unittest.TestCase):
    def test_schedule_total_seconds_sums_durations(self) -> None:
        plan = ReplayRatePlan(
            rows_per_sec=0.0,
            schedule=(
                RateStep(rows_per_sec=100.0, duration_sec=10.0),
                RateStep(rows_per_sec=250.0, duration_sec=20.0),
            ),
        )

        self.assertEqual(plan.total_seconds(), 30.0)

    def test_rate_plan_estimates_constant_rate_elapsed(self) -> None:
        plan = ReplayRatePlan(rows_per_sec=100.0, schedule=())

        self.assertEqual(plan.expected_elapsed_for_rows(250), 2.5)

    def test_rate_plan_estimates_schedule_elapsed(self) -> None:
        plan = ReplayRatePlan(
            rows_per_sec=0.0,
            schedule=(
                RateStep(rows_per_sec=100.0, duration_sec=2.0),
                RateStep(rows_per_sec=50.0, duration_sec=10.0),
            )
        )

        self.assertEqual(plan.expected_elapsed_for_rows(300), 4.0)

    def test_source_factory_can_limit_rows_and_add_required_columns(self) -> None:
        temp_dir = PROJECT_ROOT / "artifacts" / "tmp_tests" / f"replay_source_{uuid.uuid4().hex}"
        temp_dir.mkdir(parents=True, exist_ok=False)
        dataset_path = temp_dir / "trace.parquet"
        pq.write_table(
            pa.table(
                {
                    "timestamp": [
                        "2024-01-01T00:00:00Z",
                        "2024-01-01T00:00:01Z",
                        "2024-01-01T00:00:02Z",
                    ],
                    "feature_1": [1.0, 2.0, 3.0],
                }
            ),
            dataset_path,
        )

        source = ReplaySourceFactory(
            dataset_path=dataset_path,
            batch_size=2,
            row_limit=2,
        ).create()

        self.assertEqual(source.table.num_rows, 2)
        self.assertEqual(source.batch_size, 2)
        self.assertIn("flow_id", source.table.column_names)
        self.assertIn("event_time", source.table.column_names)


if __name__ == "__main__":
    unittest.main()

