from __future__ import annotations

import csv
import sys
import tempfile
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.orchestration.benchmark_runner import append_benchmark_summary_row


class BenchmarkRunnerTests(unittest.TestCase):
    def test_append_benchmark_summary_row_writes_and_replaces_by_run_id(self) -> None:
        payload_v1 = {
            "runner": {"run_id": "run-1", "layer": "A"},
            "timing_seconds": {"total": 10.0, "read": 1.0, "transform": 2.0, "inference": 3.0, "sink": 4.0},
            "latency_ms": {
                "source_to_ingest": {"p95": 11.0},
                "processing": {"p95": 22.0},
                "end_to_end": {"p95": 33.0},
            },
            "source": {"rows_scored": 100},
            "profile": {"feature_set": "full", "models": ["logistic_regression"]},
            "models": {"logistic_regression": {"metrics": {"f1": 0.9, "recall": 0.8, "fpr": 0.1}}},
        }
        payload_v2 = {
            **payload_v1,
            "timing_seconds": {"total": 5.0, "read": 1.0, "transform": 1.0, "inference": 1.0, "sink": 2.0},
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            summary_csv = Path(temp_dir) / "benchmark_summary.csv"

            append_benchmark_summary_row(summary_csv, payload_v1)
            append_benchmark_summary_row(summary_csv, payload_v2)

            with summary_csv.open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))

            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["run_id"], "run-1")
            self.assertEqual(rows[0]["rows_per_second"], "20.0")
            self.assertEqual(rows[0]["logistic_regression_f1"], "0.9")


if __name__ == "__main__":
    unittest.main()
