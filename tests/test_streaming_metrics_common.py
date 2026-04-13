from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.matrices.common import is_terminal_metric_payload, summarize_runtime_metrics


class StreamingMetricsCommonTests(unittest.TestCase):
    def test_is_terminal_metric_payload_detects_run_completed_event(self) -> None:
        self.assertTrue(is_terminal_metric_payload({"event_type": "run_completed", "run_tag": "demo"}))
        self.assertTrue(is_terminal_metric_payload({"final": True, "run_tag": "demo"}))
        self.assertFalse(is_terminal_metric_payload({"batch_id": 1, "rows": 100, "run_tag": "demo"}))

    def test_summarize_runtime_metrics_ignores_terminal_payloads(self) -> None:
        summary = summarize_runtime_metrics(
            [
                {
                    "run_tag": "demo",
                    "batch_id": 1,
                    "rows": 100,
                    "rows_per_sec": 50.0,
                    "batch_wall_ms": 2000.0,
                    "latency_ms": {
                        "source_to_ingest": {"p95": 10.0},
                        "processing": {"p95": 20.0},
                        "end_to_end": {"p95": 30.0},
                    },
                    "event_time": {"late_event_ratio": 0.01},
                    "kafka": {"lag_records_total": 5},
                    "system": {"driver_rss_mb": 128.0, "executor_mem_util_avg": 0.25},
                    "detection": {"labeled_rows": 100, "tp": 80, "tn": 10, "fp": 5, "fn": 5},
                    "avg_prediction_score": 0.5,
                    "attack_ratio": 0.85,
                },
                {
                    "run_tag": "demo",
                    "event_type": "run_completed",
                    "final": True,
                },
            ]
        )

        self.assertEqual(summary["rows_total"], 100)
        self.assertEqual(summary["batch_count"], 1)
        self.assertEqual(summary["last_payload"]["batch_id"], 1)


if __name__ == "__main__":
    unittest.main()
