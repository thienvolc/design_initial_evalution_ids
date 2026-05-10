from __future__ import annotations

import csv
import json
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.evaluation.matrices.common import (
    TIMESERIES_FIELDNAMES,
    collect_matching_metrics,
    flatten_metrics_payload,
    is_terminal_metric_payload,
    summarize_runtime_metrics,
    write_metrics_timeseries,
)


_TEST_TMP_ROOT = PROJECT_ROOT / ".tmp_test_runs"
_TEST_TMP_ROOT.mkdir(parents=True, exist_ok=True)


class StreamingMetricsCommonTests(unittest.TestCase):
    def test_collect_matching_metrics_drains_after_terminal_marker_gracefully(self) -> None:
        class _FakeMessage:
            def __init__(self, payload: dict | None):
                self._payload = payload

            def error(self):
                return None

            def value(self):
                return json.dumps(self._payload).encode("utf-8")

        class _FakeConsumer:
            def __init__(self, *_args, **_kwargs):
                self._messages = [
                    _FakeMessage({"run_tag": "demo", "event_type": "run_completed", "final": True}),
                    _FakeMessage({"run_tag": "demo", "batch_id": 2, "rows": 20}),
                    _FakeMessage({"run_tag": "demo", "batch_id": 1, "rows": 10}),
                    None,
                    None,
                ]

            def subscribe(self, _topics):
                return None

            def poll(self, _timeout):
                if self._messages:
                    return self._messages.pop(0)
                return None

            def close(self):
                return None

        with mock.patch("ids_platform.streaming.evaluation.matrices.common.Consumer", _FakeConsumer):
            rows = collect_matching_metrics(
                bootstrap_servers="dummy:9092",
                topic="ids.metrics",
                run_tag="demo",
                timeout_sec=2,
                idle_sec=0,
            )

        self.assertEqual([row["batch_id"] for row in rows], [1, 2])

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

    def test_flatten_metrics_payload_maps_nested_fields(self) -> None:
        row = flatten_metrics_payload(
            {
                "ts_utc": "2026-04-15T12:00:00+00:00",
                "ts_epoch_ms": 123456,
                "batch_id": 7,
                "run_tag": "demo_run",
                "load_profile": "profile_a",
                "model_name": "logistic_regression",
                "feature_set": "full",
                "metric_warnings": ["driver_cpu_percent_unavailable_without_psutil"],
                "rows": 100,
                "rows_per_sec": 50.0,
                "batch_wall_ms": 2000.0,
                "latency_ms": {
                    "source_to_ingest": {"p95": 10.0},
                    "processing": {"p95": 20.0},
                    "end_to_end": {"p95": 30.0},
                    "event_lateness": {"p95": 5.0},
                },
                "event_time": {"late_event_ratio": 0.01},
                "kafka": {"lag_records_total": 5, "lag_records_max_partition": 3},
                "system": {
                    "driver_cpu_percent": 12.5,
                    "driver_rss_mb": 128.0,
                    "executor_mem_util_avg": 0.25,
                    "executor_mem_util_p95": 0.4,
                    "executor_count": 1,
                },
                "detection": {
                    "labeled_rows": 100,
                    "tp": 80,
                    "tn": 10,
                    "fp": 5,
                    "fn": 5,
                    "precision": 0.94,
                    "recall": 0.94,
                    "f1": 0.94,
                    "fpr": 0.33,
                    "fnr": 0.06,
                },
            }
        )

        self.assertEqual(list(row.keys()), TIMESERIES_FIELDNAMES)
        self.assertEqual(row["batch_id"], 7)
        self.assertEqual(row["metric_warnings"], "driver_cpu_percent_unavailable_without_psutil")
        self.assertEqual(row["source_p95_ms"], 10.0)
        self.assertEqual(row["ingest_to_emit_p95_ms"], 20.0)
        self.assertEqual(row["source_to_emit_p95_ms"], 30.0)
        self.assertEqual(row["proc_p95_ms"], 20.0)
        self.assertEqual(row["e2e_p95_ms"], 30.0)
        self.assertEqual(row["kafka_lag_records_total"], 5)
        self.assertEqual(row["driver_rss_mb"], 128.0)
        self.assertEqual(row["precision"], 0.94)
        self.assertEqual(row["late_event_ratio_interpretable"], 0.01)
        self.assertEqual(row["freshness_signal_ratio"], "")

    def test_flatten_metrics_payload_maps_zero_watermark_lateness_to_freshness_signal(self) -> None:
        row = flatten_metrics_payload(
            {
                "run_tag": "demo_run",
                "batch_id": 8,
                "event_time": {"late_event_ratio": 0.25},
                "system_knobs": {"watermark_delay_sec": 0},
            }
        )

        self.assertEqual(row["late_event_ratio"], 0.25)
        self.assertEqual(row["late_event_ratio_interpretable"], "")
        self.assertEqual(row["freshness_signal_ratio"], 0.25)

    def test_flatten_metrics_payload_handles_missing_optional_fields(self) -> None:
        row = flatten_metrics_payload(
            {
                "run_tag": "demo_run",
                "batch_id": "not-a-number",
                "rows": 10,
            }
        )

        self.assertEqual(row["run_tag"], "demo_run")
        self.assertEqual(row["batch_id"], "not-a-number")
        self.assertEqual(row["rows"], 10)
        self.assertEqual(row["e2e_p95_ms"], "")
        self.assertEqual(row["ingest_to_emit_p95_ms"], "")
        self.assertEqual(row["driver_rss_mb"], "")
        self.assertEqual(row["precision"], "")

    def test_summarize_runtime_metrics_preserves_undefined_ratios_as_none(self) -> None:
        summary = summarize_runtime_metrics(
            [
                {
                    "run_tag": "demo",
                    "batch_id": 1,
                    "rows": 50,
                    "latency_ms": {
                        "source_to_ingest": {"p95": 10.0},
                        "processing": {"p95": 20.0},
                        "ingest_to_emit": {"p95": 20.0},
                        "end_to_end": {"p95": 30.0},
                        "source_to_emit": {"p95": 30.0},
                    },
                    "system": {
                        "driver_cpu_percent": 12.5,
                        "driver_rss_mb": 100.0,
                        "executor_mem_util_avg": 0.25,
                        "executor_mem_util_p95": 0.4,
                        "executor_count": 1,
                    },
                    "metric_warnings": ["driver_cpu_percent_unavailable_without_psutil"],
                    "detection": {"labeled_rows": 50, "tp": 0, "tn": 50, "fp": 0, "fn": 0},
                }
            ]
        )

        self.assertIsNone(summary["precision"])
        self.assertIsNone(summary["recall"])
        self.assertIsNone(summary["f1"])
        self.assertEqual(summary["fpr"], 0.0)
        self.assertIsNone(summary["fnr"])
        self.assertEqual(summary["driver_cpu_percent_avg"], 12.5)
        self.assertEqual(summary["executor_mem_util_p95_avg"], 0.4)
        self.assertEqual(summary["executor_count_max"], 1)
        self.assertEqual(summary["metric_warnings"], ["driver_cpu_percent_unavailable_without_psutil"])

    def test_summarize_runtime_metrics_splits_lateness_semantics_by_watermark_mode(self) -> None:
        summary = summarize_runtime_metrics(
            [
                {
                    "run_tag": "demo",
                    "batch_id": 1,
                    "rows": 10,
                    "event_time": {"late_event_ratio": 0.5},
                    "system_knobs": {"watermark_delay_sec": 0},
                },
                {
                    "run_tag": "demo",
                    "batch_id": 2,
                    "rows": 30,
                    "event_time": {"late_event_ratio": 0.2},
                    "system_knobs": {"watermark_delay_sec": 5},
                },
            ]
        )

        self.assertAlmostEqual(summary["late_event_ratio_weighted"], 0.275)
        self.assertAlmostEqual(summary["late_event_ratio_interpretable_weighted"], 0.2)
        self.assertAlmostEqual(summary["freshness_signal_ratio_weighted"], 0.5)

    def test_write_metrics_timeseries_writes_sorted_batches_csv(self) -> None:
        with tempfile.TemporaryDirectory(dir=_TEST_TMP_ROOT) as temp_dir:
            output_dir = Path(temp_dir) / "metrics"
            path = write_metrics_timeseries(
                [
                    {"run_tag": "demo", "batch_id": 2, "ts_epoch_ms": 2, "rows": 20},
                    {"run_tag": "demo", "batch_id": 1, "ts_epoch_ms": 1, "rows": 10},
                    {"run_tag": "demo", "event_type": "run_completed", "final": True},
                ],
                run_tag="demo",
                output_dir=str(output_dir),
            )

            self.assertIsNotNone(path)
            self.assertTrue(path.exists())
            with path.open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))

            self.assertEqual(rows[0]["batch_id"], "1")
            self.assertEqual(rows[1]["batch_id"], "2")
            self.assertEqual(rows[0]["run_tag"], "demo")
            self.assertEqual(rows[1]["run_tag"], "demo")

    def test_write_metrics_timeseries_preserves_summary_values(self) -> None:
        metrics_rows = [
            {
                "run_tag": "demo",
                "batch_id": 2,
                "ts_epoch_ms": 2,
                "rows": 20,
                "rows_per_sec": 10.0,
                "batch_wall_ms": 2000.0,
                "latency_ms": {
                    "source_to_ingest": {"p95": 10.0},
                    "processing": {"p95": 20.0},
                    "end_to_end": {"p95": 30.0},
                    "event_lateness": {"p95": 5.0},
                },
                "event_time": {"late_event_ratio": 0.1},
                "kafka": {"lag_records_total": 7, "lag_records_max_partition": 3},
                "system": {"driver_rss_mb": 100.0, "executor_mem_util_avg": 0.2},
                "detection": {"labeled_rows": 20, "tp": 8, "tn": 9, "fp": 2, "fn": 1},
            },
            {
                "run_tag": "demo",
                "batch_id": 1,
                "ts_epoch_ms": 1,
                "rows": 10,
                "rows_per_sec": 5.0,
                "batch_wall_ms": 1000.0,
                "latency_ms": {
                    "source_to_ingest": {"p95": 8.0},
                    "processing": {"p95": 16.0},
                    "end_to_end": {"p95": 24.0},
                    "event_lateness": {"p95": 4.0},
                },
                "event_time": {"late_event_ratio": 0.2},
                "kafka": {"lag_records_total": 4, "lag_records_max_partition": 2},
                "system": {"driver_rss_mb": 80.0, "executor_mem_util_avg": 0.1},
                "detection": {"labeled_rows": 10, "tp": 4, "tn": 4, "fp": 1, "fn": 1},
            },
        ]
        summary_before = summarize_runtime_metrics(metrics_rows)

        with tempfile.TemporaryDirectory(dir=_TEST_TMP_ROOT) as temp_dir:
            output_dir = Path(temp_dir) / "metrics"
            path = write_metrics_timeseries(metrics_rows, run_tag="demo", output_dir=str(output_dir))

            self.assertIsNotNone(path)
            with path.open("r", encoding="utf-8", newline="") as handle:
                header = next(csv.reader(handle))

        summary_after = summarize_runtime_metrics(metrics_rows)
        self.assertEqual(header, TIMESERIES_FIELDNAMES)
        self.assertEqual(summary_after, summary_before)


if __name__ == "__main__":
    unittest.main()
