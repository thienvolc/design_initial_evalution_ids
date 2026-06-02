from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.evaluation.matrices.common import summarize_runtime_metrics  # noqa: E402
from ids_platform.streaming.evaluation.matrices.common.quality import (  # noqa: E402
    summarize_prediction_quality,
)
from ids_platform.streaming.evaluation.matrices.throughput.row_builders import (  # noqa: E402
    build_model_feature_tradeoff_summary_row,
)


class StreamingPredictionQualityTests(unittest.TestCase):
    def test_summarize_prediction_quality_computes_confusion_matrix_from_parquet(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp)
            pd.DataFrame(
                {
                    "benchmark_phase": ["warmup", "measure", "measure", "measure", "measure"],
                    "label_binary": [1, 1, 1, 0, 0],
                    "prediction_label": [0, 1, 0, 1, 0],
                    "prediction_score": [0.1, 0.9, 0.2, 0.8, 0.1],
                }
            ).to_parquet(output / "part-000.parquet", index=False)

            summary = summarize_prediction_quality(output, phase="measure")

        self.assertEqual(summary["quality_status"], "ok")
        self.assertEqual(summary["tp_total"], 1)
        self.assertEqual(summary["tn_total"], 1)
        self.assertEqual(summary["fp_total"], 1)
        self.assertEqual(summary["fn_total"], 1)
        self.assertEqual(summary["precision"], 0.5)
        self.assertEqual(summary["recall"], 0.5)
        self.assertEqual(summary["f1"], 0.5)
        self.assertEqual(summary["fpr"], 0.5)
        self.assertEqual(summary["fnr"], 0.5)
        self.assertAlmostEqual(summary["avg_prediction_score_weighted"], 0.5)

    def test_summarize_prediction_quality_keeps_legacy_artifacts_without_phase(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp)
            pd.DataFrame(
                {
                    "label_binary": [1, 1, 0, 0],
                    "prediction_label": [1, 0, 1, 0],
                    "prediction_score": [0.9, 0.2, 0.8, 0.1],
                }
            ).to_parquet(output / "part-000.parquet", index=False)

            summary = summarize_prediction_quality(output)

        self.assertEqual(summary["quality_status"], "ok")
        self.assertEqual(summary["tp_total"], 1)
        self.assertEqual(summary["tn_total"], 1)
        self.assertEqual(summary["fp_total"], 1)
        self.assertEqual(summary["fn_total"], 1)
        self.assertEqual(summary["precision"], 0.5)
        self.assertEqual(summary["recall"], 0.5)
        self.assertEqual(summary["f1"], 0.5)
        self.assertEqual(summary["fpr"], 0.5)
        self.assertEqual(summary["fnr"], 0.5)
        self.assertAlmostEqual(summary["avg_prediction_score_weighted"], 0.5)

    def test_summarize_prediction_quality_reports_missing_artifact(self) -> None:
        summary = summarize_prediction_quality(Path("does/not/exist"))

        self.assertEqual(summary["quality_status"], "quality_missing")
        self.assertIsNone(summary["f1"])

    def test_model_feature_tradeoff_row_uses_quality_summary_for_f1(self) -> None:
        metrics_rows = [
            {
                "rows": 4,
                "rows_per_sec": 2.0,
                "batch_wall_ms": 10.0,
                "latency_ms": {
                    "source_to_ingest": {"p95": 1.0},
                    "ingest_to_emit": {"p95": 2.0},
                    "source_to_emit": {"p95": 3.0},
                    "processing": {"p95": 2.0},
                    "end_to_end": {"p95": 3.0},
                    "event_lateness": {"p95": 0.0},
                },
                "event_time": {"late_event_ratio": 0.0},
                "kafka": {"lag_records_total": 0},
            }
        ]
        quality_summary = {
            "quality_status": "ok",
            "avg_prediction_score_weighted": 0.5,
            "attack_ratio_weighted": 0.5,
            "scored_rows_total": 4,
            "precision": 0.75,
            "recall": 0.6,
            "f1": 2 * 0.75 * 0.6 / (0.75 + 0.6),
            "fpr": 0.1,
            "fnr": 0.4,
        }

        row = build_model_feature_tradeoff_summary_row(
            run_tag="run-1",
            repeat_index=1,
            model="rf",
            feature_set="full",
            metrics_rows=metrics_rows,
            summarize_runtime_metrics_fn=summarize_runtime_metrics,
            quality_summary=quality_summary,
            context={
                "candidate_label": "RF-17",
                "baseline_label": "RF-Full",
                "baseline_source": "capacity_calibration",
                "baseline_summary_csv": "artifacts/streaming/evaluation/capacity_calibration.csv",
                "target_rps": 500.0,
            },
        )

        self.assertEqual(row["quality_status"], "ok")
        self.assertEqual(row["rows"], 4)
        self.assertEqual(row["candidate_label"], "RF-17")
        self.assertEqual(row["baseline_label"], "RF-Full")
        self.assertEqual(row["target_rps"], 500.0)
        self.assertEqual(row["precision"], 0.75)
        self.assertEqual(row["f1"], quality_summary["f1"])


if __name__ == "__main__":
    unittest.main()
