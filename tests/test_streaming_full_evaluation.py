from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.orchestration.full_evaluation import (  # noqa: E402
    FullEvaluationOptions,
    build_layer_a_command,
    build_layer_b_command,
    build_load_quality_command,
    build_watermark_command,
)


class FullEvaluationCommandTests(unittest.TestCase):
    def test_layer_a_command_forwards_metrics_controls(self) -> None:
        options = FullEvaluationOptions(
            layer_a_metrics_timeout_sec=75,
            layer_a_metrics_idle_sec=12,
        )

        command = build_layer_a_command(options, "python")

        self.assertIn("--metrics-timeout-sec", command)
        self.assertIn("75", command)
        self.assertIn("--metrics-idle-sec", command)
        self.assertIn("12", command)

    def test_layer_b_command_forwards_metrics_controls(self) -> None:
        options = FullEvaluationOptions(
            layer_b_metrics_timeout_sec=55,
            layer_b_metrics_idle_sec=9,
        )

        command = build_layer_b_command(options, "python")

        self.assertIn("--metrics-timeout-sec", command)
        self.assertIn("55", command)
        self.assertIn("--metrics-idle-sec", command)
        self.assertIn("9", command)

    def test_watermark_command_forwards_metrics_controls(self) -> None:
        options = FullEvaluationOptions(
            watermark_metrics_timeout_sec=120,
            watermark_metrics_idle_sec=20,
        )

        command = build_watermark_command(options, "python")

        self.assertIn("--metrics-timeout-sec", command)
        self.assertIn("120", command)
        self.assertIn("--metrics-idle-sec", command)
        self.assertIn("20", command)

    def test_load_quality_command_forwards_metrics_controls(self) -> None:
        options = FullEvaluationOptions(
            load_quality_metrics_timeout_sec=150,
            load_quality_metrics_idle_sec=14,
        )

        command = build_load_quality_command(options, "python")

        self.assertIn("--metrics-timeout-sec", command)
        self.assertIn("150", command)
        self.assertIn("--metrics-idle-sec", command)
        self.assertIn("14", command)


if __name__ == "__main__":
    unittest.main()
