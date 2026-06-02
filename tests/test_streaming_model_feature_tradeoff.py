from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.config.calibration import CAPACITY_CALIBRATION_PROFILE  # noqa: E402
from ids_platform.streaming.config.common import BenchmarkMatrixConfig  # noqa: E402
from ids_platform.streaming.config.model_feature_tradeoff import (  # noqa: E402
    MODEL_FEATURE_TRADEOFF_CONFIG,
    MODEL_FEATURE_TRADEOFF_SMOKE_CONFIG,
    build_model_feature_tradeoff_main_config,
)


class ModelFeatureTradeoffConfigTests(unittest.TestCase):
    def test_default_config_is_rf17_smoke(self) -> None:
        self.assertIs(MODEL_FEATURE_TRADEOFF_CONFIG, MODEL_FEATURE_TRADEOFF_SMOKE_CONFIG)
        self.assertIsInstance(MODEL_FEATURE_TRADEOFF_CONFIG, BenchmarkMatrixConfig)
        self.assertEqual(MODEL_FEATURE_TRADEOFF_CONFIG.name, "model_feature_tradeoff_smoke")
        self.assertEqual(len(MODEL_FEATURE_TRADEOFF_CONFIG.runs), 1)

        run_plan = MODEL_FEATURE_TRADEOFF_CONFIG.runs[0]
        benchmark = run_plan.benchmark
        self.assertEqual(benchmark.profile, CAPACITY_CALIBRATION_PROFILE)
        self.assertEqual(benchmark.model_label, "random_forest")
        self.assertEqual(benchmark.feature_set, "reduced")
        self.assertEqual(benchmark.replay.source.table.num_rows, 1000)
        self.assertEqual(benchmark.replay.phase, "measure")
        self.assertIsNotNone(benchmark.warmup_replay)
        self.assertEqual(benchmark.warmup_replay.phase, "warmup")
        self.assertEqual(benchmark.warmup_replay.source.table.num_rows, 1000)
        self.assertEqual(run_plan.summary_context["candidate_label"], "RF-17")
        self.assertEqual(run_plan.summary_context["baseline_label"], "RF-Full")
        self.assertEqual(run_plan.summary_context["baseline_source"], "capacity_calibration")

    def test_main_config_runs_rf17_at_selected_operating_points(self) -> None:
        config = build_model_feature_tradeoff_main_config()

        self.assertEqual(config.name, "model_feature_tradeoff_main")
        self.assertEqual(len(config.runs), 6)
        self.assertEqual({run.summary_context["target_rps"] for run in config.runs}, {500.0, 750.0})
        self.assertEqual({run.benchmark.repeat_index for run in config.runs}, {1, 2, 3})
        self.assertEqual({run.benchmark.feature_set for run in config.runs}, {"reduced"})
        self.assertEqual({run.benchmark.model_label for run in config.runs}, {"random_forest"})
        self.assertEqual({run.benchmark.replay.source.batch_size for run in config.runs}, {500})
        self.assertTrue(all(run.benchmark.warmup_replay is not None for run in config.runs))
        self.assertEqual(
            {run.benchmark.warmup_replay.source.table.num_rows for run in config.runs},
            {5_000, 7_500},
        )
        self.assertEqual(
            [run.benchmark.run_tag for run in config.runs],
            [run.benchmark.run_tag for run in build_model_feature_tradeoff_main_config().runs],
        )

    def test_model_feature_tradeoff_entrypoint_is_config_driven(self) -> None:
        script = PROJECT_ROOT / "scripts/streaming/official/run_model_feature_tradeoff.py"
        content = script.read_text(encoding="utf-8")

        self.assertIn("MODEL_FEATURE_TRADEOFF_CONFIG", content)
        self.assertIn("run_model_feature_tradeoff_matrix", content)
        self.assertNotIn("argparse", content)


if __name__ == "__main__":
    unittest.main()
