from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.config.common import build_runtime_config
from ids_platform.streaming.runtime.config import RuntimeConfig


class RuntimeConfigTests(unittest.TestCase):
    def test_build_runtime_config_creates_direct_runtime_config(self) -> None:
        config = build_runtime_config(
            model_name="random_forest",
            feature_set="reduced",
            run_tag="run 1",
            input_run_tag="input-1",
            load_profile="profile-1",
            starting_offsets="latest",
            max_offsets_per_trigger=250,
            shuffle_partitions=8,
            trigger_interval="5 seconds",
            watermark_delay_sec=20,
            drop_late_events=True,
            reset_outputs=True,
        )

        self.assertIsInstance(config, RuntimeConfig)
        self.assertEqual(config.run.run_tag, "run 1")
        self.assertEqual(config.run.input_run_tag, "input-1")
        self.assertEqual(config.resolved_load_profile, "profile-1")
        self.assertEqual(config.kafka.starting_offsets, "latest")
        self.assertEqual(config.kafka.max_offsets_per_trigger, 250)
        self.assertEqual(config.spark.shuffle_partitions, 8)
        self.assertEqual(config.spark.trigger_interval, "5 seconds")
        self.assertEqual(config.lifecycle.watermark_delay_sec, 20)
        self.assertTrue(config.lifecycle.drop_late_events)
        self.assertTrue(config.lifecycle.reset_outputs)
        self.assertEqual(config.model.name, "random_forest")
        self.assertEqual(config.model.reported_name, "random_forest")
        self.assertEqual(config.features.feature_set, "reduced")
        self.assertTrue(config.features.feature_columns)
        self.assertEqual(config.output.parquet_checkpoint.name, "parquet")

    def test_pass_through_runtime_config_does_not_require_model_artifact(self) -> None:
        config = build_runtime_config(model_name="missing", mode="pass_through")

        self.assertTrue(config.model.is_pass_through)
        self.assertEqual(config.model.reported_name, "pass_through")
        self.assertIsNone(config.model.artifact_path)
        self.assertIsNone(config.model.threshold)
        self.assertEqual(config.resolved_load_profile, "pass_through:full")


if __name__ == "__main__":
    unittest.main()
