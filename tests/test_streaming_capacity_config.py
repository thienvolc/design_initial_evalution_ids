from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.config.capacity import (  # noqa: E402
    CAPACITY_CONFIG,
    CAPACITY_MAIN_PROFILES,
    CAPACITY_SMOKE_CONFIG,
    CapacityMatrixConfig,
    RuntimeProfile,
)


class CapacityConfigTests(unittest.TestCase):
    def test_capacity_config_is_smoke_preset_by_default(self) -> None:
        self.assertIs(CAPACITY_CONFIG, CAPACITY_SMOKE_CONFIG)
        self.assertIsInstance(CAPACITY_CONFIG, CapacityMatrixConfig)
        self.assertEqual(CAPACITY_CONFIG.name, "capacity_smoke")
        self.assertEqual(len(CAPACITY_CONFIG.runs), 1)

    def test_capacity_smoke_run_contains_runtime_and_replay_configs(self) -> None:
        run = CAPACITY_SMOKE_CONFIG.runs[0].benchmark

        self.assertEqual(run.profile, RuntimeProfile("capacity_smoke", 500, 4, "5 seconds"))
        self.assertEqual(run.runtime.run.run_tag, run.run_tag)
        self.assertEqual(run.runtime.run.input_run_tag, run.run_tag)
        self.assertEqual(run.runtime.run.load_profile, "capacity_smoke")
        self.assertEqual(run.runtime.kafka.max_offsets_per_trigger, 500)
        self.assertEqual(run.runtime.spark.shuffle_partitions, 4)
        self.assertEqual(run.runtime.spark.trigger_interval, "5 seconds")
        self.assertTrue(run.runtime.lifecycle.reset_outputs)
        self.assertEqual(run.replay.runtime.run_tag, run.run_tag)
        self.assertEqual(run.replay.runtime.bootstrap_servers, run.runtime.kafka.bootstrap_servers)
        self.assertEqual(run.replay.runtime.topic, run.runtime.kafka.input_topic)
        self.assertEqual(run.replay.source.table.num_rows, 1000)
        self.assertEqual(run.replay.source.batch_size, 500)

    def test_capacity_main_profiles_are_named_by_capacity_intent(self) -> None:
        profiles = [profile.name for profile in CAPACITY_MAIN_PROFILES]

        self.assertEqual(profiles, ["capacity_low", "capacity_mid", "capacity_high"])

    def test_runtime_profile_legacy_dict_preserves_summary_keys(self) -> None:
        profile = RuntimeProfile("capacity_mid", 2000, 8, "")

        self.assertEqual(
            profile.to_legacy_dict(),
            {
                "name": "capacity_mid",
                "max_offsets_per_trigger": 2000,
                "shuffle_partitions": 8,
                "trigger_interval": "",
            },
        )

    def test_layer_a_entrypoint_is_config_driven(self) -> None:
        script = PROJECT_ROOT / "scripts/streaming/official/run_layer_a_matrix.py"
        content = script.read_text(encoding="utf-8")

        self.assertIn("CAPACITY_CONFIG", content)
        self.assertIn("run_capacity_matrix", content)
        self.assertNotIn("argparse", content)


if __name__ == "__main__":
    unittest.main()
