from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.core.artifacts import (  # noqa: E402
    resolve_feature_set_path,
    resolve_model_artifact_path,
)
from ids_platform.streaming.runtime.config import (  # noqa: E402
    RuntimeConfig,
    RuntimeFeatureConfig,
    RuntimeKafkaConfig,
    RuntimeLifecycleConfig,
    RuntimeMetricsConfig,
    RuntimeModelConfig,
    RuntimeOutputConfig,
    RuntimeRunConfig,
    RuntimeSparkConfig,
)


class StreamingArtifactsTests(unittest.TestCase):
    def test_resolve_feature_set_path_returns_full_and_reduced_paths(self) -> None:
        cfg = {
            "feature_manifest": "artifacts/offline/preprocessing/feature_manifest.json",
            "feature_manifest_by_feature_set": {
                "full": "artifacts/offline/preprocessing/feature_manifest.json",
                "reduced": "artifacts/offline/preprocessing/feature_manifest_reduced.json",
            },
        }

        full_path = resolve_feature_set_path(cfg, "feature_manifest", "full")
        reduced_path = resolve_feature_set_path(cfg, "feature_manifest", "reduced")

        self.assertEqual(
            full_path,
            PROJECT_ROOT / "artifacts" / "offline" / "preprocessing" / "feature_manifest.json",
        )
        self.assertEqual(
            reduced_path,
            PROJECT_ROOT / "artifacts" / "offline" / "preprocessing" / "feature_manifest_reduced.json",
        )

    def test_resolve_model_artifact_path_returns_reduced_model_path(self) -> None:
        cfg = {
            "name": "random_forest",
            "joblib_path": "artifacts/offline/models/random_forest.joblib",
            "joblib_path_by_feature_set": {
                "full": "artifacts/offline/models/random_forest.joblib",
                "reduced": "artifacts/offline/models/random_forest_reduced.joblib",
            },
        }

        reduced_path = resolve_model_artifact_path(cfg, "reduced")

        self.assertEqual(
            reduced_path,
            PROJECT_ROOT / "artifacts" / "offline" / "models" / "random_forest_reduced.joblib",
        )

    def test_runtime_config_load_profile_falls_back_to_model_and_feature_set(self) -> None:
        output = RuntimeOutputConfig(
            parquet_checkpoint=Path("checkpoints/parquet"),
            metrics_checkpoint=Path("checkpoints/metrics"),
            sentinel_checkpoint=Path("checkpoints/sentinel"),
            artifact_output=Path("predictions/run"),
        )
        config = RuntimeConfig(
            run=RuntimeRunConfig(load_profile=""),
            spark=RuntimeSparkConfig(),
            kafka=RuntimeKafkaConfig(),
            model=RuntimeModelConfig(name="logistic_regression"),
            features=RuntimeFeatureConfig(feature_set="full", columns=["f1"], fill_values={}),
            output=output,
            lifecycle=RuntimeLifecycleConfig(),
            metrics=RuntimeMetricsConfig(),
        )
        self.assertEqual(
            config.resolved_load_profile,
            "logistic_regression:full",
        )
        config = RuntimeConfig(
            run=RuntimeRunConfig(load_profile="A_mid"),
            spark=RuntimeSparkConfig(),
            kafka=RuntimeKafkaConfig(),
            model=RuntimeModelConfig(name="logistic_regression"),
            features=RuntimeFeatureConfig(feature_set="full", columns=["f1"], fill_values={}),
            output=output,
            lifecycle=RuntimeLifecycleConfig(),
            metrics=RuntimeMetricsConfig(),
        )
        self.assertEqual(
            config.resolved_load_profile,
            "A_mid",
        )

if __name__ == "__main__":
    unittest.main()
