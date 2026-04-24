from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.offline.config import PreprocessingConfig  # noqa: E402
from ids_platform.offline.paths import Paths  # noqa: E402
from ids_platform.offline.training import (  # noqa: E402
    _merge_selected_features,
    _should_apply_feature_selection,
)


class _DummyLog:
    def warning(self, *args, **kwargs) -> None:
        return None


class OfflineFeatureSelectionTests(unittest.TestCase):
    def test_preprocessing_config_enables_hybrid_selection_for_reduced(self) -> None:
        config = PreprocessingConfig.from_yaml(
            PROJECT_ROOT / "configs" / "modeling" / "preprocessing.yaml"
        )

        self.assertTrue(config.feature_selection.enabled)
        self.assertEqual(config.feature_selection.strategy, "hybrid")
        self.assertEqual(config.feature_selection.method, "mutual_info")
        self.assertEqual(config.feature_selection.k, 17)
        self.assertEqual(config.feature_selection.apply_feature_sets, ("reduced",))

    def test_feature_selection_applies_only_to_reduced_paths(self) -> None:
        package_dir = PROJECT_ROOT / "src" / "ids_platform" / "offline"
        config = PreprocessingConfig.from_yaml(
            PROJECT_ROOT / "configs" / "modeling" / "preprocessing.yaml"
        )

        full_paths = Paths.build(package_dir, feature_set="full")
        reduced_paths = Paths.build(package_dir, feature_set="reduced")

        self.assertFalse(_should_apply_feature_selection(full_paths, config.feature_selection))
        self.assertTrue(_should_apply_feature_selection(reduced_paths, config.feature_selection))

    def test_hybrid_merge_keeps_required_features_and_fills_from_ranking(self) -> None:
        merged = _merge_selected_features(
            strategy="hybrid",
            ranked_selected_features=["f3", "f4", "f2", "f5", "f6"],
            required_features=["f1", "f2"],
            default_features=["f1", "f2", "f3"],
            k=4,
            log=_DummyLog(),
        )

        self.assertEqual(merged, ["f1", "f2", "f3", "f4"])


if __name__ == "__main__":
    unittest.main()
