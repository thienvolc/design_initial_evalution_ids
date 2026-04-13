from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.offline.paths import Paths


class OfflinePathsTests(unittest.TestCase):
    def test_build_uses_project_root_for_package_layout_logs_and_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            project_root = Path(temp_dir)
            package_dir = project_root / "src" / "ids_platform" / "offline"
            package_dir.mkdir(parents=True, exist_ok=True)

            paths = Paths.build(package_dir, feature_set="reduced")
            paths.ensure_dirs()

            self.assertEqual(paths.root, project_root.resolve())
            self.assertEqual(paths.models_dir, project_root / "artifacts" / "offline" / "models")
            self.assertEqual(paths.log_dir, project_root / "logs" / "offline")
            self.assertTrue(paths.models_dir.is_dir())
            self.assertTrue(paths.log_dir.is_dir())
            self.assertFalse((project_root / "src" / "offline" / "logs").exists())

    def test_build_from_legacy_src_layout_still_writes_logs_under_project_root(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            project_root = Path(temp_dir)
            legacy_src_dir = project_root / "src" / "offline"

            paths = Paths.build(legacy_src_dir, feature_set="full")

            self.assertEqual(paths.root, project_root.resolve())
            self.assertEqual(paths.feature_registry_path.name, "feature_registry_full.yaml")
            self.assertEqual(paths.log_dir, project_root / "logs" / "offline")


if __name__ == "__main__":
    unittest.main()
