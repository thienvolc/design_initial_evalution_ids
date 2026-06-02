from __future__ import annotations

import subprocess
import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


def _run_script(arguments: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, *arguments],
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


class StreamingEntrypointIntegrationTests(unittest.TestCase):
    def test_plot_entrypoint_supports_help_cli(self) -> None:
        result = _run_script(["scripts/streaming/official/build_timeseries_plots.py", "--help"])

        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("usage:", result.stdout.lower())

    def test_config_driven_entrypoints_reject_cli_arguments(self) -> None:
        scripts = [
            "scripts/streaming/official/run_structured_streaming.py",
            "scripts/streaming/official/replay_parquet_to_kafka.py",
            "scripts/streaming/official/run_capacity_calibration.py",
            "scripts/streaming/official/run_model_feature_tradeoff.py",
            "scripts/streaming/official/run_layer_c_matrix.py",
            "scripts/streaming/official/run_watermark_matrix.py",
            "scripts/streaming/official/run_load_quality_matrix.py",
        ]

        for script_path in scripts:
            with self.subTest(script=script_path):
                result = _run_script([script_path, "--help"])
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("config-driven", result.stderr)

if __name__ == "__main__":
    unittest.main()


