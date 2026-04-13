from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.orchestration.profile_service import (
    ProfileExecutionOptions,
    ProfileListFilters,
    list_profiles,
    run_profile,
)
from ids_platform.streaming.orchestration.profile_runner import resolve_profile


class _RunResult:
    def __init__(self, returncode: int = 0) -> None:
        self.returncode = returncode


class ProfileServiceTests(unittest.TestCase):
    def test_local_profile_aliases_resolve_to_current_profiles(self) -> None:
        profiles = {
            "layer_a_local_light": {"extends": "layer_a_local_medium"},
            "layer_a_local_medium": {
                "script": "scripts/streaming/run_layer_a_matrix.py",
                "runtime": "docker",
                "meta": {"resource_class": "medium"},
                "args": {"summary_csv": "artifacts/streaming/scale_up/layer_a_summary_local_medium.csv"},
            },
            "layer_b_light": {"extends": "layer_b_local_medium"},
            "layer_b_local_medium": {
                "script": "scripts/streaming/run_layer_b_matrix.py",
                "runtime": "docker",
                "meta": {"resource_class": "medium"},
                "args": {"summary_csv": "artifacts/streaming/scale_up/layer_b_summary_local_medium.csv"},
            },
            "watermark_local_light": {"extends": "watermark_local_medium"},
            "watermark_local_medium": {
                "script": "scripts/streaming/run_watermark_matrix.py",
                "runtime": "docker",
                "meta": {"resource_class": "medium"},
                "args": {"summary_csv": "artifacts/streaming/scale_up/watermark_summary_local_medium.csv"},
            },
            "layer_c_local_light": {"extends": "layer_c_local_medium"},
            "layer_c_local_medium": {
                "script": "scripts/streaming/run_layer_c_matrix.py",
                "runtime": "host",
                "meta": {"resource_class": "medium"},
                "args": {"summary_csv": "artifacts/streaming/scale_up/layer_c_summary_local_medium.csv"},
            },
            "stress_local_light": {"extends": "stress_local_medium"},
            "stress_local_medium": {
                "script": "scripts/streaming/run_load_quality_matrix.py",
                "runtime": "docker",
                "meta": {"resource_class": "medium"},
                "args": {"summary_csv": "artifacts/streaming/scale_up/load_quality_summary_local_medium.csv"},
            },
        }

        self.assertEqual(
            resolve_profile("layer_a_local_light", profiles),
            resolve_profile("layer_a_local_medium", profiles),
        )
        self.assertEqual(resolve_profile("layer_b_light", profiles), resolve_profile("layer_b_local_medium", profiles))
        self.assertEqual(
            resolve_profile("watermark_local_light", profiles),
            resolve_profile("watermark_local_medium", profiles),
        )
        self.assertEqual(
            resolve_profile("layer_c_local_light", profiles),
            resolve_profile("layer_c_local_medium", profiles),
        )
        self.assertEqual(
            resolve_profile("stress_local_light", profiles),
            resolve_profile("stress_local_medium", profiles),
        )

    def test_list_profiles_filters_official_profiles(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "profiles.yaml"
            config_path.write_text(
                "\n".join(
                    [
                        "profiles:",
                        "  smoke_gate:",
                        "    script: scripts/streaming/run_layer_a_matrix.py",
                        "    meta:",
                        "      mode: official",
                        "      resource_class: light",
                        "  watermark_ablation:",
                        "    script: scripts/streaming/run_watermark_matrix.py",
                        "    meta:",
                        "      mode: ablation",
                        "      resource_class: heavy",
                    ]
                ),
                encoding="utf-8",
            )

            items = list_profiles(config_path, ProfileListFilters(official_only=True))

            self.assertEqual([item.name for item in items], ["smoke_gate"])

    def test_run_profile_blocks_heavy_profiles_without_allow_flag(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "profiles.yaml"
            config_path.write_text(
                "\n".join(
                    [
                        "profiles:",
                        "  heavy_run:",
                        "    script: scripts/streaming/run_layer_b_matrix.py",
                        "    meta:",
                        "      mode: official",
                        "      resource_class: heavy",
                    ]
                ),
                encoding="utf-8",
            )

            result = run_profile(
                ProfileExecutionOptions(
                    profile_config_path=str(config_path),
                    profile_name="heavy_run",
                )
            )

            self.assertEqual(result.exit_code, 2)
            self.assertIn("Blocked heavy profile", result.blocked_reason)

    def test_run_profile_gate_only_returns_gate_result(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            summary_path = temp_path / "summary.csv"
            summary_path.write_text(
                "status,rows_total,fpr_avg,fnr_avg\nok,1500,0.01,0.02\n",
                encoding="utf-8",
            )
            config_path = temp_path / "profiles.yaml"
            config_path.write_text(
                "\n".join(
                    [
                        "profiles:",
                        "  smoke_gate:",
                        "    script: scripts/streaming/run_layer_a_matrix.py",
                        "    args:",
                        f"      summary_csv: {summary_path.as_posix()}",
                        "    meta:",
                        "      mode: official",
                        "      resource_class: light",
                        "      pass_fail:",
                        "        required: true",
                        "        min_rows_total: 1000",
                    ]
                ),
                encoding="utf-8",
            )

            result = run_profile(
                ProfileExecutionOptions(
                    profile_config_path=str(config_path),
                    profile_name="smoke_gate",
                    gate_only=True,
                )
            )

            self.assertEqual(result.exit_code, 0)
            self.assertTrue(any("PASS" in line for line in result.gate_lines))

    def test_run_profile_executes_command_through_injected_runner(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "profiles.yaml"
            config_path.write_text(
                "\n".join(
                    [
                        "profiles:",
                        "  smoke_gate:",
                        "    script: scripts/streaming/run_layer_a_matrix.py",
                        "    args:",
                        "      config: configs/streaming/online.yaml",
                        "    meta:",
                        "      mode: official",
                        "      resource_class: light",
                    ]
                ),
                encoding="utf-8",
            )

            captured: dict[str, object] = {}

            def fake_run(command: list[str], *, cwd: Path):
                captured["command"] = command
                captured["cwd"] = cwd
                return _RunResult(returncode=0)

            result = run_profile(
                ProfileExecutionOptions(
                    profile_config_path=str(config_path),
                    profile_name="smoke_gate",
                    skip_gates=True,
                    python_executable="python",
                ),
                run_command_fn=fake_run,
                project_root=PROJECT_ROOT,
            )

            self.assertEqual(result.exit_code, 0)
            self.assertEqual(captured["cwd"], PROJECT_ROOT)
            self.assertEqual(captured["command"][0:2], ["python", "scripts/streaming/run_layer_a_matrix.py"])


if __name__ == "__main__":
    unittest.main()

