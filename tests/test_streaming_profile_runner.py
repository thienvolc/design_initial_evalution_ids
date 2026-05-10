from __future__ import annotations

import sys
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.evaluation.orchestration.profile_runner import (
    build_command,
    evaluate_pass_fail,
    evaluate_profile_gates,
    include_profile_by_filters,
    resolve_profile,
)


class ProfileRunnerTests(unittest.TestCase):
    def test_resolve_profile_applies_inheritance(self) -> None:
        profiles = {
            "base": {
                "script": "scripts/streaming/official/run_layer_a_matrix.py",
                "args": {"config": "configs/streaming/streaming.yaml", "repeats": 1},
                "meta": {"mode": "official"},
            },
            "child": {
                "extends": "base",
                "args": {"repeats": 3},
                "description": "child profile",
            },
        }

        resolved = resolve_profile("child", profiles)
        self.assertEqual(resolved["args"]["config"], "configs/streaming/streaming.yaml")
        self.assertEqual(resolved["args"]["repeats"], 3)
        self.assertNotIn("extends", resolved)

    def test_build_command_formats_layer_a_profile_objects(self) -> None:
        profile = {
            "script": "scripts/streaming/official/run_layer_a_matrix.py",
            "runtime": "host",
            "args": {
                "profiles": [
                    {
                        "name": "A_low",
                        "max_offsets_per_trigger": 500,
                        "shuffle_partitions": 4,
                        "trigger_interval": "10 seconds",
                    }
                ]
            },
        }

        command = build_command(profile, "python")
        self.assertEqual(command[0:2], ["python", "scripts/streaming/official/run_layer_a_matrix.py"])
        self.assertIn("A_low:500:4:10 seconds", command)

    def test_build_command_uses_noninteractive_docker_exec(self) -> None:
        profile = {
            "script": "scripts/streaming/official/run_layer_b_matrix.py",
            "runtime": "docker",
            "docker_service": "ids-dev",
            "args": {"config": "configs/streaming/streaming.yaml"},
        }

        command = build_command(profile, "python")
        self.assertEqual(command[0:6], ["docker", "compose", "exec", "-T", "ids-dev", "python"])
        self.assertIn("scripts/streaming/official/run_layer_b_matrix.py", command)

    def test_include_profile_by_filters_respects_mode_and_resource_class(self) -> None:
        args = Namespace(
            list_official_only=True,
            list_ablation_only=False,
            list_light_only=False,
            list_heavy_only=False,
        )
        self.assertTrue(include_profile_by_filters({"mode": "official", "resource_class": "light"}, args))
        self.assertFalse(include_profile_by_filters({"mode": "ablation", "resource_class": "light"}, args))

    def test_evaluate_pass_fail_supports_threshold_checks(self) -> None:
        rows = [
            {"status": "ok", "rows_total": "1200", "fpr_avg": "0.01", "fnr_avg": "0.02"},
            {"status": "pass", "rows_total": "1300", "fpr_avg": "0.02", "fnr_avg": "0.03"},
        ]
        passed, details = evaluate_pass_fail(
            rows,
            {"required": True, "min_rows_total": 2000, "max_fpr": 0.05, "max_fnr": 0.05},
        )
        self.assertTrue(passed)
        self.assertTrue(any("status gate" in detail for detail in details))

    def test_evaluate_profile_gates_reads_summary_csv(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            summary_path = Path(temp_dir) / "summary.csv"
            summary_path.write_text(
                "status,rows_total,fpr_avg,fnr_avg\nok,1500,0.01,0.02\n",
                encoding="utf-8",
            )
            resolved_profile = {
                "args": {"summary_csv": str(summary_path)},
                "meta": {"pass_fail": {"required": True, "min_rows_total": 1000, "max_fpr": 0.05}},
            }

            exit_code, lines = evaluate_profile_gates("test_profile", resolved_profile)
            self.assertEqual(exit_code, 0)
            self.assertTrue(any("PASS" in line for line in lines))

    def test_evaluate_pass_fail_prefers_explicit_latency_columns(self) -> None:
        rows = [
            {
                "status": "ok",
                "source_to_emit_p95_ms": "80.0",
                "e2e_p95_ms": "999.0",
                "ingest_to_emit_p95_ms": "20.0",
                "proc_p95_ms": "999.0",
            }
        ]

        passed, details = evaluate_pass_fail(
            rows,
            {
                "required": True,
                "max_source_to_emit_p95_ms": 100.0,
                "max_ingest_to_emit_p95_ms": 30.0,
                "max_e2e_p95_ms": 100.0,
                "max_proc_p95_ms": 30.0,
            },
        )

        self.assertTrue(passed, msg=details)
        self.assertTrue(any("source_to_emit_p95_ms" in detail for detail in details))
        self.assertTrue(any("ingest_to_emit_p95_ms" in detail for detail in details))


if __name__ == "__main__":
    unittest.main()

