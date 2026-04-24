from __future__ import annotations

import csv
import json
import shutil
import subprocess
import sys
import tempfile
import textwrap
import unittest
from contextlib import contextmanager
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TEST_TEMP_ROOT = PROJECT_ROOT / "artifacts" / "tmp_tests"


def _run_script(arguments: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, *arguments],
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


@contextmanager
def _workspace_tempdir():
    TEST_TEMP_ROOT.mkdir(parents=True, exist_ok=True)
    temp_dir = tempfile.mkdtemp(dir=TEST_TEMP_ROOT)
    try:
        yield Path(temp_dir)
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


class StreamingEntrypointIntegrationTests(unittest.TestCase):
    def test_online_profile_dry_run_resolves_command(self) -> None:
        with _workspace_tempdir() as temp_path:
            profile_config = temp_path / "profiles.yaml"
            profile_config.write_text(
                textwrap.dedent(
                    """
                    profiles:
                      demo_profile:
                        script: scripts/streaming/run_layer_a_matrix.py
                        runtime: host
                        args:
                          config: configs/streaming/online.yaml
                          repeats: 2
                          max_rows: 100
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            result = _run_script(
                [
                    "scripts/streaming/run_online_profile.py",
                    "--profile-config",
                    str(profile_config),
                    "--profile",
                    "demo_profile",
                    "--dry-run",
                ]
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertIn("Resolved command:", result.stdout)
            self.assertIn("scripts/streaming/run_layer_a_matrix.py", result.stdout)
            self.assertIn("--repeats 2", result.stdout)

    def test_online_profile_gate_only_evaluates_existing_summary(self) -> None:
        with _workspace_tempdir() as temp_path:
            summary_csv = temp_path / "summary.csv"
            summary_csv.write_text(
                "status,rows_total,fpr_avg,fnr_avg\nok,1500,0.01,0.02\n",
                encoding="utf-8",
            )
            profile_config = temp_path / "profiles.yaml"
            profile_config.write_text(
                textwrap.dedent(
                    f"""
                    profiles:
                      gate_profile:
                        script: scripts/streaming/run_layer_b_matrix.py
                        runtime: host
                        args:
                          summary_csv: {summary_csv.as_posix()}
                        meta:
                          pass_fail:
                            required: true
                            min_rows_total: 1000
                            max_fpr: 0.05
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            result = _run_script(
                [
                    "scripts/streaming/run_online_profile.py",
                    "--profile-config",
                    str(profile_config),
                    "--profile",
                    "gate_profile",
                    "--gate-only",
                ]
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertIn("Gate-only mode", result.stdout)
            self.assertIn("PASS", result.stdout)

    def test_online_eval_report_generates_markdown_and_json(self) -> None:
        with _workspace_tempdir() as temp_path:
            layer_a = temp_path / "layer_a.csv"
            layer_b = temp_path / "layer_b.csv"
            layer_c = temp_path / "layer_c.csv"
            out_md = temp_path / "report.md"
            out_json = temp_path / "report.json"

            self._write_csv(
                layer_a,
                [
                    "status",
                    "profile",
                    "max_offsets_per_trigger",
                    "shuffle_partitions",
                    "trigger_interval",
                    "load_profile",
                    "rows_per_sec_avg",
                    "source_to_emit_p95_ms_max",
                    "e2e_p95_ms_max",
                    "watermark_delay_sec",
                    "metric_warnings",
                    "driver_cpu_percent_avg",
                    "driver_rss_mb_avg",
                    "executor_mem_util_avg",
                ],
                [
                    {
                        "status": "ok",
                        "profile": "A_mid",
                        "max_offsets_per_trigger": "2000",
                        "shuffle_partitions": "8",
                        "trigger_interval": "10 seconds",
                        "load_profile": "A_mid",
                        "rows_per_sec_avg": "100.0",
                        "source_to_emit_p95_ms_max": "50.0",
                        "e2e_p95_ms_max": "50.0",
                        "watermark_delay_sec": "0",
                        "metric_warnings": "driver_cpu_percent_unavailable_without_psutil",
                        "driver_cpu_percent_avg": "10.0",
                        "driver_rss_mb_avg": "256.0",
                        "executor_mem_util_avg": "0.2",
                    }
                ],
            )
            self._write_csv(
                layer_b,
                [
                    "status",
                    "model",
                    "feature_set",
                    "load_profile",
                    "rows",
                    "source_to_emit_p95_ms",
                    "ingest_to_emit_p95_ms",
                    "e2e_p95_ms",
                    "proc_p95_ms",
                    "watermark_delay_sec",
                    "metric_warnings",
                    "driver_cpu_percent",
                    "driver_rss_mb",
                    "executor_mem_util_avg",
                ],
                [
                    {
                        "status": "ok",
                        "model": "logistic_regression",
                        "feature_set": "full",
                        "load_profile": "logistic_regression:full",
                        "rows": "1000",
                        "source_to_emit_p95_ms": "20.0",
                        "ingest_to_emit_p95_ms": "5.0",
                        "e2e_p95_ms": "20.0",
                        "proc_p95_ms": "5.0",
                        "watermark_delay_sec": "0",
                        "metric_warnings": "driver_cpu_percent_unavailable_without_psutil",
                        "driver_cpu_percent": "12.0",
                        "driver_rss_mb": "128.0",
                        "executor_mem_util_avg": "0.1",
                    }
                ],
            )
            self._write_csv(
                layer_c,
                ["status", "scenario", "recovery_seconds", "rows_per_sec_after_fault", "proc_p95_ms_after_fault", "e2e_p95_ms_after_fault"],
                [
                    {
                        "status": "ok",
                        "scenario": "kafka_restart",
                        "recovery_seconds": "12.0",
                        "rows_per_sec_after_fault": "80.0",
                        "proc_p95_ms_after_fault": "9.0",
                        "e2e_p95_ms_after_fault": "30.0",
                    }
                ],
            )

            result = _run_script(
                [
                    "scripts/streaming/build_online_report.py",
                    "--layer-a",
                    str(layer_a),
                    "--layer-b",
                    str(layer_b),
                    "--layer-c",
                    str(layer_c),
                    "--out-md",
                    str(out_md),
                    "--out-json",
                    str(out_json),
                ]
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertTrue(out_md.exists())
            self.assertTrue(out_json.exists())
            self.assertIn("Saved report markdown:", result.stdout)

            report_payload = json.loads(out_json.read_text(encoding="utf-8"))
            self.assertEqual(report_payload["layer_a"]["best"]["profile"], "A_mid")
            self.assertEqual(report_payload["layer_b"]["best"]["model"], "logistic_regression")
            self.assertEqual(report_payload["layer_c"]["worst_recovery_seconds"], 12.0)
            self.assertEqual(report_payload["methodology"]["boundary_mode"], "sut_metrics_only")
            self.assertTrue(report_payload["warnings"])
            self.assertEqual(report_payload["layer_a"]["best"]["source_to_emit_p95_ms_p95"], 50.0)
            self.assertEqual(report_payload["layer_b"]["best"]["source_to_emit_p95_ms_p95"], 20.0)
            self.assertEqual(
                report_payload["methodology"]["official_evaluation"]["source_of_truth"],
                "matrix_summaries_derived_from_ids_metrics",
            )

            markdown = out_md.read_text(encoding="utf-8")
            self.assertIn("# Online Evaluation Report", markdown)
            self.assertIn("## Methodology", markdown)
            self.assertIn("Layer A (System Knobs)", markdown)
            self.assertIn("matrix runners currently aggregate SUT-emitted `ids.metrics`", markdown)
            self.assertIn("late_event_ratio was collected with watermark_delay_sec=0", markdown)
            self.assertIn("source_to_emit_p95_ms_p95", markdown)
            self.assertIn("Probe warning observed in metrics: driver_cpu_percent_unavailable_without_psutil", markdown)
            self.assertTrue(
                any("compatibility alias" in warning for warning in report_payload["warnings"]),
                msg=report_payload["warnings"],
            )

    @staticmethod
    def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)


if __name__ == "__main__":
    unittest.main()


