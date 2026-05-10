from __future__ import annotations

import csv
import shutil
import subprocess
import sys
import tempfile
import textwrap
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest import mock


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TEST_TEMP_ROOT = PROJECT_ROOT / "artifacts" / "tmp_tests"
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


@contextmanager
def _workspace_tempdir():
    TEST_TEMP_ROOT.mkdir(parents=True, exist_ok=True)
    temp_dir = tempfile.mkdtemp(dir=TEST_TEMP_ROOT)
    try:
        yield Path(temp_dir)
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


class StreamingEntrypointIntegrationTests(unittest.TestCase):
    def test_layer_c_scenario_flow_run_writes_summary_and_timeseries(self) -> None:
        from ids_platform.streaming.evaluation.matrices.layer_c.scenario_flow import run_layer_c_fault_matrix
        from ids_platform.streaming.evaluation.matrices.layer_c.types import LayerCFaultMatrixOptions

        with _workspace_tempdir() as temp_path:
            summary_csv = temp_path / "layer_c_summary.csv"
            options = LayerCFaultMatrixOptions(
                config="configs/streaming/streaming.yaml",
                model="logistic_regression",
                feature_set="full",
                scenarios=("kafka_restart", "producer_restart"),
                warmup_rows=10,
                warmup_rows_per_sec=1.0,
                warmup_rate_schedule="",
                fault_delay_sec=0,
                post_fault_rows=10,
                post_fault_rows_per_sec=1.0,
                batch_size=10,
                trace_input_parquet="artifacts/demo.parquet",
                trace_order_column="event_id",
                slowdown_rows_per_sec=1.0,
                producer_restart_pause_sec=1,
                replay_retries=1,
                replay_retry_wait_sec=1,
                stream_run_seconds=60,
                startup_wait_sec=1,
                metrics_timeout_sec=90,
                execution_mode="docker",
                python_executable="python",
                bootstrap_servers="kafka:29092",
                summary_csv=str(summary_csv),
            )

            execute_rows = [
                {
                    "run_tag": "layerC_01_kafka_restart_demo",
                    "status": "ok",
                    "recovery_seconds": 12.5,
                    "notes": "",
                },
                {
                    "run_tag": "layerC_02_producer_restart_demo",
                    "status": "failed",
                    "recovery_seconds": "",
                    "notes": "demo failure",
                },
            ]
            metrics_rows = [
                [
                    {"event_type": "batch_metrics", "run_tag": "layerC_01_kafka_restart_demo", "batch_id": 2, "rows": 20},
                    {"event_type": "run_completed", "run_tag": "layerC_01_kafka_restart_demo"},
                ],
                [],
            ]
            written_timeseries = [
                str(temp_path / "ts_1.csv"),
                None,
            ]

            with (
                mock.patch(
                    "ids_platform.streaming.evaluation.matrices.layer_c.scenario_flow.execute_scenario",
                    side_effect=execute_rows,
                ) as execute_mock,
                mock.patch(
                    "ids_platform.streaming.evaluation.matrices.layer_c.scenario_flow.collect_scenario_metrics",
                    side_effect=metrics_rows,
                ) as collect_mock,
                mock.patch(
                    "ids_platform.streaming.evaluation.matrices.layer_c.scenario_flow.write_metrics_timeseries",
                    side_effect=written_timeseries,
                ) as timeseries_mock,
            ):
                result = run_layer_c_fault_matrix(options)

            self.assertEqual(result, 0)
            self.assertTrue(summary_csv.exists())
            self.assertEqual(execute_mock.call_count, 2)
            self.assertEqual(timeseries_mock.call_count, 2)

            first_collect_call = collect_mock.call_args_list[0]
            self.assertEqual(first_collect_call.kwargs["config"], options.config)
            self.assertEqual(first_collect_call.kwargs["run_tag"], "layerC_01_kafka_restart_demo")
            self.assertEqual(first_collect_call.kwargs["timeout_sec"], options.metrics_timeout_sec)
            self.assertEqual(first_collect_call.kwargs["bootstrap_servers_override"], options.bootstrap_servers)
            self.assertNotIn("execution_mode", first_collect_call.kwargs)

            with summary_csv.open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["run_tag"], "layerC_01_kafka_restart_demo")
            self.assertEqual(rows[0]["status"], "ok")
            self.assertEqual(rows[1]["run_tag"], "layerC_02_producer_restart_demo")
            self.assertEqual(rows[1]["status"], "failed")

    def test_streaming_entrypoints_support_help_cli(self) -> None:
        scripts = [
            "scripts/streaming/official/run_structured_streaming.py",
            "scripts/streaming/official/replay_parquet_to_kafka.py",
            "scripts/streaming/official/run_layer_a_matrix.py",
            "scripts/streaming/official/run_layer_b_matrix.py",
            "scripts/streaming/official/run_layer_c_matrix.py",
            "scripts/streaming/official/run_watermark_matrix.py",
            "scripts/streaming/official/run_load_quality_matrix.py",
            "scripts/streaming/official/run_streaming_profile.py",
            "scripts/streaming/official/build_timeseries_plots.py",
            "scripts/streaming/benchmark/run_benchmark_matrix.py",
            "scripts/streaming/benchmark/run_pandas_udf_benchmark.py",
            "scripts/streaming/observability/export_prometheus_summary.py",
        ]

        for script_path in scripts:
            with self.subTest(script=script_path):
                result = _run_script([script_path, "--help"])
                self.assertEqual(result.returncode, 0, msg=result.stderr)
                self.assertIn("usage:", result.stdout.lower())

    def test_streaming_profile_dry_run_resolves_command(self) -> None:
        with _workspace_tempdir() as temp_path:
            profile_config = temp_path / "profiles.yaml"
            profile_config.write_text(
                textwrap.dedent(
                    """
                    profiles:
                      demo_profile:
                        script: scripts/streaming/official/run_layer_a_matrix.py
                        runtime: host
                        args:
                          config: configs/streaming/streaming.yaml
                          repeats: 2
                          max_rows: 100
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            result = _run_script(
                [
                    "scripts/streaming/official/run_streaming_profile.py",
                    "--profile-config",
                    str(profile_config),
                    "--profile",
                    "demo_profile",
                    "--dry-run",
                ]
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertIn("Resolved command:", result.stdout)
            self.assertIn("scripts/streaming/official/run_layer_a_matrix.py", result.stdout)
            self.assertIn("--repeats 2", result.stdout)

    def test_streaming_profile_gate_only_evaluates_existing_summary(self) -> None:
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
                        script: scripts/streaming/official/run_layer_b_matrix.py
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
                    "scripts/streaming/official/run_streaming_profile.py",
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


    @staticmethod
    def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)


if __name__ == "__main__":
    unittest.main()


