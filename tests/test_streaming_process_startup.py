from __future__ import annotations

import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

if "confluent_kafka" not in sys.modules:
    confluent_kafka_stub = types.ModuleType("confluent_kafka")

    class _UnusedConsumer:
        pass

    class _UnusedTopicPartition:
        def __init__(self, *args, **kwargs) -> None:
            pass

    confluent_kafka_stub.Consumer = _UnusedConsumer
    confluent_kafka_stub.TopicPartition = _UnusedTopicPartition
    sys.modules["confluent_kafka"] = confluent_kafka_stub

from ids_platform.streaming.evaluation.matrices.common import wait_for_process_startup  # noqa: E402
from ids_platform.streaming.evaluation.matrices.common import wait_for_log_quiescence  # noqa: E402
import ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix as layer_c_facade  # noqa: E402
from ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix import (  # noqa: E402
    LayerCFaultMatrixOptions,
    _execute_scenario,
    _next_run_tag,
    _scenario_requires_real_network_fault,
    _validate_runtime_metric,
    run,
)


class _FakeProcess:
    def __init__(self) -> None:
        self.ids_log_start_offset = 0

    def poll(self):
        return None


class ProcessStartupTests(unittest.TestCase):
    def test_layer_c_facade_exports_expected_compatibility_symbols(self) -> None:
        expected_symbols = {
            "LayerCFaultMatrixOptions",
            "_execute_scenario",
            "_load_docker_kafka_bootstrap",
            "_load_host_kafka_runtime_targets",
            "_next_run_tag",
            "_scenario_requires_real_network_fault",
            "_validate_runtime_metric",
            "cleanup_stream_processes",
            "describe_kafka_topics_state",
            "fetch_metric",
            "replay_with_retries",
            "restart_service",
            "run",
            "run_layer_c_fault_matrix",
            "start_stream_process",
            "stop_stream_process",
            "time",
            "wait_for_kafka_bootstrap_ready",
            "wait_for_kafka_topics_ready",
            "wait_for_log_patterns",
            "wait_for_process_exit",
            "wait_for_process_startup",
            "wait_for_stream_shutdown",
        }

        self.assertTrue(expected_symbols.issubset(set(layer_c_facade.__all__)))
        for symbol in expected_symbols:
            self.assertTrue(hasattr(layer_c_facade, symbol), symbol)

    def test_layer_c_run_wrapper_delegates_to_scenario_flow(self) -> None:
        options = LayerCFaultMatrixOptions(
            config="configs/streaming/streaming.yaml",
            model="logistic_regression",
            feature_set="full",
            scenarios=("kafka_restart",),
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
            summary_csv="artifacts/summary.csv",
        )

        with mock.patch(
            "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix.run_layer_c_fault_matrix",
            return_value=7,
        ) as delegate_mock:
            result = run(options)

        self.assertEqual(result, 7)
        delegate_mock.assert_called_once_with(options)

    def test_wait_for_process_startup_ignores_old_log_content_before_start_offset(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            log_path = Path(temp_dir) / "runtime.log"
            log_path.write_text(
                "[stream] event=job_start run_tag=layerC_test_old\n",
                encoding="utf-8",
            )

            process = _FakeProcess()
            process.ids_log_start_offset = log_path.stat().st_size

            ready = wait_for_process_startup(
                process,
                startup_wait_sec=1,
                poll_seconds=0.1,
                ready_log_path=str(log_path),
                ready_pattern="[stream] event=job_start run_tag=layerC_test_old",
                require_ready_marker=True,
            )
            self.assertFalse(ready)

            with log_path.open("a", encoding="utf-8") as handle:
                handle.write("[stream] event=job_start run_tag=layerC_test_new\n")

            ready = wait_for_process_startup(
                process,
                startup_wait_sec=1,
                poll_seconds=0.1,
                ready_log_path=str(log_path),
                ready_pattern="[stream] event=job_start run_tag=layerC_test_new",
                require_ready_marker=True,
            )
            self.assertTrue(ready)

    def test_wait_for_log_quiescence_returns_true_when_log_stops_growing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            log_path = Path(temp_dir) / "runtime.log"
            log_path.write_text("line-1\n", encoding="utf-8")
            process = _FakeProcess()

            idle = wait_for_log_quiescence(
                process=process,
                log_path=str(log_path),
                idle_sec=0.2,
                timeout_sec=1.0,
                poll_seconds=0.05,
            )

            self.assertTrue(idle)

    def test_layer_c_runtime_metric_validation_rejects_terminal_or_empty_payloads(self) -> None:
        ok, reason = _validate_runtime_metric({"event_type": "run_completed", "rows": 10})
        self.assertFalse(ok)
        self.assertIn("unexpected metric event_type", reason)

        ok, reason = _validate_runtime_metric({"event_type": "batch_metrics", "rows": 0})
        self.assertFalse(ok)
        self.assertIn("missing_or_zero", reason)

        ok, reason = _validate_runtime_metric({"event_type": "batch_metrics", "rows": 5})
        self.assertTrue(ok)
        self.assertEqual(reason, "")

    def test_layer_c_run_tag_uses_millisecond_precision(self) -> None:
        tag1 = _next_run_tag(index=2, scenario="spark_process_restart")
        tag2 = _next_run_tag(index=2, scenario="spark_process_restart")
        self.assertTrue(tag1.startswith("layerC_02_spark_process_restart_"))
        self.assertTrue(tag2.startswith("layerC_02_spark_process_restart_"))
        self.assertNotEqual(tag1, "")
        self.assertNotEqual(tag2, "")

    def test_network_slowdown_is_marked_as_requiring_real_network_fault(self) -> None:
        self.assertTrue(_scenario_requires_real_network_fault("network_slowdown"))
        self.assertFalse(_scenario_requires_real_network_fault("producer_restart"))

    def test_layer_c_kafka_restart_allows_late_ready_recheck_before_failing(self) -> None:
        options = LayerCFaultMatrixOptions(
            config="configs/streaming/streaming.yaml",
            model="logistic_regression",
            feature_set="full",
            scenarios=("kafka_restart",),
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
            summary_csv="artifacts/summary.csv",
        )

        process = mock.Mock()
        process.poll.return_value = None
        process.ids_log_path = "runtime.log"
        process.ids_log_start_offset = 0

        wait_topics_results = [False, False, True, True, True, True]
        topic_states = ["ready", "ready", "ready", "ready"]

        with (
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix.cleanup_stream_processes"
            ),
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix.start_stream_process",
                return_value=process,
            ),
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix.wait_for_process_startup",
                return_value=True,
            ),
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix.replay_with_retries"
            ),
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix.fetch_metric",
                side_effect=[
                    {"event_type": "batch_metrics", "rows": 10},
                    {
                        "event_type": "batch_metrics",
                        "rows": 12,
                        "rows_per_sec": 5.0,
                        "ts_utc": "2026-04-24T00:00:05+00:00",
                        "latency_ms": {
                            "processing": {"p95": 1.0},
                            "ingest_to_emit": {"p95": 2.0},
                            "end_to_end": {"p95": 3.0},
                            "source_to_emit": {"p95": 4.0},
                        },
                    },
                ],
            ),
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix.restart_service"
            ),
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix._load_host_kafka_runtime_targets",
                return_value=("localhost:9092", ["ids.raw.flows", "ids.predictions.binary", "ids.metrics"]),
            ),
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix._load_docker_kafka_bootstrap",
                return_value="kafka:29092",
            ),
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix.wait_for_kafka_bootstrap_ready",
                side_effect=[True, True],
            ),
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix.wait_for_kafka_topics_ready",
                side_effect=wait_topics_results,
            ) as wait_topics_mock,
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix.describe_kafka_topics_state",
                side_effect=topic_states,
            ),
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix.wait_for_log_patterns",
                return_value="",
            ),
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix.wait_for_stream_shutdown",
                return_value=True,
            ),
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix.wait_for_process_exit",
                return_value=True,
            ),
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix.stop_stream_process"
            ),
            mock.patch("ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix.time.sleep"),
        ):
            row = _execute_scenario(options, "kafka_restart", 1, "")

        self.assertEqual(row["status"], "ok")
        self.assertEqual(row["notes"], "")
        self.assertGreaterEqual(wait_topics_mock.call_count, 6)

    def test_layer_c_kafka_restart_accepts_ready_state_after_late_recheck_timeout(self) -> None:
        options = LayerCFaultMatrixOptions(
            config="configs/streaming/streaming.yaml",
            model="logistic_regression",
            feature_set="full",
            scenarios=("kafka_restart",),
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
            summary_csv="artifacts/summary.csv",
        )

        process = mock.Mock()
        process.poll.return_value = None
        process.ids_log_path = "runtime.log"
        process.ids_log_start_offset = 0

        with (
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix.cleanup_stream_processes"
            ),
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix.start_stream_process",
                return_value=process,
            ),
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix.wait_for_process_startup",
                return_value=True,
            ),
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix.replay_with_retries"
            ),
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix.fetch_metric",
                side_effect=[
                    {"event_type": "batch_metrics", "rows": 10},
                    {
                        "event_type": "batch_metrics",
                        "rows": 12,
                        "rows_per_sec": 5.0,
                        "ts_utc": "2026-04-24T00:00:05+00:00",
                        "latency_ms": {
                            "processing": {"p95": 1.0},
                            "ingest_to_emit": {"p95": 2.0},
                            "end_to_end": {"p95": 3.0},
                            "source_to_emit": {"p95": 4.0},
                        },
                    },
                ],
            ),
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix.restart_service"
            ),
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix._load_host_kafka_runtime_targets",
                return_value=("localhost:9092", ["ids.raw.flows", "ids.predictions.binary", "ids.metrics"]),
            ),
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix._load_docker_kafka_bootstrap",
                return_value="kafka:29092",
            ),
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix.wait_for_kafka_bootstrap_ready",
                side_effect=[True, True],
            ),
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix.wait_for_kafka_topics_ready",
                side_effect=[False, False, True, False, True, True],
            ),
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix.describe_kafka_topics_state",
                side_effect=["ready", "ready", "ready", "ready"],
            ),
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix.wait_for_log_patterns",
                return_value="",
            ),
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix.wait_for_stream_shutdown",
                return_value=True,
            ),
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix.wait_for_process_exit",
                return_value=True,
            ),
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix.stop_stream_process"
            ),
            mock.patch("ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix.time.sleep"),
        ):
            row = _execute_scenario(options, "kafka_restart", 1, "")

        self.assertEqual(row["status"], "ok")
        self.assertEqual(row["notes"], "")

    def test_layer_c_kafka_restart_accepts_ready_state_after_settle_timeout(self) -> None:
        options = LayerCFaultMatrixOptions(
            config="configs/streaming/streaming.yaml",
            model="logistic_regression",
            feature_set="full",
            scenarios=("kafka_restart",),
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
            summary_csv="artifacts/summary.csv",
        )

        process = mock.Mock()
        process.poll.return_value = None
        process.ids_log_path = "runtime.log"
        process.ids_log_start_offset = 0

        with (
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix.cleanup_stream_processes"
            ),
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix.start_stream_process",
                return_value=process,
            ),
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix.wait_for_process_startup",
                return_value=True,
            ),
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix.replay_with_retries"
            ),
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix.fetch_metric",
                side_effect=[
                    {"event_type": "batch_metrics", "rows": 10},
                    {
                        "event_type": "batch_metrics",
                        "rows": 12,
                        "rows_per_sec": 5.0,
                        "ts_utc": "2026-04-24T00:00:05+00:00",
                        "latency_ms": {
                            "processing": {"p95": 1.0},
                            "ingest_to_emit": {"p95": 2.0},
                            "end_to_end": {"p95": 3.0},
                            "source_to_emit": {"p95": 4.0},
                        },
                    },
                ],
            ),
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix.restart_service"
            ),
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix._load_host_kafka_runtime_targets",
                return_value=("localhost:9092", ["ids.raw.flows", "ids.predictions.binary", "ids.metrics"]),
            ),
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix._load_docker_kafka_bootstrap",
                return_value="kafka:29092",
            ),
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix.wait_for_kafka_bootstrap_ready",
                side_effect=[True, True],
            ),
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix.wait_for_kafka_topics_ready",
                side_effect=[True, True, True, False],
            ),
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix.describe_kafka_topics_state",
                side_effect=["ready", "ready"],
            ),
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix.wait_for_log_patterns",
                return_value="",
            ),
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix.wait_for_stream_shutdown",
                return_value=True,
            ),
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix.wait_for_process_exit",
                return_value=True,
            ),
            mock.patch(
                "ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix.stop_stream_process"
            ),
            mock.patch("ids_platform.streaming.evaluation.matrices.layer_c_fault_matrix.time.sleep"),
        ):
            row = _execute_scenario(options, "kafka_restart", 1, "")

        self.assertEqual(row["status"], "ok")
        self.assertEqual(row["notes"], "")

    def test_layer_c_network_slowdown_fails_fast_as_unsupported_surrogate(self) -> None:
        options = LayerCFaultMatrixOptions(
            config="configs/streaming/streaming.yaml",
            model="logistic_regression",
            feature_set="full",
            scenarios=("network_slowdown",),
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
            summary_csv="artifacts/summary.csv",
        )

        row = _execute_scenario(options, "network_slowdown", 1, "")

        self.assertEqual(row["status"], "failed")
        self.assertIn("no real network fault injector", row["notes"])


if __name__ == "__main__":
    unittest.main()
