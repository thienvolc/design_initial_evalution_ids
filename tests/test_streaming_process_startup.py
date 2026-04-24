from __future__ import annotations

import sys
import tempfile
import types
import unittest
from pathlib import Path

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

from ids_platform.streaming.matrices.common import wait_for_process_startup  # noqa: E402
from ids_platform.streaming.matrices.common import wait_for_log_quiescence  # noqa: E402
from ids_platform.streaming.matrices.layer_c_fault_matrix import (  # noqa: E402
    _next_run_tag,
    _validate_runtime_metric,
)


class _FakeProcess:
    def __init__(self) -> None:
        self.ids_log_start_offset = 0

    def poll(self):
        return None


class ProcessStartupTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
