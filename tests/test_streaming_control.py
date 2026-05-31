from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest import mock

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.evaluation.orchestration import fault_matrix  # noqa: E402


class StreamingControlTests(unittest.TestCase):
    def test_stop_stream_process_uses_kill_then_process_cleanup(self) -> None:
        process = mock.Mock()
        process.ids_execution_mode = "docker"
        process.ids_run_tag = "unit_test_run_tag"

        with (
            mock.patch.object(fault_matrix, "wait_for_stream_shutdown", return_value=True) as wait_shutdown_mock,
            mock.patch.object(fault_matrix, "wait_for_process_exit", return_value=False) as wait_exit_mock,
            mock.patch.object(fault_matrix, "cleanup_stream_processes") as cleanup_mock,
            mock.patch.object(fault_matrix, "stop_background_process") as stop_background_process_mock,
        ):
            fault_matrix.stop_stream_process(process)

        wait_exit_mock.assert_called_once_with(process, timeout_sec=15)
        cleanup_mock.assert_called_once_with(run_tag="unit_test_run_tag", execution_mode="docker")
        wait_shutdown_mock.assert_called_once_with(
            run_tag="unit_test_run_tag",
            execution_mode="docker",
            timeout_sec=45,
            poll_sec=0.5,
            settle_sec=2.0,
        )
        stop_background_process_mock.assert_called_once_with(process)

    def test_wait_for_stream_shutdown_rechecks_after_settle_window(self) -> None:
        with mock.patch.object(
            fault_matrix,
            "_iter_docker_stream_processes",
            side_effect=[
                [{"pid": 101, "command_line": "run_structured_streaming.py demo"}],
                [],
                [{"pid": 102, "command_line": "run_structured_streaming.py demo"}],
                [],
                [],
            ],
        ):
            stopped = fault_matrix.wait_for_stream_shutdown(
                run_tag="demo",
                execution_mode="docker",
                timeout_sec=2,
                poll_sec=0.01,
                settle_sec=0.01,
            )

        self.assertTrue(stopped)

    def test_wait_for_kafka_topics_ready_uses_docker_probe_in_docker_mode(self) -> None:
        with mock.patch.object(
            fault_matrix,
            "run_command",
            side_effect=[
                mock.Mock(returncode=1, stdout="", stderr="not ready"),
                mock.Mock(returncode=0, stdout="", stderr=""),
                mock.Mock(returncode=0, stdout="", stderr=""),
            ],
        ) as run_command_mock:
            ready = fault_matrix.wait_for_kafka_topics_ready(
                bootstrap_servers="kafka:29092",
                topic_names=["ids.raw.flows", "ids.metrics"],
                timeout_sec=1,
                poll_sec=0.01,
                execution_mode="docker",
                consecutive_successes=2,
            )

        self.assertTrue(ready)
        self.assertEqual(run_command_mock.call_count, 3)
        first_call = run_command_mock.call_args_list[0]
        self.assertIn("docker", first_call.args[0])
        self.assertIn("ids-dev", first_call.args[0])
        self.assertIn("kafka:29092", first_call.args[0])

    def test_wait_for_kafka_topics_ready_returns_false_when_docker_probe_never_recovers(self) -> None:
        with mock.patch.object(
            fault_matrix,
            "run_command",
            return_value=mock.Mock(returncode=1, stdout="", stderr="not ready"),
        ):
            ready = fault_matrix.wait_for_kafka_topics_ready(
                bootstrap_servers="kafka:29092",
                topic_names=["ids.metrics"],
                timeout_sec=1,
                poll_sec=0.01,
                execution_mode="docker",
                consecutive_successes=2,
            )

        self.assertFalse(ready)

    def test_wait_for_kafka_topics_ready_docker_resets_success_streak_after_failure(self) -> None:
        with mock.patch.object(
            fault_matrix,
            "run_command",
            side_effect=[
                mock.Mock(returncode=0, stdout="", stderr=""),
                mock.Mock(returncode=1, stdout="", stderr="not ready"),
                mock.Mock(returncode=0, stdout="", stderr=""),
                mock.Mock(returncode=0, stdout="", stderr=""),
            ],
        ):
            ready = fault_matrix.wait_for_kafka_topics_ready(
                bootstrap_servers="kafka:29092",
                topic_names=["ids.metrics"],
                timeout_sec=1,
                poll_sec=0.01,
                execution_mode="docker",
                consecutive_successes=2,
            )

        self.assertTrue(ready)

    def test_describe_kafka_topics_state_reads_docker_probe_output(self) -> None:
        with mock.patch.object(
            fault_matrix,
            "run_command",
            return_value=mock.Mock(returncode=0, stdout="leader_unavailable:ids.metrics:0\n", stderr=""),
        ):
            state = fault_matrix.describe_kafka_topics_state(
                bootstrap_servers="kafka:29092",
                topic_names=["ids.metrics"],
                execution_mode="docker",
            )

        self.assertEqual(state, "leader_unavailable:ids.metrics:0")


if __name__ == "__main__":
    unittest.main()
