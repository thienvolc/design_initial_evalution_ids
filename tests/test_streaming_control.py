from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest import mock

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.runtime.control import (  # noqa: E402
    clear_shutdown_request,
    shutdown_request_path,
    write_shutdown_request,
)
from ids_platform.streaming.orchestration import fault_matrix  # noqa: E402


class StreamingControlTests(unittest.TestCase):
    def test_shutdown_request_round_trip(self) -> None:
        run_tag = "unit_test_shutdown_request"
        clear_shutdown_request(run_tag)
        path = write_shutdown_request(run_tag)
        self.assertEqual(path, shutdown_request_path(run_tag))
        self.assertTrue(path.exists())
        clear_shutdown_request(run_tag)
        self.assertFalse(path.exists())

    def test_stop_stream_process_docker_avoids_force_terminate_after_natural_exit(self) -> None:
        process = mock.Mock()
        process.poll.side_effect = [None, None]
        process.ids_execution_mode = "docker"
        process.ids_run_tag = "unit_test_run_tag"

        with (
            mock.patch.object(fault_matrix, "write_shutdown_request") as write_shutdown_request_mock,
            mock.patch.object(fault_matrix, "wait_for_stream_shutdown", return_value=True) as wait_shutdown_mock,
            mock.patch.object(fault_matrix, "wait_for_process_exit", return_value=True) as wait_exit_mock,
            mock.patch.object(fault_matrix, "cleanup_stream_processes") as cleanup_mock,
            mock.patch.object(fault_matrix, "stop_background_process") as stop_background_process_mock,
        ):
            fault_matrix.stop_stream_process(process)

        write_shutdown_request_mock.assert_called_once_with("unit_test_run_tag")
        wait_shutdown_mock.assert_called()
        wait_exit_mock.assert_called_once_with(process, timeout_sec=45)
        cleanup_mock.assert_not_called()
        stop_background_process_mock.assert_called_once_with(process)


if __name__ == "__main__":
    unittest.main()
