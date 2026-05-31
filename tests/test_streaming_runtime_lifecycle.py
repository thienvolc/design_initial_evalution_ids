from __future__ import annotations

import sys
import threading
import unittest
from pathlib import Path
from unittest import mock

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.runtime.runner import (  # noqa: E402
    QUERY_STOP_TIMEOUT_SEC,
    RuntimeQueries,
    StructuredStreamingRunner,
)


class _FakeQuery:
    def __init__(self, *, active: bool = True) -> None:
        self.isActive = active
        self.stop_calls = 0
        self.await_calls: list[float] = []

    def stop(self) -> None:
        self.stop_calls += 1
        self.isActive = False

    def awaitTermination(self, timeout: float) -> bool:
        self.await_calls.append(timeout)
        return not self.isActive


class RuntimeLifecycleTests(unittest.TestCase):
    def test_runner_waits_for_sentinel_then_stops_all_queries(self) -> None:
        data_query = _FakeQuery(active=True)
        sentinel_query = _FakeQuery(active=True)
        sentinel_seen = threading.Event()
        runner = StructuredStreamingRunner(
            queries=RuntimeQueries(
                data_queries=[(data_query, "predictions")],
                all_queries=[(sentinel_query, "input_sentinel"), (data_query, "predictions")],
                active_queries=[data_query, sentinel_query],
                sentinel_query=sentinel_query,
            ),
            sentinel_seen=sentinel_seen,
        )

        with mock.patch("ids_platform.streaming.runtime.runner.time.sleep", side_effect=lambda _delay: sentinel_seen.set()):
            runner.wait()

        self.assertEqual(data_query.stop_calls, 1)
        self.assertEqual(sentinel_query.stop_calls, 1)
        self.assertEqual(data_query.await_calls, [QUERY_STOP_TIMEOUT_SEC])
        self.assertEqual(sentinel_query.await_calls, [QUERY_STOP_TIMEOUT_SEC])

    def test_runner_fails_when_data_query_stops_before_sentinel(self) -> None:
        data_query = _FakeQuery(active=False)
        sentinel_query = _FakeQuery(active=True)
        runner = StructuredStreamingRunner(
            queries=RuntimeQueries(
                data_queries=[(data_query, "predictions")],
                all_queries=[(sentinel_query, "input_sentinel"), (data_query, "predictions")],
                active_queries=[data_query, sentinel_query],
                sentinel_query=sentinel_query,
            ),
            sentinel_seen=threading.Event(),
        )

        with self.assertRaisesRegex(RuntimeError, "before input sentinel"):
            runner.wait()

        self.assertEqual(sentinel_query.stop_calls, 1)
        self.assertEqual(data_query.stop_calls, 0)


if __name__ == "__main__":
    unittest.main()
