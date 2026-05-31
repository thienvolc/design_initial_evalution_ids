from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest import mock

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.runtime.structured_streaming_job import (
    _any_query_active,
    _query_is_active,
    _stop_queries_gracefully,
)


class _FakeQuery:
    def __init__(self, active: bool) -> None:
        self.isActive = active


class _ExplodingQuery:
    @property
    def isActive(self):
        raise RuntimeError("query state unavailable")


class StructuredJobHelperTests(unittest.TestCase):
    def test_query_is_active_handles_none_and_exceptions(self) -> None:
        self.assertFalse(_query_is_active(None))
        self.assertFalse(_query_is_active(_ExplodingQuery()))
        self.assertTrue(_query_is_active(_FakeQuery(True)))
        self.assertFalse(_query_is_active(_FakeQuery(False)))

    def test_any_query_active_uses_defensive_active_check(self) -> None:
        queries = [
            (_ExplodingQuery(), "broken"),
            (_FakeQuery(False), "inactive"),
            (_FakeQuery(True), "active"),
        ]

        self.assertTrue(_any_query_active(queries))

    def test_stop_queries_gracefully_skips_none_queries(self) -> None:
        query = _FakeQuery(True)

        with mock.patch(
            "ids_platform.streaming.runtime.structured_streaming_job.stop_query_gracefully"
        ) as stop:
            _stop_queries_gracefully([(None, "missing"), (query, "metrics")])

        stop.assert_called_once_with(query, name="metrics")


if __name__ == "__main__":
    unittest.main()
