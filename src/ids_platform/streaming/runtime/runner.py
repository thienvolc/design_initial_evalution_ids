from __future__ import annotations

import threading
import time
from dataclasses import dataclass

QUERY_STOP_TIMEOUT_SEC = 30


@dataclass(frozen=True)
class RuntimeQueries:
    data_queries: list[tuple[object, str]]
    all_queries: list[tuple[object, str]]
    active_queries: list[object]
    sentinel_query: object | None


class StructuredStreamingRunner:
    def __init__(
        self,
        *,
        queries: RuntimeQueries,
        sentinel_seen: threading.Event,
    ) -> None:
        self.queries = queries
        self.sentinel_seen = sentinel_seen
        self._stopped = False

    def wait(self, *, timeout_sec: float | None = None) -> None:
        deadline = None
        if timeout_sec is not None:
            deadline = time.time() + max(float(timeout_sec), 0.0)
        try:
            while not self.sentinel_seen.is_set():
                if deadline is not None and time.time() >= deadline:
                    raise RuntimeError("timed out waiting for input sentinel")
                inactive = [
                    name
                    for query, name in self.queries.data_queries
                    if not self._query_is_active(query)
                ]
                if inactive:
                    raise RuntimeError(
                        "streaming data query stopped before input sentinel: "
                        + ", ".join(inactive)
                    )
                time.sleep(0.5)
            self._drain_queries(self.queries.data_queries)
        finally:
            self.stop()

    def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        self._stop_queries(self.queries.all_queries)

    @staticmethod
    def _query_is_active(query) -> bool:
        if query is None:
            return False
        return bool(query.isActive)

    @staticmethod
    def _stop_queries(queries: list[tuple[object, str]]) -> None:
        for query, _name in queries:
            if query is not None:
                if query.isActive:
                    query.stop()
                query.awaitTermination(QUERY_STOP_TIMEOUT_SEC)

    @staticmethod
    def _drain_queries(queries: list[tuple[object, str]]) -> None:
        for query, _name in queries:
            if query is not None and query.isActive:
                query.processAllAvailable()
