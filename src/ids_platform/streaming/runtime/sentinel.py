from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, field

from ids_platform.streaming.runtime.query import safe_tag


@dataclass
class InputSentinelWatcher:
    bootstrap_servers: str
    topic: str
    run_tag: str
    poll_timeout_sec: float = 0.5
    seen: threading.Event = field(default_factory=threading.Event, init=False)
    _stop: threading.Event = field(default_factory=threading.Event, init=False)
    _thread: threading.Thread | None = field(default=None, init=False)
    _error: Exception | None = field(default=None, init=False)

    def start(self) -> "InputSentinelWatcher":
        self._thread = threading.Thread(
            target=self._run,
            name=f"input-sentinel-watcher-{safe_tag(self.run_tag) or 'runtime'}",
            daemon=True,
        )
        self._thread.start()
        return self

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=max(float(self.poll_timeout_sec) * 2.0, 1.0))

    def error(self) -> Exception | None:
        return self._error

    def _run(self) -> None:
        consumer = None
        try:
            from confluent_kafka import Consumer

            consumer = Consumer(
                {
                    "bootstrap.servers": self.bootstrap_servers,
                    "group.id": (
                        f"runtime-sentinel-{safe_tag(self.run_tag)}-"
                        f"{int(time.time() * 1000)}"
                    ),
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": False,
                }
            )
            consumer.subscribe([self.topic])
            while not self._stop.is_set() and not self.seen.is_set():
                message = consumer.poll(float(self.poll_timeout_sec))
                if message is None:
                    continue
                if message.error():
                    raise RuntimeError(str(message.error()))
                if self._is_input_sentinel(message.value()):
                    self.seen.set()
                    return
        except Exception as exc:
            self._error = exc
        finally:
            if consumer is not None:
                consumer.close()

    def _is_input_sentinel(self, value) -> bool:
        try:
            if isinstance(value, bytes):
                value = value.decode("utf-8")
            payload = json.loads(str(value))
        except (TypeError, ValueError, UnicodeDecodeError):
            return False

        expected_run_tag = str(self.run_tag or "").strip()
        if expected_run_tag and str(payload.get("replay_run_tag") or "") != expected_run_tag:
            return False

        return str(payload.get("control_type") or "") == "input_sentinel"
