from __future__ import annotations

import time
from dataclasses import dataclass

from ids_platform.streaming.replay.config import ReplayRuntimeConfig
from ids_platform.streaming.replay.encoder import EncodedReplayRecord


@dataclass(frozen=True, slots=True)
class ReplayPublisherStats:
    produced_count: int
    buffer_stall_count: int
    buffer_stall_sec: float
    flush_sec: float


def create_producer(bootstrap_servers: str):
    from confluent_kafka import Producer

    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
            "retries": 3,
            "linger.ms": 0,
            "queue.buffering.max.messages": 1_000_000,
        }
    )


class ReplayPublisher:
    def __init__(self, config: ReplayRuntimeConfig) -> None:
        self.producer = create_producer(config.bootstrap_servers)
        self.topic = config.topic
        self.run_tag = config.run_tag
        self.delivery_failures: list[str] = []
        self.produced_count = 0
        self.buffer_stall_count = 0
        self.buffer_stall_sec = 0.0
        self.flush_sec = 0.0

    def publish(self, record: EncodedReplayRecord) -> None:
        self._produce(key=record.key, value=record.value)
        self.produced_count += 1

    def publish_sentinel(self, record: EncodedReplayRecord):
        self.publish(record)
        self.flush_or_raise("sentinel publish")

    def poll(self, timeout: float = 0.0) -> None:
        self.producer.poll(float(timeout))

    def flush_or_raise(self, phase: str, *, chunk_index: int | None = None) -> None:
        started_at = time.perf_counter()
        remaining_messages = self.producer.flush()
        self.flush_sec += time.perf_counter() - started_at

        if remaining_messages:
            chunk_detail = f" chunk_index={chunk_index}" if chunk_index is not None else ""
            self.delivery_failures.append(
                f"flush_incomplete topic={self.topic}{chunk_detail} "
                f"remaining_messages={remaining_messages}"
            )

        if self.delivery_failures:
            details = "\n".join(self.delivery_failures[:10])
            raise RuntimeError(
                f"Replay delivery failed after {phase} "
                f"for run_tag={self.run_tag}.\n{details}"
            )

    def stats(self) -> ReplayPublisherStats:
        return ReplayPublisherStats(
            produced_count=int(self.produced_count),
            buffer_stall_count=int(self.buffer_stall_count),
            buffer_stall_sec=float(self.buffer_stall_sec),
            flush_sec=float(self.flush_sec),
        )

    def _on_delivery(self, error, message) -> None:
        if error is None:
            return

        self.delivery_failures.append(
            f"topic={message.topic()} key={message.key()!r} error={error}"
        )

    def _produce(self, *, key: str, value: str) -> None:
        while True:
            try:
                self.producer.produce(
                    topic=self.topic,
                    key=key,
                    value=value,
                    on_delivery=self._on_delivery,
                )
                return
            except BufferError:
                self.buffer_stall_count += 1
                started_at = time.perf_counter()
                self.poll(0.1)
                self.buffer_stall_sec += time.perf_counter() - started_at
