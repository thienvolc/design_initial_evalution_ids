from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from ids_platform.streaming.replay.config import ReplayRuntimeConfig
from ids_platform.streaming.replay.serializer import replay_record_to_json


@dataclass(frozen=True, slots=True)
class ReplayRecordBuilder:
    run_tag: str
    phase: str = "measure"

    def build_replay_record(self, row: pd.Series, *, row_index: int) -> pd.Series:
        ingest_time = time.time()

        record = row.copy()
        record["source_ingest_ts"] = datetime.fromtimestamp(
            ingest_time,
            tz=timezone.utc,
        ).isoformat()
        record["source_ingest_epoch_ms"] = int(ingest_time * 1000)
        record["replay_run_tag"] = self.run_tag
        record["replay_row_index"] = row_index
        record["benchmark_phase"] = self.phase
        return record

    def build_input_sentinel_record(self) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()
        epoch_ms = int(now.timestamp() * 1000)
        run_tag = self.run_tag

        return {
            "flow_id": f"{run_tag}__input_sentinel",
            "replay_run_tag": run_tag,
            "benchmark_phase": "control",
            "event_time": now_iso,
            "timestamp": now_iso,
            "source_ingest_ts": now_iso,
            "source_ingest_epoch_ms": epoch_ms,
            "label_binary": None,
            "label": None,
            "is_control_record": 1,
            "control_type": "input_sentinel",
        }


def create_producer(bootstrap_servers: str):
    from confluent_kafka import Producer

    config = {
        "bootstrap.servers": bootstrap_servers,
        "enable.idempotence": True,
        "acks": "all",
        "retries": 3,
    }

    return Producer(config)


class ReplayPublisher:
    def __init__(self, config: ReplayRuntimeConfig) -> None:
        self.producer = create_producer(config.bootstrap_servers)
        self.topic = config.topic
        self.run_tag = config.run_tag
        self.delivery_failures: list[str] = []

    def publish_record(self, record: pd.Series) -> None:
        self._produce(
            key=str(record["flow_id"]),
            value=replay_record_to_json(record),
        )

    def publish_sentinel(self, sentinel_record: dict[str, Any]):
        self._produce(
            key=str(sentinel_record["flow_id"]),
            value=replay_record_to_json(pd.Series(sentinel_record)),
        )

        self.flush_or_raise("sentinel publish")

    def flush_or_raise(self, phase: str, *, chunk_index: int | None = None) -> None:
        remaining_messages = self.producer.flush()

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

    def _on_delivery(self, error, message) -> None:
        if error is None:
            return

        self.delivery_failures.append(
            f"topic={message.topic()} key={message.key()!r} error={error}"
        )

    def _produce(self, *, key: str, value: str) -> None:
        self.producer.produce(
            topic=self.topic,
            key=key,
            value=value,
            on_delivery=self._on_delivery,
        )
