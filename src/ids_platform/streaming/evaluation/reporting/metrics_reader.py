from __future__ import annotations

import json
import time
import uuid
from datetime import datetime
from functools import lru_cache
from pathlib import Path

from confluent_kafka import Consumer, TopicPartition

from ids_platform.common.config import load_yaml_mapping
from ids_platform.common.paths import resolve_project_path
from ids_platform.streaming.core.config import resolve_kafka_bootstrap_servers
from ids_platform.streaming.evaluation.matrices.common import is_terminal_metric_payload


def parse_iso_timestamp(value: str) -> datetime | None:
    raw = value.strip()
    if not raw:
        return None
    normalized = raw.replace("Z", "+00:00")
    return datetime.fromisoformat(normalized)


def _poll_timeout_seconds(*, end_time: float, max_poll_seconds: float = 1.0) -> float:
    remaining = end_time - time.time()
    if remaining <= 0:
        return 0.0
    return max(0.05, min(max_poll_seconds, remaining))


def _assign_consumer_from_timestamp(
    consumer: Consumer,
    *,
    topic: str,
    start_timestamp_ms: int,
) -> bool:
    try:
        metadata = consumer.list_topics(topic=topic, timeout=5.0)
        topic_metadata = metadata.topics.get(topic)
        if topic_metadata is None:
            return False

        partitions = sorted(topic_metadata.partitions.keys())
        if not partitions:
            return False

        requested = [TopicPartition(topic, partition_id, start_timestamp_ms) for partition_id in partitions]
        resolved = consumer.offsets_for_times(requested, timeout=5.0)
        assignments: list[TopicPartition] = []
        for requested_partition, resolved_partition in zip(requested, resolved):
            offset = resolved_partition.offset
            if offset is None or int(offset) < 0:
                topic_partition = TopicPartition(topic, requested_partition.partition)
                low, high = consumer.get_watermark_offsets(topic_partition, timeout=5.0, cached=False)
                fallback_offset = max(int(high), int(low), 0)
                assignments.append(TopicPartition(topic, requested_partition.partition, fallback_offset))
            else:
                assignments.append(TopicPartition(topic, requested_partition.partition, int(offset)))

        consumer.assign(assignments)
        return True
    except Exception:
        return False


def read_first_matching_metric(
    *,
    bootstrap_servers: str,
    metrics_topic: str,
    run_tag: str,
    timeout_sec: int = 60,
    after_ts_utc: str = "",
    after_epoch_ms: int = 0,
) -> dict | None:
    threshold_ts = parse_iso_timestamp(after_ts_utc)
    threshold_epoch_ms = int(after_epoch_ms or 0)
    seek_timestamp_ms = threshold_epoch_ms
    if seek_timestamp_ms <= 0 and threshold_ts is not None:
        seek_timestamp_ms = int(threshold_ts.timestamp() * 1000)

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"read-metrics-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
        }
    )
    if seek_timestamp_ms > 0:
        assigned = _assign_consumer_from_timestamp(
            consumer,
            topic=metrics_topic,
            start_timestamp_ms=max(seek_timestamp_ms - 30_000, 0),
        )
        if not assigned:
            consumer.subscribe([metrics_topic])
    else:
        consumer.subscribe([metrics_topic])

    end = time.time() + max(timeout_sec, 1)
    found = None
    try:
        while time.time() < end:
            msg = consumer.poll(_poll_timeout_seconds(end_time=end))
            if msg is None or msg.error():
                continue

            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except Exception:
                continue

            if payload.get("run_tag") != run_tag:
                continue

            if is_terminal_metric_payload(payload):
                continue

            payload_epoch_ms = payload.get("ts_epoch_ms")
            try:
                payload_epoch_ms = int(payload_epoch_ms)
            except Exception:
                payload_epoch_ms = 0

            if threshold_epoch_ms > 0 and payload_epoch_ms > 0 and payload_epoch_ms <= threshold_epoch_ms:
                continue

            payload_ts = parse_iso_timestamp(str(payload.get("ts_utc", "")))
            if threshold_ts is not None and payload_ts is not None and payload_ts <= threshold_ts:
                continue

            found = payload
            break
    finally:
        consumer.close()

    return found


@lru_cache(maxsize=16)
def _load_metrics_connection(config_path: str) -> tuple[str, str]:
    cfg = load_yaml_mapping(resolve_project_path(config_path))
    kafka_cfg = cfg.get("kafka") or {}
    bootstrap_servers = resolve_kafka_bootstrap_servers(
        str(kafka_cfg.get("bootstrap_servers", "kafka:29092"))
    )
    metrics_topic = str(kafka_cfg.get("metrics_topic", "ids.metrics"))
    return bootstrap_servers, metrics_topic


def read_metric_from_config(
    *,
    config_path: str | Path,
    run_tag: str,
    timeout_sec: int = 60,
    after_ts_utc: str = "",
    after_epoch_ms: int = 0,
) -> dict | None:
    bootstrap_servers, metrics_topic = _load_metrics_connection(str(config_path))
    return read_first_matching_metric(
        bootstrap_servers=bootstrap_servers,
        metrics_topic=metrics_topic,
        run_tag=run_tag,
        timeout_sec=timeout_sec,
        after_ts_utc=after_ts_utc,
        after_epoch_ms=after_epoch_ms,
    )
