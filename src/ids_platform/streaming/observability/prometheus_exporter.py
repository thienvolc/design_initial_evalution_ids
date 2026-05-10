from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from prometheus_client import Gauge, start_http_server

from ids_platform.streaming.core.config import resolve_kafka_bootstrap_servers


LABELS = ["run_tag", "model_name", "feature_set", "load_profile"]
RUNTIME_METRICS = {
    "ids_runtime_rows": lambda payload: _to_float(payload.get("rows")),
    "ids_runtime_rows_per_sec": lambda payload: _to_float(payload.get("rows_per_sec")),
    "ids_runtime_batch_wall_ms": lambda payload: _to_float(payload.get("batch_wall_ms")),
    "ids_runtime_latency_source_p95_ms": lambda payload: _nested_float(payload, "latency_ms", "source_to_ingest", "p95"),
    "ids_runtime_latency_processing_p95_ms": lambda payload: _nested_float(payload, "latency_ms", "processing", "p95"),
    "ids_runtime_latency_e2e_p95_ms": lambda payload: _nested_float(payload, "latency_ms", "end_to_end", "p95"),
    "ids_runtime_late_event_ratio": lambda payload: _nested_float(payload, "event_time", "late_event_ratio"),
    "ids_runtime_kafka_lag_records_total": lambda payload: _nested_float(payload, "kafka", "lag_records_total"),
    "ids_runtime_kafka_lag_records_max_partition": lambda payload: _nested_float(payload, "kafka", "lag_records_max_partition"),
    "ids_runtime_driver_cpu_percent": lambda payload: _nested_float(payload, "system", "driver_cpu_percent"),
    "ids_runtime_driver_rss_mb": lambda payload: _nested_float(payload, "system", "driver_rss_mb"),
    "ids_runtime_executor_mem_util_avg": lambda payload: _nested_float(payload, "system", "executor_mem_util_avg"),
    "ids_runtime_executor_mem_util_p95": lambda payload: _nested_float(payload, "system", "executor_mem_util_p95"),
    "ids_runtime_executor_count": lambda payload: _nested_float(payload, "system", "executor_count"),
    "ids_runtime_last_batch_id": lambda payload: _to_float(payload.get("batch_id")),
    "ids_runtime_last_metric_ts_epoch_ms": lambda payload: _to_float(payload.get("ts_epoch_ms")),
}


@dataclass(frozen=True)
class PrometheusExporterOptions:
    config_path: Path | None
    bootstrap_servers: str | None = None
    metrics_topic: str | None = None
    port: int = 9108
    refresh_seconds: int = 15
    stale_seconds: int = 300
    offset_reset: str = "latest"
    once: bool = False


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _nested_float(payload: dict[str, Any], *keys: str) -> float | None:
    current: Any = payload
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return _to_float(current)


def _load_connection_from_config(config_path: Path | None) -> tuple[str, str]:
    if config_path is None or not config_path.exists():
        raise FileNotFoundError("A valid streaming config path is required for the runtime metrics exporter.")
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Invalid config payload in {config_path}")
    kafka_cfg = payload.get("kafka") or {}
    if not isinstance(kafka_cfg, dict):
        raise ValueError(f"Missing kafka config in {config_path}")
    bootstrap_servers = resolve_kafka_bootstrap_servers(str(kafka_cfg.get("bootstrap_servers", "")).strip())
    metrics_topic = str(kafka_cfg.get("metrics_topic", "ids.metrics")).strip()
    if not bootstrap_servers:
        raise ValueError(f"Missing kafka.bootstrap_servers in {config_path}")
    if not metrics_topic:
        raise ValueError(f"Missing kafka.metrics_topic in {config_path}")
    return bootstrap_servers, metrics_topic


def _normalize_offset_reset(value: str) -> str:
    text = (value or "").strip().lower()
    if text in {"latest", "earliest"}:
        return text
    return "earliest"


def _build_consumer(bootstrap_servers: str, *, offset_reset: str):
    from confluent_kafka import Consumer

    return Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"ids-runtime-exporter-{uuid.uuid4()}",
            "enable.auto.commit": False,
            "auto.offset.reset": _normalize_offset_reset(offset_reset),
        }
    )


def _drain_messages(consumer, poll_timeout_seconds: float) -> list[dict[str, Any]]:
    deadline = time.monotonic() + max(poll_timeout_seconds, 0.25)
    messages: list[dict[str, Any]] = []
    while time.monotonic() < deadline:
        remaining = max(deadline - time.monotonic(), 0.1)
        message = consumer.poll(min(remaining, 1.0))
        if message is None:
            continue
        if message.error():
            continue
        try:
            payload = json.loads(message.value().decode("utf-8"))
        except Exception:
            continue
        if isinstance(payload, dict):
            messages.append(payload)
    return messages


def _label_values(payload: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(payload.get("run_tag", "na")).strip() or "na",
        str(payload.get("model_name", "na")).strip() or "na",
        str(payload.get("feature_set", "na")).strip() or "na",
        str(payload.get("load_profile", "na")).strip() or "na",
    )


def _payload_last_seen_ts(payload: dict[str, Any], *, fallback_ts: float) -> float:
    metric_ts_epoch_ms = _to_float(payload.get("ts_epoch_ms"))
    if metric_ts_epoch_ms is None:
        return fallback_ts
    metric_ts_seconds = metric_ts_epoch_ms / 1000.0
    if metric_ts_seconds <= 0:
        return fallback_ts
    return metric_ts_seconds


def build_gauges() -> dict[str, Gauge]:
    gauges: dict[str, Gauge] = {}
    for metric_name in RUNTIME_METRICS:
        gauges[metric_name] = Gauge(metric_name, metric_name.replace("_", " "), LABELS)
    gauges["ids_runtime_exporter_up"] = Gauge("ids_runtime_exporter_up", "1 if runtime metrics exporter is healthy")
    gauges["ids_runtime_active_streams"] = Gauge("ids_runtime_active_streams", "Count of active stream label sets")
    gauges["ids_runtime_messages_ingested_total"] = Gauge(
        "ids_runtime_messages_ingested_total",
        "Cumulative runtime telemetry messages consumed by the exporter",
    )
    gauges["ids_runtime_last_refresh_ts_unix"] = Gauge(
        "ids_runtime_last_refresh_ts_unix",
        "Unix timestamp of the last exporter refresh",
    )
    return gauges


def _apply_state_to_gauges(
    *,
    state: dict[tuple[str, str, str, str], dict[str, Any]],
    gauges: dict[str, Gauge],
    stale_seconds: int,
    now_ts: float,
    messages_ingested_total: int,
) -> tuple[int, int]:
    active_streams = 0
    exported_values = 0

    for metric_name, gauge in gauges.items():
        if metric_name.startswith("ids_runtime_") and hasattr(gauge, "clear"):
            try:
                gauge.clear()
            except Exception:
                pass

    for label_values, payload in list(state.items()):
        last_seen_ts = _to_float(payload.get("_last_seen_ts"))
        if last_seen_ts is None or (now_ts - last_seen_ts) > stale_seconds:
            state.pop(label_values, None)
            continue

        active_streams += 1
        for metric_name, value_getter in RUNTIME_METRICS.items():
            value = value_getter(payload)
            if value is None:
                continue
            gauges[metric_name].labels(*label_values).set(value)
            exported_values += 1

    gauges["ids_runtime_exporter_up"].set(1.0)
    gauges["ids_runtime_active_streams"].set(float(active_streams))
    gauges["ids_runtime_messages_ingested_total"].set(float(messages_ingested_total))
    gauges["ids_runtime_last_refresh_ts_unix"].set(now_ts)
    return active_streams, exported_values


def run_prometheus_exporter(options: PrometheusExporterOptions) -> int:
    bootstrap_servers = options.bootstrap_servers
    metrics_topic = options.metrics_topic
    if not bootstrap_servers or not metrics_topic:
        bootstrap_servers, metrics_topic = _load_connection_from_config(options.config_path)

    gauges = build_gauges()
    consumer = _build_consumer(bootstrap_servers, offset_reset=options.offset_reset)
    state: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    messages_ingested_total = 0

    try:
        consumer.subscribe([metrics_topic])

        def refresh_once() -> tuple[int, int]:
            nonlocal messages_ingested_total
            now_ts = time.time()
            messages = _drain_messages(consumer, poll_timeout_seconds=max(options.refresh_seconds, 1))
            for payload in messages:
                label_values = _label_values(payload)
                state[label_values] = {
                    **payload,
                    "_last_seen_ts": _payload_last_seen_ts(payload, fallback_ts=now_ts),
                }
            messages_ingested_total += len(messages)
            return _apply_state_to_gauges(
                state=state,
                gauges=gauges,
                stale_seconds=max(options.stale_seconds, 1),
                now_ts=now_ts,
                messages_ingested_total=messages_ingested_total,
            )

        if options.once:
            active_streams, exported_values = refresh_once()
            print(
                f"runtime exporter: active_streams={active_streams} exported_values={exported_values} "
                f"messages_ingested_total={messages_ingested_total}"
            )
            return 0

        start_http_server(options.port)
        print(f"runtime exporter running at http://127.0.0.1:{options.port}/metrics", flush=True)

        while True:
            active_streams, exported_values = refresh_once()
            print(
                f"runtime exporter refresh: active_streams={active_streams} "
                f"exported_values={exported_values} messages_ingested_total={messages_ingested_total}",
                flush=True,
            )
    finally:
        consumer.close()
