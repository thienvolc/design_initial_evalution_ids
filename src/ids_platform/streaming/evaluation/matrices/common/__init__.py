from __future__ import annotations

import csv
import time
import uuid
from pathlib import Path

from confluent_kafka import Consumer, TopicPartition

from ids_platform.common.paths import PROJECT_ROOT, resolve_project_path
from ids_platform.streaming.evaluation.matrices.common import metrics, outputs, waiters
from ids_platform.streaming.evaluation.matrices.common.outputs import TIMESERIES_FIELDNAMES


def _log_metrics_event(event: str, **fields) -> None:
    parts = [f"[metrics] event={event}"]
    for key, value in fields.items():
        if value is None:
            continue
        parts.append(f"{key}={value}")
    print(" ".join(parts), flush=True)


def _poll_timeout_seconds(*, end_time: float, max_poll_seconds: float = 1.0) -> float:
    return waiters.poll_timeout_seconds(end_time=end_time, max_poll_seconds=max_poll_seconds, time_module=time)


def is_terminal_metric_payload(payload: dict) -> bool:
    if not isinstance(payload, dict):
        return False
    event_type = str(payload.get("event_type", "")).strip().lower()
    if event_type == "run_completed":
        return True
    return bool(payload.get("final"))


def _assign_consumer_from_timestamp(
    consumer: Consumer,
    *,
    topic: str,
    start_timestamp_ms: int,
) -> bool:
    return metrics.assign_consumer_from_timestamp(
        consumer,
        topic=topic,
        start_timestamp_ms=start_timestamp_ms,
        topic_partition_cls=TopicPartition,
    )


def wait_for_process_startup(
    process,
    *,
    startup_wait_sec: int,
    poll_seconds: float = 0.25,
    ready_log_path: str = "",
    ready_pattern: str = "",
    ready_log_start_offset: int | None = None,
    require_ready_marker: bool = False,
) -> bool:
    return waiters.wait_for_process_startup(
        process,
        startup_wait_sec=startup_wait_sec,
        poll_seconds=poll_seconds,
        ready_log_path=ready_log_path,
        ready_pattern=ready_pattern,
        ready_log_start_offset=ready_log_start_offset,
        require_ready_marker=require_ready_marker,
        time_module=time,
    )


def describe_process_startup_state(
    process,
    *,
    log_path: str = "",
    ready_pattern: str = "",
    max_lines: int = 8,
) -> str:
    return waiters.describe_process_startup_state(
        process,
        log_path=log_path,
        ready_pattern=ready_pattern,
        max_lines=max_lines,
    )


def wait_for_log_patterns(
    *,
    process,
    log_path: str,
    patterns: list[str],
    timeout_sec: float,
    poll_seconds: float = 0.25,
    log_start_offset: int | None = None,
) -> str:
    return waiters.wait_for_log_patterns(
        process=process,
        log_path=log_path,
        patterns=patterns,
        timeout_sec=timeout_sec,
        poll_seconds=poll_seconds,
        log_start_offset=log_start_offset,
        time_module=time,
    )


def wait_for_log_quiescence(
    *,
    process,
    log_path: str,
    idle_sec: float,
    timeout_sec: float,
    poll_seconds: float = 0.5,
) -> bool:
    return waiters.wait_for_log_quiescence(
        process=process,
        log_path=log_path,
        idle_sec=idle_sec,
        timeout_sec=timeout_sec,
        poll_seconds=poll_seconds,
        time_module=time,
    )


def wait_for_matching_metric(
    *,
    bootstrap_servers: str,
    topic: str,
    run_tag: str,
    timeout_sec: int,
    idle_sec: int = 0,
    group_prefix: str = "streaming-metrics",
    start_timestamp_ms: int | None = None,
) -> dict | None:
    return metrics.wait_for_matching_metric(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        run_tag=run_tag,
        timeout_sec=timeout_sec,
        idle_sec=idle_sec,
        group_prefix=group_prefix,
        start_timestamp_ms=start_timestamp_ms,
        consumer_cls=Consumer,
        topic_partition_cls=TopicPartition,
        time_module=time,
        uuid_module=uuid,
        log_metrics_event=_log_metrics_event,
        poll_timeout_seconds=_poll_timeout_seconds,
        is_terminal_metric_payload=is_terminal_metric_payload,
    )


def collect_matching_metrics(
    *,
    bootstrap_servers: str,
    topic: str,
    run_tag: str,
    timeout_sec: int,
    idle_sec: int,
    group_prefix: str = "streaming-metrics",
    start_timestamp_ms: int | None = None,
) -> list[dict]:
    return metrics.collect_matching_metrics(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        run_tag=run_tag,
        timeout_sec=timeout_sec,
        idle_sec=idle_sec,
        group_prefix=group_prefix,
        start_timestamp_ms=start_timestamp_ms,
        consumer_cls=Consumer,
        topic_partition_cls=TopicPartition,
        time_module=time,
        uuid_module=uuid,
        log_metrics_event=_log_metrics_event,
        poll_timeout_seconds=_poll_timeout_seconds,
        is_terminal_metric_payload=is_terminal_metric_payload,
        payload_batch_id=_payload_batch_id,
    )


def _safe_float(value) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _nested_float(payload: dict, *keys: str) -> float | None:
    current = payload
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return _safe_float(current)


def _payload_batch_id(payload: dict) -> int:
    batch_id = _safe_float(payload.get("batch_id"))
    return int(batch_id) if batch_id is not None else 0


def _compute_f1_score(precision: float | None, recall: float | None) -> float | None:
    if precision is None or recall is None:
        return None
    denominator = precision + recall
    if denominator <= 0:
        return 0.0
    return float((2.0 * precision * recall) / denominator)


def flatten_metrics_payload(payload: dict) -> dict:
    return outputs.flatten_metrics_payload(payload, safe_float=_safe_float)


def _sanitize_run_tag(run_tag: str) -> str:
    return outputs.sanitize_run_tag(run_tag)


def runtime_log_output_path(
    *,
    run_tag: str,
    output_dir: str = "logs/streaming/runtime",
) -> Path:
    return outputs.runtime_log_output_path(
        run_tag=run_tag,
        output_dir=output_dir,
        resolve_project_path=resolve_project_path,
        project_root=PROJECT_ROOT,
    )


def timeseries_output_path(
    *,
    run_tag: str,
    output_dir: str = "artifacts/streaming/metrics_timeseries",
) -> Path:
    return outputs.timeseries_output_path(
        run_tag=run_tag,
        output_dir=output_dir,
        resolve_project_path=resolve_project_path,
        project_root=PROJECT_ROOT,
    )


def write_metrics_timeseries(
    metrics_rows: list[dict],
    *,
    run_tag: str,
    output_dir: str = "artifacts/streaming/metrics_timeseries",
) -> Path | None:
    return outputs.write_metrics_timeseries(
        metrics_rows,
        run_tag=run_tag,
        output_dir=output_dir,
        is_terminal_metric_payload=is_terminal_metric_payload,
        payload_batch_id=_payload_batch_id,
        flatten_metrics_payload=flatten_metrics_payload,
        timeseries_output_path=timeseries_output_path,
    )


def summarize_runtime_metrics(metrics_rows: list[dict]) -> dict:
    return outputs.summarize_runtime_metrics(
        metrics_rows,
        is_terminal_metric_payload=is_terminal_metric_payload,
        safe_float=_safe_float,
        nested_float=_nested_float,
        compute_f1_score=_compute_f1_score,
    )


def annotate_sut_debug_summary(row: dict) -> dict:
    return outputs.annotate_sut_debug_summary(row)


def write_summary_rows(path: Path, rows: list[dict]) -> None:
    outputs.write_summary_rows(path, rows)
