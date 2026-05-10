from __future__ import annotations

import sys
import time
from dataclasses import dataclass

from ids_platform.common.config import load_yaml_mapping
from ids_platform.common.paths import resolve_project_path
from ids_platform.streaming.replay.config import parse_rate_schedule


@dataclass(frozen=True)
class ThroughputRunContext:
    run_started_at: float
    run_start_timestamp_ms: int
    run_tag: str


def resolve_streaming_metrics_context(config_path: str) -> tuple[str, str, dict, dict, str]:
    cfg = load_yaml_mapping(resolve_project_path(config_path))
    kafka_cfg = cfg.get("kafka") or {}
    runtime_cfg = cfg.get("runtime") or {}
    spark_cfg = runtime_cfg.get("spark") or {}
    bootstrap_servers = str(kafka_cfg.get("bootstrap_servers", "kafka:29092"))
    metrics_topic = str(kafka_cfg.get("metrics_topic", "ids.metrics"))
    return bootstrap_servers, metrics_topic, cfg, runtime_cfg, spark_cfg


def current_python_executable() -> str:
    return sys.executable


def build_run_context(*, run_prefix: str, tag_parts: list[str]) -> ThroughputRunContext:
    run_started_at = time.time()
    run_start_timestamp_ms = int(time.time() * 1000)
    normalized_parts = [str(part).strip() for part in tag_parts if str(part).strip()]
    base = "_".join(normalized_parts)
    run_tag = f"{run_prefix}_{base}_{run_start_timestamp_ms}"
    return ThroughputRunContext(
        run_started_at=run_started_at,
        run_start_timestamp_ms=run_start_timestamp_ms,
        run_tag=run_tag,
    )


def log_layer_a_run_start(
    *,
    log_phase_fn,
    run_tag: str,
    repeat_index: int,
    run_index: int,
    model: str,
    feature_set: str,
    profile: dict,
) -> None:
    log_phase_fn(
        "run_start",
        run_tag=run_tag,
        repeat_index=repeat_index,
        run_index=run_index,
        model=model,
        feature_set=feature_set,
        profile=profile["name"],
        max_offsets=profile["max_offsets_per_trigger"],
        shuffle=profile["shuffle_partitions"],
        trigger=(profile.get("trigger_interval") or "config_default"),
    )


def log_layer_b_run_start(
    *,
    log_phase_fn,
    run_tag: str,
    repeat_index: int,
    run_index: int,
    model: str,
    feature_set: str,
    trace_mode: bool,
    max_rows: int,
    batch_size: int,
) -> None:
    log_phase_fn(
        "run_start",
        run_tag=run_tag,
        repeat_index=repeat_index,
        run_index=run_index,
        model=model,
        feature_set=feature_set,
        trace_mode=trace_mode,
        max_rows=max_rows,
        batch_size=batch_size,
    )


def parse_trace_and_warmup_schedules(
    *,
    trace_rate_schedule: str,
    warmup_rate_schedule: str,
    trace_error_message: str = "trace rate schedule must be rps:seconds,rps:seconds",
    warmup_error_message: str = "trace rate schedule must be rps:seconds,rps:seconds",
):
    trace_schedule = parse_rate_schedule(trace_rate_schedule, error_message=trace_error_message)
    warmup_schedule = parse_rate_schedule(warmup_rate_schedule, error_message=warmup_error_message)
    return trace_schedule, warmup_schedule


def validate_schedule_mode(
    *,
    trace_schedule,
    trace_rows_per_sec: float,
    warmup_schedule,
    warmup_rows_per_sec: float,
) -> bool:
    trace_mode = bool(trace_schedule) or trace_rows_per_sec > 0
    if trace_schedule and trace_rows_per_sec > 0:
        raise ValueError("Use either --trace-rows-per-sec or --trace-rate-schedule, not both")
    if warmup_schedule and warmup_rows_per_sec > 0:
        raise ValueError("Use either --warmup-rows-per-sec or --warmup-rate-schedule, not both")
    return trace_mode


def collect_metrics_with_logging(
    *,
    run_tag: str,
    timeout_sec: int,
    idle_sec: int,
    run_started_at: float,
    run_start_timestamp_ms: int,
    bootstrap_servers: str,
    metrics_topic: str,
    collect_metrics_fn,
    log_phase_fn,
) -> list[dict]:
    log_phase_fn(
        "metrics_collect_start",
        run_tag=run_tag,
        timeout_sec=timeout_sec,
        idle_sec=idle_sec,
    )
    metrics_rows = collect_metrics_fn(
        bootstrap_servers=bootstrap_servers,
        topic=metrics_topic,
        run_tag=run_tag,
        timeout_sec=timeout_sec,
        idle_sec=idle_sec,
        start_timestamp_ms=max(run_start_timestamp_ms - 30_000, 0),
    )
    log_phase_fn(
        "metrics_collect_done",
        run_tag=run_tag,
        metrics_rows=len(metrics_rows),
        elapsed_sec=f"{time.time() - run_started_at:.2f}",
    )
    return metrics_rows


def reset_kafka_topics(
    *,
    bootstrap_servers: str,
    topic_names: list[str],
    timeout_sec: float = 60.0,
    poll_sec: float = 1.0,
) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic

    topic_names = [str(name).strip() for name in topic_names if str(name).strip()]
    if not topic_names:
        return

    admin = AdminClient({"bootstrap.servers": bootstrap_servers})

    def _topics() -> set[str]:
        return set((admin.list_topics(timeout=10.0).topics or {}).keys())

    existing = [topic_name for topic_name in topic_names if topic_name in _topics()]
    if existing:
        for topic_name, future in admin.delete_topics(existing).items():
            try:
                future.result()
            except Exception as exc:
                if "UnknownTopicOrPart" not in str(exc):
                    raise RuntimeError(f"delete topic failed: {topic_name}: {exc}") from exc
        deadline = time.time() + timeout_sec
        while time.time() < deadline and any(topic_name in _topics() for topic_name in existing):
            time.sleep(poll_sec)
        remaining = [topic_name for topic_name in existing if topic_name in _topics()]
        if remaining:
            raise RuntimeError(f"timed out deleting topics: {', '.join(remaining)}")

    pending = list(topic_names)
    deadline = time.time() + timeout_sec
    while pending and time.time() < deadline:
        create_futures = admin.create_topics(
            [NewTopic(topic_name, num_partitions=1, replication_factor=1) for topic_name in pending]
        )
        next_pending: list[str] = []
        for topic_name, future in create_futures.items():
            try:
                future.result()
            except Exception as exc:
                message = str(exc)
                if "TopicAlreadyExists" in message:
                    continue
                if "marked for deletion" in message:
                    next_pending.append(topic_name)
                    continue
                raise RuntimeError(f"create topic failed: {topic_name}: {exc}") from exc
        if not next_pending:
            break
        time.sleep(poll_sec)
        pending = next_pending

    deadline = time.time() + timeout_sec
    while time.time() < deadline and not all(topic_name in _topics() for topic_name in topic_names):
        time.sleep(poll_sec)
    missing = [topic_name for topic_name in topic_names if topic_name not in _topics()]
    if missing:
        raise RuntimeError(f"timed out creating topics: {', '.join(missing)}")
