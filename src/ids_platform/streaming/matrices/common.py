from __future__ import annotations

import csv
import json
import re
import statistics
import time
import uuid
from pathlib import Path

from confluent_kafka import Consumer, TopicPartition

from ids_platform.common.paths import PROJECT_ROOT, resolve_project_path


def _log_metrics_event(event: str, **fields) -> None:
    parts = [f"[metrics] event={event}"]
    for key, value in fields.items():
        if value is None:
            continue
        parts.append(f"{key}={value}")
    print(" ".join(parts), flush=True)


def _poll_timeout_seconds(*, end_time: float, max_poll_seconds: float = 1.0) -> float:
    remaining = end_time - time.time()
    if remaining <= 0:
        return 0.0
    return max(0.05, min(max_poll_seconds, remaining))


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
                # When no message exists at/after the requested timestamp, wait from the
                # current end of the partition instead of rewinding to offset 0.
                assignments.append(TopicPartition(topic, requested_partition.partition, fallback_offset))
            else:
                assignments.append(TopicPartition(topic, requested_partition.partition, int(offset)))

        consumer.assign(assignments)
        return True
    except Exception:
        return False


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
    wait_seconds = max(float(startup_wait_sec), 0.0)
    if wait_seconds <= 0:
        return process.poll() is None

    def _ready_marker_seen() -> bool:
        normalized_path = str(ready_log_path or "").strip()
        normalized_pattern = str(ready_pattern or "").strip()
        if not normalized_path or not normalized_pattern:
            return False
        try:
            start_offset = ready_log_start_offset
            if start_offset is None:
                start_offset = int(getattr(process, "ids_log_start_offset", 0) or 0)
            content = Path(normalized_path).read_bytes()
            if start_offset > 0:
                content = content[max(int(start_offset), 0):]
            return normalized_pattern.encode("utf-8") in content
        except Exception:
            return False

    end_time = time.time() + wait_seconds
    while time.time() < end_time:
        if process.poll() is not None:
            return False
        if _ready_marker_seen():
            return True
        time.sleep(_poll_timeout_seconds(end_time=end_time, max_poll_seconds=poll_seconds))
    if require_ready_marker:
        return process.poll() is None and _ready_marker_seen()
    return process.poll() is None


def describe_process_startup_state(
    process,
    *,
    log_path: str = "",
    ready_pattern: str = "",
    max_lines: int = 8,
) -> str:
    parts: list[str] = []
    try:
        exit_code = process.poll()
    except Exception:
        exit_code = None
    parts.append(f"process_alive={exit_code is None}")
    if exit_code is not None:
        parts.append(f"exit_code={exit_code}")

    normalized_pattern = str(ready_pattern or "").strip()
    normalized_path = str(log_path or "").strip()
    if normalized_pattern and normalized_path:
        try:
            start_offset = int(getattr(process, "ids_log_start_offset", 0) or 0)
            content = Path(normalized_path).read_text(encoding="utf-8", errors="replace")
            if start_offset > 0:
                content = content[start_offset:]
            parts.append(f"ready_marker_seen={normalized_pattern in content}")
            tail_lines = [line.strip() for line in content.splitlines() if line.strip()]
            if tail_lines:
                rendered_tail = " | ".join(tail_lines[-max(int(max_lines), 1):])
                parts.append(f"log_tail={rendered_tail}")
        except Exception:
            parts.append("log_tail_unavailable=true")
    elif normalized_path:
        try:
            content = Path(normalized_path).read_text(encoding="utf-8", errors="replace")
            tail_lines = [line.strip() for line in content.splitlines() if line.strip()]
            if tail_lines:
                rendered_tail = " | ".join(tail_lines[-max(int(max_lines), 1):])
                parts.append(f"log_tail={rendered_tail}")
        except Exception:
            parts.append("log_tail_unavailable=true")
    return "; ".join(parts)


def wait_for_log_patterns(
    *,
    process,
    log_path: str,
    patterns: list[str],
    timeout_sec: float,
    poll_seconds: float = 0.25,
    log_start_offset: int | None = None,
) -> str:
    normalized_path = str(log_path or "").strip()
    normalized_patterns = [str(pattern).strip() for pattern in patterns if str(pattern).strip()]
    if not normalized_path or not normalized_patterns:
        return ""

    path = Path(normalized_path)
    start_offset = log_start_offset
    if start_offset is None:
        start_offset = int(getattr(process, "ids_log_start_offset", 0) or 0)

    encoded_patterns = [(pattern, pattern.encode("utf-8")) for pattern in normalized_patterns]
    end_time = time.time() + max(float(timeout_sec), 0.0)
    while time.time() < end_time:
        if process is not None and process.poll() is not None:
            return ""
        try:
            content = path.read_bytes()
        except OSError:
            content = b""
        if start_offset > 0:
            content = content[max(int(start_offset), 0):]
        for pattern_text, pattern_bytes in encoded_patterns:
            if pattern_bytes in content:
                return pattern_text
        time.sleep(_poll_timeout_seconds(end_time=end_time, max_poll_seconds=poll_seconds))
    return ""


def wait_for_log_quiescence(
    *,
    process,
    log_path: str,
    idle_sec: float,
    timeout_sec: float,
    poll_seconds: float = 0.5,
) -> bool:
    normalized_path = str(log_path or "").strip()
    if not normalized_path:
        return False

    path = Path(normalized_path)
    end_time = time.time() + max(float(timeout_sec), 0.0)
    last_size = None
    last_change_at = time.time()

    while time.time() < end_time:
        if process is not None and process.poll() is not None:
            return False
        try:
            current_size = path.stat().st_size
        except OSError:
            current_size = -1
        if last_size is None or current_size != last_size:
            last_size = current_size
            last_change_at = time.time()
        elif (time.time() - last_change_at) >= max(float(idle_sec), 0.0):
            return True
        time.sleep(_poll_timeout_seconds(end_time=end_time, max_poll_seconds=poll_seconds))
    return False


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
    collector_started_at = time.time()
    first_match_at: float | None = None
    matched_count = 0
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"{group_prefix}-{uuid.uuid4()}",
            # Matrix collectors often subscribe after the run already emitted metrics.
            # Read from the topic start for this ephemeral consumer and filter by run_tag.
            "auto.offset.reset": "earliest",
        }
    )
    if start_timestamp_ms is not None and start_timestamp_ms > 0:
        assigned = _assign_consumer_from_timestamp(
            consumer,
            topic=topic,
            start_timestamp_ms=int(start_timestamp_ms),
        )
        if not assigned:
            consumer.subscribe([topic])
        seek_mode = "timestamp" if assigned else "earliest_fallback"
    else:
        consumer.subscribe([topic])
        seek_mode = "earliest"

    _log_metrics_event(
        "wait_start",
        run_tag=run_tag,
        topic=topic,
        timeout_sec=timeout_sec,
        idle_sec=idle_sec,
        start_timestamp_ms=start_timestamp_ms,
        seek_mode=seek_mode,
    )

    end_time = time.time() + timeout_sec
    last_seen = None
    found = None
    end_reason = "hard_timeout"
    while time.time() < end_time:
        message = consumer.poll(_poll_timeout_seconds(end_time=end_time))
        if message is None or message.error():
            if idle_sec > 0 and last_seen is not None and (time.time() - last_seen) >= idle_sec:
                end_reason = "idle_timeout"
                break
            continue

        try:
            payload = json.loads(message.value().decode("utf-8"))
        except Exception:
            continue

        if payload.get("run_tag") != run_tag:
            continue

        if is_terminal_metric_payload(payload):
            end_reason = "terminal_marker"
            break

        found = payload
        matched_count += 1
        last_seen = time.time()
        if first_match_at is None:
            first_match_at = last_seen
        if idle_sec <= 0:
            end_reason = "first_match"
            break

    consumer.close()
    _log_metrics_event(
        "wait_end",
        run_tag=run_tag,
        matched_count=matched_count,
        found=bool(found),
        end_reason=end_reason if found or matched_count > 0 else ("no_matching_metrics" if end_reason == "hard_timeout" else end_reason),
        elapsed_sec=f"{time.time() - collector_started_at:.2f}",
        first_match_delay_sec=(f"{first_match_at - collector_started_at:.2f}" if first_match_at is not None else None),
    )
    return found


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
    collector_started_at = time.time()
    first_match_at: float | None = None
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"{group_prefix}-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
        }
    )
    if start_timestamp_ms is not None and start_timestamp_ms > 0:
        assigned = _assign_consumer_from_timestamp(
            consumer,
            topic=topic,
            start_timestamp_ms=int(start_timestamp_ms),
        )
        if not assigned:
            consumer.subscribe([topic])
        seek_mode = "timestamp" if assigned else "earliest_fallback"
    else:
        consumer.subscribe([topic])
        seek_mode = "earliest"

    _log_metrics_event(
        "collect_start",
        run_tag=run_tag,
        topic=topic,
        timeout_sec=timeout_sec,
        idle_sec=idle_sec,
        start_timestamp_ms=start_timestamp_ms,
        seek_mode=seek_mode,
    )

    end_time = time.time() + timeout_sec
    last_seen = None
    rows: list[dict] = []
    end_reason = "hard_timeout"
    while time.time() < end_time:
        message = consumer.poll(_poll_timeout_seconds(end_time=end_time))
        if message is None or message.error():
            if last_seen is not None and (time.time() - last_seen) >= idle_sec:
                end_reason = "idle_timeout"
                break
            continue

        try:
            payload = json.loads(message.value().decode("utf-8"))
        except Exception:
            continue

        if payload.get("run_tag") != run_tag:
            continue

        if is_terminal_metric_payload(payload):
            end_reason = "terminal_marker"
            break

        rows.append(payload)
        last_seen = time.time()
        if first_match_at is None:
            first_match_at = last_seen
        if len(rows) == 1 or len(rows) % 5 == 0:
            _log_metrics_event(
                "collect_progress",
                run_tag=run_tag,
                matched_rows=len(rows),
                latest_batch_id=payload.get("batch_id"),
                elapsed_sec=f"{time.time() - collector_started_at:.2f}",
            )

    consumer.close()
    rows.sort(key=_payload_batch_id)
    _log_metrics_event(
        "collect_end",
        run_tag=run_tag,
        matched_rows=len(rows),
        end_reason=end_reason if rows else ("no_matching_metrics" if end_reason == "hard_timeout" else end_reason),
        elapsed_sec=f"{time.time() - collector_started_at:.2f}",
        first_match_delay_sec=(f"{first_match_at - collector_started_at:.2f}" if first_match_at is not None else None),
        latest_batch_id=(rows[-1].get("batch_id") if rows else None),
    )
    return rows


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


TIMESERIES_FIELDNAMES = [
    "ts_utc",
    "ts_epoch_ms",
    "batch_id",
    "run_tag",
    "load_profile",
    "model_name",
    "feature_set",
    "metric_warnings",
    "rows",
    "rows_per_sec",
    "batch_wall_ms",
    "source_p95_ms",
    "ingest_to_emit_p95_ms",
    "source_to_emit_p95_ms",
    "proc_p95_ms",
    "e2e_p95_ms",
    "event_lateness_p95_ms",
    "late_event_ratio",
    "late_event_ratio_interpretable",
    "freshness_signal_ratio",
    "watermark_delay_sec",
    "kafka_lag_records_total",
    "kafka_lag_records_max_partition",
    "driver_cpu_percent",
    "driver_rss_mb",
    "executor_mem_util_avg",
    "executor_mem_util_p95",
    "executor_count",
    "labeled_rows",
    "tp",
    "tn",
    "fp",
    "fn",
    "precision",
    "recall",
    "f1",
    "fpr",
    "fnr",
]


def flatten_metrics_payload(payload: dict) -> dict:
    detection = payload.get("detection") or {}
    latency = payload.get("latency_ms") or {}
    source_latency = latency.get("source_to_ingest") or {}
    ingest_to_emit_latency = latency.get("ingest_to_emit") or {}
    source_to_emit_latency = latency.get("source_to_emit") or {}
    processing_latency = latency.get("processing") or {}
    end_to_end_latency = latency.get("end_to_end") or {}
    event_lateness_latency = latency.get("event_lateness") or {}
    event_time = payload.get("event_time") or {}
    kafka = payload.get("kafka") or {}
    system = payload.get("system") or {}
    system_knobs = payload.get("system_knobs") or {}
    watermark_delay_sec = system_knobs.get("watermark_delay_sec", "")
    late_event_ratio = event_time.get("late_event_ratio", "")
    interpretable_late_ratio = late_event_ratio
    freshness_signal_ratio = ""
    watermark_delay_num = _safe_float(watermark_delay_sec)
    if watermark_delay_num == 0:
        interpretable_late_ratio = ""
        freshness_signal_ratio = late_event_ratio

    row = {
        "ts_utc": payload.get("ts_utc", ""),
        "ts_epoch_ms": payload.get("ts_epoch_ms", ""),
        "batch_id": payload.get("batch_id", ""),
        "run_tag": payload.get("run_tag", ""),
        "load_profile": payload.get("load_profile", ""),
        "model_name": payload.get("model_name", ""),
        "feature_set": payload.get("feature_set", ""),
        "metric_warnings": "; ".join(str(item).strip() for item in (payload.get("metric_warnings") or []) if str(item).strip()),
        "rows": payload.get("rows", ""),
        "rows_per_sec": payload.get("rows_per_sec", ""),
        "batch_wall_ms": payload.get("batch_wall_ms", ""),
        "source_p95_ms": source_latency.get("p95", ""),
        "ingest_to_emit_p95_ms": ingest_to_emit_latency.get("p95", processing_latency.get("p95", "")),
        "source_to_emit_p95_ms": source_to_emit_latency.get("p95", end_to_end_latency.get("p95", "")),
        "proc_p95_ms": processing_latency.get("p95", ""),
        "e2e_p95_ms": end_to_end_latency.get("p95", ""),
        "event_lateness_p95_ms": event_lateness_latency.get("p95", ""),
        "late_event_ratio": late_event_ratio,
        "late_event_ratio_interpretable": interpretable_late_ratio,
        "freshness_signal_ratio": freshness_signal_ratio,
        "watermark_delay_sec": watermark_delay_sec,
        "kafka_lag_records_total": kafka.get("lag_records_total", ""),
        "kafka_lag_records_max_partition": kafka.get("lag_records_max_partition", ""),
        "driver_cpu_percent": system.get("driver_cpu_percent", ""),
        "driver_rss_mb": system.get("driver_rss_mb", ""),
        "executor_mem_util_avg": system.get("executor_mem_util_avg", ""),
        "executor_mem_util_p95": system.get("executor_mem_util_p95", ""),
        "executor_count": system.get("executor_count", ""),
        "labeled_rows": detection.get("labeled_rows", ""),
        "tp": detection.get("tp", ""),
        "tn": detection.get("tn", ""),
        "fp": detection.get("fp", ""),
        "fn": detection.get("fn", ""),
        "precision": detection.get("precision", ""),
        "recall": detection.get("recall", ""),
        "f1": detection.get("f1", ""),
        "fpr": detection.get("fpr", ""),
        "fnr": detection.get("fnr", ""),
    }
    return row


def _sanitize_run_tag(run_tag: str) -> str:
    text = str(run_tag).strip()
    if not text:
        return "unknown_run"
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", text)


def runtime_log_output_path(
    *,
    run_tag: str,
    output_dir: str = "logs/streaming/runtime",
) -> Path:
    resolved_dir = resolve_project_path(output_dir, PROJECT_ROOT)
    return resolved_dir / f"{_sanitize_run_tag(run_tag)}.log"


def timeseries_output_path(
    *,
    run_tag: str,
    output_dir: str = "artifacts/streaming/metrics_timeseries",
) -> Path:
    resolved_dir = resolve_project_path(output_dir, PROJECT_ROOT)
    return resolved_dir / f"{_sanitize_run_tag(run_tag)}.csv"


def write_metrics_timeseries(
    metrics_rows: list[dict],
    *,
    run_tag: str,
    output_dir: str = "artifacts/streaming/metrics_timeseries",
) -> Path | None:
    materialized_rows = [payload for payload in metrics_rows if not is_terminal_metric_payload(payload)]
    if not materialized_rows:
        return None

    path = timeseries_output_path(run_tag=run_tag, output_dir=output_dir)
    path.parent.mkdir(parents=True, exist_ok=True)

    ordered_rows = sorted(
        materialized_rows,
        key=_payload_batch_id,
    )
    flattened_rows = [flatten_metrics_payload(payload) for payload in ordered_rows]

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=TIMESERIES_FIELDNAMES)
        writer.writeheader()
        writer.writerows(flattened_rows)

    return path


def summarize_runtime_metrics(metrics_rows: list[dict]) -> dict:
    materialized_rows = [payload for payload in metrics_rows if not is_terminal_metric_payload(payload)]
    if not materialized_rows:
        return {}

    rows_total = 0.0
    labeled_rows_total = 0.0
    tp_total = 0.0
    tn_total = 0.0
    fp_total = 0.0
    fn_total = 0.0
    score_weighted_total = 0.0
    attack_ratio_weighted_total = 0.0
    late_ratio_weighted_total = 0.0
    late_ratio_interpretable_weighted_total = 0.0
    freshness_signal_weighted_total = 0.0
    late_ratio_interpretable_rows_total = 0.0
    freshness_signal_rows_total = 0.0

    rows_per_sec_values: list[float] = []
    batch_wall_values: list[float] = []
    source_p95_values: list[float] = []
    ingest_to_emit_p95_values: list[float] = []
    source_to_emit_p95_values: list[float] = []
    proc_p95_values: list[float] = []
    e2e_p95_values: list[float] = []
    event_lateness_p95_values: list[float] = []
    lag_totals: list[float] = []
    driver_cpu_values: list[float] = []
    driver_rss_values: list[float] = []
    executor_mem_values: list[float] = []
    executor_mem_p95_values: list[float] = []
    executor_count_values: list[float] = []
    metric_warnings: set[str] = set()

    last_payload = materialized_rows[-1]

    for payload in materialized_rows:
        for warning in payload.get("metric_warnings") or []:
            warning_text = str(warning).strip()
            if warning_text:
                metric_warnings.add(warning_text)
        row_count = _safe_float(payload.get("rows")) or 0.0
        rows_total += row_count

        rows_per_sec = _safe_float(payload.get("rows_per_sec"))
        if rows_per_sec is not None:
            rows_per_sec_values.append(rows_per_sec)

        batch_wall_ms = _safe_float(payload.get("batch_wall_ms"))
        if batch_wall_ms is not None:
            batch_wall_values.append(batch_wall_ms)

        source_p95 = _nested_float(payload, "latency_ms", "source_to_ingest", "p95")
        if source_p95 is not None:
            source_p95_values.append(source_p95)

        ingest_to_emit_p95 = _nested_float(payload, "latency_ms", "ingest_to_emit", "p95")
        if ingest_to_emit_p95 is not None:
            ingest_to_emit_p95_values.append(ingest_to_emit_p95)

        source_to_emit_p95 = _nested_float(payload, "latency_ms", "source_to_emit", "p95")
        if source_to_emit_p95 is not None:
            source_to_emit_p95_values.append(source_to_emit_p95)

        proc_p95 = _nested_float(payload, "latency_ms", "processing", "p95")
        if proc_p95 is not None:
            proc_p95_values.append(proc_p95)

        e2e_p95 = _nested_float(payload, "latency_ms", "end_to_end", "p95")
        if e2e_p95 is not None:
            e2e_p95_values.append(e2e_p95)

        event_lateness_p95 = _nested_float(payload, "latency_ms", "event_lateness", "p95")
        if event_lateness_p95 is not None:
            event_lateness_p95_values.append(event_lateness_p95)

        avg_score = _safe_float(payload.get("avg_prediction_score"))
        if avg_score is not None and row_count > 0:
            score_weighted_total += avg_score * row_count

        attack_ratio = _safe_float(payload.get("attack_ratio"))
        if attack_ratio is not None and row_count > 0:
            attack_ratio_weighted_total += attack_ratio * row_count

        late_ratio = _nested_float(payload, "event_time", "late_event_ratio")
        watermark_delay_sec = _nested_float(payload, "system_knobs", "watermark_delay_sec")
        if late_ratio is not None and row_count > 0:
            late_ratio_weighted_total += late_ratio * row_count
            if watermark_delay_sec == 0:
                freshness_signal_weighted_total += late_ratio * row_count
                freshness_signal_rows_total += row_count
            else:
                late_ratio_interpretable_weighted_total += late_ratio * row_count
                late_ratio_interpretable_rows_total += row_count

        detection = payload.get("detection") or {}
        labeled_rows_total += _safe_float(detection.get("labeled_rows")) or 0.0
        tp_total += _safe_float(detection.get("tp")) or 0.0
        tn_total += _safe_float(detection.get("tn")) or 0.0
        fp_total += _safe_float(detection.get("fp")) or 0.0
        fn_total += _safe_float(detection.get("fn")) or 0.0

        lag_total = _nested_float(payload, "kafka", "lag_records_total")
        if lag_total is not None:
            lag_totals.append(lag_total)

        driver_cpu = _nested_float(payload, "system", "driver_cpu_percent")
        if driver_cpu is not None:
            driver_cpu_values.append(driver_cpu)

        driver_rss = _nested_float(payload, "system", "driver_rss_mb")
        if driver_rss is not None:
            driver_rss_values.append(driver_rss)

        executor_mem = _nested_float(payload, "system", "executor_mem_util_avg")
        if executor_mem is not None:
            executor_mem_values.append(executor_mem)

        executor_mem_p95 = _nested_float(payload, "system", "executor_mem_util_p95")
        if executor_mem_p95 is not None:
            executor_mem_p95_values.append(executor_mem_p95)

        executor_count = _nested_float(payload, "system", "executor_count")
        if executor_count is not None:
            executor_count_values.append(executor_count)

    precision = (tp_total / (tp_total + fp_total)) if (tp_total + fp_total) > 0 else None
    recall = (tp_total / (tp_total + fn_total)) if (tp_total + fn_total) > 0 else None
    f1 = _compute_f1_score(precision, recall)
    fpr = (fp_total / (fp_total + tn_total)) if (fp_total + tn_total) > 0 else None
    fnr = (fn_total / (fn_total + tp_total)) if (fn_total + tp_total) > 0 else None

    return {
        "rows_total": int(rows_total),
        "rows_per_sec_avg": statistics.fmean(rows_per_sec_values) if rows_per_sec_values else None,
        "batch_wall_ms_avg": statistics.fmean(batch_wall_values) if batch_wall_values else None,
        "source_p95_ms_max": max(source_p95_values) if source_p95_values else None,
        "ingest_to_emit_p95_ms_max": max(ingest_to_emit_p95_values) if ingest_to_emit_p95_values else None,
        "source_to_emit_p95_ms_max": max(source_to_emit_p95_values) if source_to_emit_p95_values else None,
        "proc_p95_ms_max": max(proc_p95_values) if proc_p95_values else None,
        "e2e_p95_ms_max": max(e2e_p95_values) if e2e_p95_values else None,
        "event_lateness_p95_ms_max": max(event_lateness_p95_values) if event_lateness_p95_values else None,
        "avg_prediction_score_weighted": (score_weighted_total / rows_total) if rows_total > 0 else None,
        "attack_ratio_weighted": (attack_ratio_weighted_total / rows_total) if rows_total > 0 else None,
        "late_event_ratio_weighted": (late_ratio_weighted_total / rows_total) if rows_total > 0 else None,
        "late_event_ratio_interpretable_weighted": (
            late_ratio_interpretable_weighted_total / late_ratio_interpretable_rows_total
        ) if late_ratio_interpretable_rows_total > 0 else None,
        "freshness_signal_ratio_weighted": (
            freshness_signal_weighted_total / freshness_signal_rows_total
        ) if freshness_signal_rows_total > 0 else None,
        "labeled_rows_total": int(labeled_rows_total),
        "tp_total": int(tp_total),
        "tn_total": int(tn_total),
        "fp_total": int(fp_total),
        "fn_total": int(fn_total),
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "fpr": fpr,
        "fnr": fnr,
        "kafka_lag_records_max": max(lag_totals) if lag_totals else None,
        "driver_cpu_percent_avg": statistics.fmean(driver_cpu_values) if driver_cpu_values else None,
        "driver_rss_mb_avg": statistics.fmean(driver_rss_values) if driver_rss_values else None,
        "executor_mem_util_avg": statistics.fmean(executor_mem_values) if executor_mem_values else None,
        "executor_mem_util_p95_avg": statistics.fmean(executor_mem_p95_values) if executor_mem_p95_values else None,
        "executor_count_max": max(executor_count_values) if executor_count_values else None,
        "metric_warnings": sorted(metric_warnings),
        "watermark_delay_sec": _nested_float(last_payload, "system_knobs", "watermark_delay_sec"),
        "last_payload": last_payload,
        "batch_count": len(materialized_rows),
    }


def annotate_sut_debug_summary(row: dict) -> dict:
    annotated = dict(row)
    annotated.setdefault("evaluation_source", "sut_debug_metrics")
    annotated.setdefault("boundary_mode", "sut_metrics_only")
    annotated.setdefault("official_source_of_truth", "matrix_summaries_derived_from_ids_metrics")
    annotated.setdefault("legacy_metrics_status", str(annotated.get("status", "")).strip().lower() or "missing")
    annotated.setdefault("metrics_comparison_status", "not_applicable")
    annotated.setdefault("comparison_notes", "official summary currently derived from SUT-emitted debug metrics")
    return annotated


def write_summary_rows(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return

    fieldnames: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row.keys():
            if key in seen:
                continue
            seen.add(key)
            fieldnames.append(key)

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
