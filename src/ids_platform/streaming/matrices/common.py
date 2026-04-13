from __future__ import annotations

import csv
import json
import statistics
import time
import uuid
from pathlib import Path

from confluent_kafka import Consumer, TopicPartition


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
                assignments.append(TopicPartition(topic, requested_partition.partition, 0))
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
) -> bool:
    wait_seconds = max(float(startup_wait_sec), 0.0)
    if wait_seconds <= 0:
        return process.poll() is None

    end_time = time.time() + wait_seconds
    while time.time() < end_time:
        if process.poll() is not None:
            return False
        time.sleep(_poll_timeout_seconds(end_time=end_time, max_poll_seconds=poll_seconds))
    return process.poll() is None


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
    rows.sort(key=lambda item: int(item.get("batch_id", 0)))
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

    rows_per_sec_values: list[float] = []
    batch_wall_values: list[float] = []
    source_p95_values: list[float] = []
    proc_p95_values: list[float] = []
    e2e_p95_values: list[float] = []
    event_lateness_p95_values: list[float] = []
    lag_totals: list[float] = []
    driver_rss_values: list[float] = []
    executor_mem_values: list[float] = []

    last_payload = materialized_rows[-1]

    for payload in materialized_rows:
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
        if late_ratio is not None and row_count > 0:
            late_ratio_weighted_total += late_ratio * row_count

        detection = payload.get("detection") or {}
        labeled_rows_total += _safe_float(detection.get("labeled_rows")) or 0.0
        tp_total += _safe_float(detection.get("tp")) or 0.0
        tn_total += _safe_float(detection.get("tn")) or 0.0
        fp_total += _safe_float(detection.get("fp")) or 0.0
        fn_total += _safe_float(detection.get("fn")) or 0.0

        lag_total = _nested_float(payload, "kafka", "lag_records_total")
        if lag_total is not None:
            lag_totals.append(lag_total)

        driver_rss = _nested_float(payload, "system", "driver_rss_mb")
        if driver_rss is not None:
            driver_rss_values.append(driver_rss)

        executor_mem = _nested_float(payload, "system", "executor_mem_util_avg")
        if executor_mem is not None:
            executor_mem_values.append(executor_mem)

    precision = tp_total / (tp_total + fp_total) if (tp_total + fp_total) > 0 else 0.0
    recall = tp_total / (tp_total + fn_total) if (tp_total + fn_total) > 0 else 0.0
    f1 = (2.0 * precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0
    fpr = fp_total / (fp_total + tn_total) if (fp_total + tn_total) > 0 else 0.0
    fnr = fn_total / (fn_total + tp_total) if (fn_total + tp_total) > 0 else 0.0

    return {
        "rows_total": int(rows_total),
        "rows_per_sec_avg": statistics.fmean(rows_per_sec_values) if rows_per_sec_values else None,
        "batch_wall_ms_avg": statistics.fmean(batch_wall_values) if batch_wall_values else None,
        "source_p95_ms_max": max(source_p95_values) if source_p95_values else None,
        "proc_p95_ms_max": max(proc_p95_values) if proc_p95_values else None,
        "e2e_p95_ms_max": max(e2e_p95_values) if e2e_p95_values else None,
        "event_lateness_p95_ms_max": max(event_lateness_p95_values) if event_lateness_p95_values else None,
        "avg_prediction_score_weighted": (score_weighted_total / rows_total) if rows_total > 0 else None,
        "attack_ratio_weighted": (attack_ratio_weighted_total / rows_total) if rows_total > 0 else None,
        "late_event_ratio_weighted": (late_ratio_weighted_total / rows_total) if rows_total > 0 else None,
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
        "driver_rss_mb_avg": statistics.fmean(driver_rss_values) if driver_rss_values else None,
        "executor_mem_util_avg": statistics.fmean(executor_mem_values) if executor_mem_values else None,
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
