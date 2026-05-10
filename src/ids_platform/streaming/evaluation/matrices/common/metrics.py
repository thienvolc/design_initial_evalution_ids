from __future__ import annotations

import json


def assign_consumer_from_timestamp(
    consumer,
    *,
    topic: str,
    start_timestamp_ms: int,
    topic_partition_cls,
) -> bool:
    try:
        metadata = consumer.list_topics(topic=topic, timeout=5.0)
        topic_metadata = metadata.topics.get(topic)
        if topic_metadata is None:
            return False

        partitions = sorted(topic_metadata.partitions.keys())
        if not partitions:
            return False

        requested = [topic_partition_cls(topic, partition_id, start_timestamp_ms) for partition_id in partitions]
        resolved = consumer.offsets_for_times(requested, timeout=5.0)
        assignments: list = []
        for requested_partition, resolved_partition in zip(requested, resolved):
            offset = resolved_partition.offset
            if offset is None or int(offset) < 0:
                topic_partition = topic_partition_cls(topic, requested_partition.partition)
                low, high = consumer.get_watermark_offsets(topic_partition, timeout=5.0, cached=False)
                fallback_offset = max(int(high), int(low), 0)
                assignments.append(topic_partition_cls(topic, requested_partition.partition, fallback_offset))
            else:
                assignments.append(topic_partition_cls(topic, requested_partition.partition, int(offset)))

        consumer.assign(assignments)
        return True
    except Exception:
        return False


def wait_for_matching_metric(
    *,
    bootstrap_servers: str,
    topic: str,
    run_tag: str,
    timeout_sec: int,
    idle_sec: int,
    group_prefix: str,
    start_timestamp_ms: int | None,
    consumer_cls,
    topic_partition_cls,
    time_module,
    uuid_module,
    log_metrics_event,
    poll_timeout_seconds,
    is_terminal_metric_payload,
) -> dict | None:
    collector_started_at = time_module.time()
    first_match_at: float | None = None
    matched_count = 0
    consumer = consumer_cls(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"{group_prefix}-{uuid_module.uuid4()}",
            "auto.offset.reset": "earliest",
        }
    )
    if start_timestamp_ms is not None and start_timestamp_ms > 0:
        assigned = assign_consumer_from_timestamp(
            consumer,
            topic=topic,
            start_timestamp_ms=int(start_timestamp_ms),
            topic_partition_cls=topic_partition_cls,
        )
        if not assigned:
            consumer.subscribe([topic])
        seek_mode = "timestamp" if assigned else "earliest_fallback"
    else:
        consumer.subscribe([topic])
        seek_mode = "earliest"

    log_metrics_event(
        "wait_start",
        run_tag=run_tag,
        topic=topic,
        timeout_sec=timeout_sec,
        idle_sec=idle_sec,
        start_timestamp_ms=start_timestamp_ms,
        seek_mode=seek_mode,
    )

    end_time = time_module.time() + timeout_sec
    last_seen = None
    found = None
    end_reason = "hard_timeout"
    terminal_deadline: float | None = None
    while time_module.time() < end_time:
        if terminal_deadline is not None and time_module.time() >= terminal_deadline:
            end_reason = "terminal_marker"
            break
        message = consumer.poll(poll_timeout_seconds(end_time=end_time))
        if message is None or message.error():
            if idle_sec > 0 and last_seen is not None and (time_module.time() - last_seen) >= idle_sec:
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
            grace_seconds = float(idle_sec) if idle_sec > 0 else 2.0
            terminal_deadline = time_module.time() + max(min(grace_seconds, 5.0), 0.5)
            last_seen = time_module.time()
            continue

        found = payload
        matched_count += 1
        last_seen = time_module.time()
        if first_match_at is None:
            first_match_at = last_seen
        if idle_sec <= 0:
            end_reason = "first_match"
            break

    consumer.close()
    log_metrics_event(
        "wait_end",
        run_tag=run_tag,
        matched_count=matched_count,
        found=bool(found),
        end_reason=end_reason if found or matched_count > 0 else ("no_matching_metrics" if end_reason == "hard_timeout" else end_reason),
        elapsed_sec=f"{time_module.time() - collector_started_at:.2f}",
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
    group_prefix: str,
    start_timestamp_ms: int | None,
    consumer_cls,
    topic_partition_cls,
    time_module,
    uuid_module,
    log_metrics_event,
    poll_timeout_seconds,
    is_terminal_metric_payload,
    payload_batch_id,
) -> list[dict]:
    collector_started_at = time_module.time()
    first_match_at: float | None = None
    consumer = consumer_cls(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"{group_prefix}-{uuid_module.uuid4()}",
            "auto.offset.reset": "earliest",
        }
    )
    if start_timestamp_ms is not None and start_timestamp_ms > 0:
        assigned = assign_consumer_from_timestamp(
            consumer,
            topic=topic,
            start_timestamp_ms=int(start_timestamp_ms),
            topic_partition_cls=topic_partition_cls,
        )
        if not assigned:
            consumer.subscribe([topic])
        seek_mode = "timestamp" if assigned else "earliest_fallback"
    else:
        consumer.subscribe([topic])
        seek_mode = "earliest"

    log_metrics_event(
        "collect_start",
        run_tag=run_tag,
        topic=topic,
        timeout_sec=timeout_sec,
        idle_sec=idle_sec,
        start_timestamp_ms=start_timestamp_ms,
        seek_mode=seek_mode,
    )

    end_time = time_module.time() + timeout_sec
    last_seen = None
    rows: list[dict] = []
    end_reason = "hard_timeout"
    terminal_deadline: float | None = None
    while time_module.time() < end_time:
        if terminal_deadline is not None and time_module.time() >= terminal_deadline:
            end_reason = "terminal_marker"
            break
        message = consumer.poll(poll_timeout_seconds(end_time=end_time))
        if message is None or message.error():
            if last_seen is not None and (time_module.time() - last_seen) >= idle_sec:
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
            grace_seconds = float(idle_sec) if idle_sec > 0 else 2.0
            terminal_deadline = time_module.time() + max(min(grace_seconds, 5.0), 0.5)
            last_seen = time_module.time()
            continue

        rows.append(payload)
        last_seen = time_module.time()
        if first_match_at is None:
            first_match_at = last_seen
        if len(rows) == 1 or len(rows) % 5 == 0:
            log_metrics_event(
                "collect_progress",
                run_tag=run_tag,
                matched_rows=len(rows),
                latest_batch_id=payload.get("batch_id"),
                elapsed_sec=f"{time_module.time() - collector_started_at:.2f}",
            )

    consumer.close()
    rows.sort(key=payload_batch_id)
    log_metrics_event(
        "collect_end",
        run_tag=run_tag,
        matched_rows=len(rows),
        end_reason=end_reason if rows else ("no_matching_metrics" if end_reason == "hard_timeout" else end_reason),
        elapsed_sec=f"{time_module.time() - collector_started_at:.2f}",
        first_match_delay_sec=(f"{first_match_at - collector_started_at:.2f}" if first_match_at is not None else None),
        latest_batch_id=(rows[-1].get("batch_id") if rows else None),
    )
    return rows
