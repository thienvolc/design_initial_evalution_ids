from __future__ import annotations

from .types import LayerCFaultMatrixOptions


LAYER_C_RUNTIME_BUFFER_SEC = 300
KAFKA_BOOTSTRAP_READY_MIN_TIMEOUT_SEC = 60
KAFKA_BOOTSTRAP_READY_MAX_TIMEOUT_SEC = 120
KAFKA_TOPIC_READY_MIN_TIMEOUT_SEC = 90
KAFKA_TOPIC_READY_MAX_TIMEOUT_SEC = 180
KAFKA_RESTART_POLL_SEC = 2.0
KAFKA_RESTART_REQUIRED_SUCCESSES = 2
KAFKA_LATE_READY_RECHECK_TIMEOUT_SEC = 20
KAFKA_LATE_READY_RECHECK_POLL_SEC = 1.0
KAFKA_SETTLE_DELAY_SEC = 5
KAFKA_POST_READY_SETTLE_TIMEOUT_SEC = 20
KAFKA_POST_READY_SETTLE_POLL_SEC = 1.0
KAFKA_POST_RESTART_GRACE_SEC = 8
SPARK_DRAIN_IDLE_SEC = 15.0
SPARK_DRAIN_MIN_TIMEOUT_SEC = 30
SPARK_DRAIN_MAX_TIMEOUT_SEC = 120
SPARK_DRAIN_POLL_SEC = 0.5
GRACEFUL_STOP_MARKER_TIMEOUT_SEC = 120.0
GRACEFUL_STOP_MARKER_POLL_SEC = 0.5
STREAM_SHUTDOWN_TIMEOUT_SEC = 300
STREAM_SHUTDOWN_POLL_SEC = 0.5
STREAM_SHUTDOWN_SETTLE_SEC = 2.0
STREAM_EXIT_WAIT_TIMEOUT_SEC = 30


def _parse_rate_schedule(rate_schedule: str) -> list[tuple[float, float]]:
    schedule = []
    for raw_step in str(rate_schedule or "").split(","):
        if not raw_step.strip():
            continue
        rows_per_sec, duration_sec = raw_step.split(":", 1)
        schedule.append((float(rows_per_sec), float(duration_sec)))
    return schedule


def validate_runtime_metric(payload: dict | None) -> tuple[bool, str]:
    if not isinstance(payload, dict):
        return False, "metric payload missing"
    event_type = str(payload.get("event_type", "")).strip().lower()
    if event_type and event_type != "batch_metrics":
        return False, f"unexpected metric event_type={event_type}"
    try:
        rows_value = int(payload.get("rows") or 0)
    except Exception:
        rows_value = 0
    if rows_value <= 0:
        return False, "metric rows missing_or_zero"
    return True, ""


def estimate_replay_seconds_from_schedule(*, rows: int, rate_schedule: str) -> float:
    remaining_rows = max(int(rows), 0)
    if remaining_rows <= 0:
        return 0.0
    total_seconds = 0.0
    for rows_per_second, duration_seconds in _parse_rate_schedule(rate_schedule):
        segment_capacity = rows_per_second * duration_seconds
        if remaining_rows <= segment_capacity:
            total_seconds += remaining_rows / rows_per_second
            return total_seconds
        total_seconds += duration_seconds
        remaining_rows -= int(segment_capacity)
    return total_seconds


def estimate_layer_c_stream_runtime_seconds(
    *,
    options: LayerCFaultMatrixOptions,
    scenario: str,
    warmup_rate_schedule: str,
    is_restart: bool = False,
) -> int:
    buffer_seconds = LAYER_C_RUNTIME_BUFFER_SEC
    startup_budget = max(int(options.startup_wait_sec), 0)
    fault_delay_seconds = max(int(options.fault_delay_sec), 0)

    warmup_seconds = 0.0
    if not is_restart:
        if warmup_rate_schedule.strip():
            warmup_seconds = estimate_replay_seconds_from_schedule(
                rows=options.warmup_rows,
                rate_schedule=warmup_rate_schedule,
            )
        elif float(options.warmup_rows_per_sec) > 0:
            warmup_seconds = float(options.warmup_rows) / float(options.warmup_rows_per_sec)

    if scenario == "producer_restart":
        post_fault_seconds = 0.0
        if float(options.post_fault_rows_per_sec) > 0:
            post_fault_seconds = float(options.post_fault_rows) / float(options.post_fault_rows_per_sec)
        post_fault_seconds += max(int(options.producer_restart_pause_sec), 0)
    elif scenario == "network_slowdown":
        slowdown_rps = max(float(options.slowdown_rows_per_sec), 0.0)
        post_fault_seconds = (
            float(options.post_fault_rows) / slowdown_rps if slowdown_rps > 0 else 0.0
        )
    else:
        replay_rps = max(float(options.post_fault_rows_per_sec), 0.0)
        post_fault_seconds = (
            float(options.post_fault_rows) / replay_rps if replay_rps > 0 else 0.0
        )

    estimated_seconds = startup_budget + warmup_seconds + fault_delay_seconds + post_fault_seconds + buffer_seconds
    return max(int(estimated_seconds), int(options.stream_run_seconds), 1)
