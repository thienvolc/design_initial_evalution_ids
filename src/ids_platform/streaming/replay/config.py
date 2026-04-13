from __future__ import annotations


def parse_rate_schedule(
    raw: str,
    *,
    error_message: str = "rate schedule must be rps:seconds,rps:seconds",
) -> list[tuple[float, float]]:
    text = (raw or "").strip()
    if not text:
        return []

    schedule: list[tuple[float, float]] = []
    for token in text.split(","):
        piece = token.strip()
        if not piece:
            continue
        parts = [item.strip() for item in piece.split(":")]
        if len(parts) != 2:
            raise ValueError(error_message)
        rows_per_second = float(parts[0])
        duration_seconds = float(parts[1])
        if rows_per_second <= 0 or duration_seconds <= 0:
            raise ValueError("rate schedule values must be > 0")
        schedule.append((rows_per_second, duration_seconds))
    return schedule


def schedule_total_seconds(schedule: list[tuple[float, float]]) -> float:
    return sum(duration_seconds for _, duration_seconds in schedule)


def _parse_trigger_interval_seconds(trigger_interval: str) -> float | None:
    text = (trigger_interval or "").strip().lower()
    if not text:
        return None

    parts = text.split()
    if len(parts) != 2:
        return None

    try:
        value = float(parts[0])
    except ValueError:
        return None

    unit = parts[1]
    if value <= 0:
        return None
    if unit in {"s", "sec", "secs", "second", "seconds"}:
        return value
    if unit in {"m", "min", "mins", "minute", "minutes"}:
        return value * 60.0
    return None


def _estimate_stream_capacity_runtime_seconds(
    *,
    max_rows: int,
    max_offsets_per_trigger: int,
    trigger_interval: str,
) -> float | None:
    trigger_seconds = _parse_trigger_interval_seconds(trigger_interval)
    if max_rows <= 0 or max_offsets_per_trigger <= 0 or trigger_seconds is None:
        return None
    rows_per_second = max_offsets_per_trigger / max(trigger_seconds, 1e-9)
    if rows_per_second <= 0:
        return None
    return max_rows / rows_per_second


def estimate_stream_runtime(
    *,
    max_rows: int,
    trace_rows_per_sec: float,
    trace_schedule: list[tuple[float, float]],
    override_seconds: int,
    max_offsets_per_trigger: int = 0,
    trigger_interval: str = "",
    schedule_buffer_seconds: int = 180,
    default_seconds: int = 300,
) -> int:
    if override_seconds > 0:
        return max(int(override_seconds), 1)

    replay_runtime_seconds: float | None = None
    if trace_schedule:
        replay_runtime_seconds = schedule_total_seconds(trace_schedule)
    elif trace_rows_per_sec > 0:
        replay_runtime_seconds = max_rows / trace_rows_per_sec

    stream_runtime_seconds = _estimate_stream_capacity_runtime_seconds(
        max_rows=max_rows,
        max_offsets_per_trigger=max_offsets_per_trigger,
        trigger_interval=trigger_interval,
    )

    candidate_seconds = [value for value in (replay_runtime_seconds, stream_runtime_seconds) if value is not None]
    if candidate_seconds:
        return int(max(candidate_seconds) + schedule_buffer_seconds)
    return default_seconds


def build_replay_command(
    *,
    python_exe: str,
    config: str,
    run_tag: str,
    max_rows: int,
    batch_size: int,
    rows_per_sec: float = 0.0,
    rate_schedule: str = "",
    input_parquet: str | None = None,
    trace_order_column: str | None = None,
    force_sort_input: bool = False,
    reorder_window_size: int | None = None,
    late_event_ratio: float | None = None,
    late_event_max_sec: float | None = None,
    random_seed: int | None = None,
) -> list[str]:
    command = [
        python_exe,
        "scripts/streaming/replay_parquet_to_kafka.py",
        "--config",
        config,
        "--run-tag",
        run_tag,
        "--max-rows",
        str(max_rows),
        "--batch-size",
        str(batch_size),
    ]

    if input_parquet:
        command.extend(["--input-parquet", input_parquet])
    if trace_order_column:
        command.extend(["--trace-order-column", trace_order_column])
    if force_sort_input:
        command.append("--force-sort-input")
    if reorder_window_size is not None:
        command.extend(["--reorder-window-size", str(reorder_window_size)])
    if late_event_ratio is not None:
        command.extend(["--late-event-ratio", str(late_event_ratio)])
    if late_event_max_sec is not None:
        command.extend(["--late-event-max-sec", str(late_event_max_sec)])
    if random_seed is not None:
        command.extend(["--random-seed", str(random_seed)])

    if rate_schedule.strip():
        command.extend(["--rate-schedule", rate_schedule.strip()])
    elif rows_per_sec > 0:
        command.extend(["--rows-per-sec", str(rows_per_sec)])

    return command

