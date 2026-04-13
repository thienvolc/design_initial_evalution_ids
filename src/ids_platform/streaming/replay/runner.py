from __future__ import annotations

import json
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone

from confluent_kafka import Producer

from ids_platform.common.config import load_yaml_mapping
from ids_platform.common.paths import resolve_project_path
from ids_platform.streaming.replay.config import parse_rate_schedule
from ids_platform.streaming.replay.service import (
    apply_lateness,
    apply_reorder,
    ensure_event_time_column,
    expected_elapsed_for_rows,
    iter_replay_batches,
    load_trace_batches,
    normalize_flow_id,
    to_json_value,
)


@dataclass(frozen=True)
class ReplayJobOptions:
    config_path: str = "configs/streaming/online.yaml"
    bootstrap_servers: str | None = None
    topic: str | None = None
    run_tag: str = ""

    input_parquet: str | None = None
    trace_order_column: str = "event_time"
    force_sort_input: bool = False
    max_rows: int = 0

    batch_size: int = 5000
    rows_per_sec: float = 0.0
    rate_schedule: str = ""

    reorder_window_size: int = 0
    late_event_ratio: float = 0.0
    late_event_max_sec: float = 0.0
    random_seed: int = 42


def _log_replay_event(event: str, **fields) -> None:
    parts = [f"[replay] event={event}"]
    for key, value in fields.items():
        if value is None:
            continue
        parts.append(f"{key}={value}")
    print(" ".join(parts), flush=True)


def run_replay_job(options: ReplayJobOptions) -> int:
    replay_started_at = time.perf_counter()

    # ── configs ─────────────────────────────────────────
    config = load_yaml_mapping(resolve_project_path(options.config_path))
    kafka_config = config.get("kafka") or {}
    paths_config = config.get("paths") or {}


    # ── runtime ─────────────────────────────────────────
    bootstrap_servers = options.bootstrap_servers or str(kafka_config.get("bootstrap_servers", "localhost:9092"))
    topic = options.topic or str(kafka_config.get("input_topic", "ids.raw.flows"))
    input_parquet = resolve_project_path(
        options.input_parquet or str(paths_config.get("input_parquet", "data/gold/splits/test.parquet"))
    )


    # ── replay manifest ─────────────────────────────────
    manifest_path = resolve_project_path(
        str(paths_config.get("feature_manifest", "artifacts/offline/preprocessing/feature_manifest.json"))
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    feature_columns = [str(column_name) for column_name in manifest.get("feature_columns", [])]
    if not feature_columns:
        raise ValueError("feature_columns missing in feature manifest")

    replay_columns = [
        "flow_id",
        "event_time",
        "timestamp",
        "Timestamp",
        "label_binary",
        "label",
        *feature_columns,
    ]


    # ── replay schedule ─────────────────────────────────
    rate_schedule = parse_rate_schedule(
        options.rate_schedule,
        error_message="rate schedule item must be rps:seconds",
    )
    if rate_schedule and options.rows_per_sec > 0:
        raise ValueError("Use either rows_per_sec or rate_schedule, not both")
    if options.batch_size <= 0:
        raise ValueError("batch_size must be > 0")
    if options.reorder_window_size < 0:
        raise ValueError("reorder_window_size must be >= 0")
    if options.late_event_ratio < 0 or options.late_event_ratio > 1:
        raise ValueError("late_event_ratio must be in [0, 1]")
    if options.late_event_max_sec < 0:
        raise ValueError("late_event_max_sec must be >= 0")
    if options.max_rows < 0:
        raise ValueError("max_rows must be >= 0")

    rng = random.Random(int(options.random_seed))
    record_batches, trace_col = load_trace_batches(
        parquet_path=input_parquet,
        columns=replay_columns,
        batch_size=options.batch_size,
        trace_order_column=options.trace_order_column,
        force_sort_input=bool(options.force_sort_input),
    )
    if not record_batches:
        raise ValueError(f"No readable rows found in parquet source: {input_parquet}")

    _log_replay_event(
        "start",
        run_tag=options.run_tag,
        topic=topic,
        input_parquet=input_parquet,
        trace_order_column=options.trace_order_column,
        trace_col=(trace_col or "none"),
        batch_size=options.batch_size,
        max_rows=options.max_rows,
        rows_per_sec=options.rows_per_sec,
        rate_schedule=(options.rate_schedule or ""),
        force_sort_input=bool(options.force_sort_input),
        reorder_window_size=options.reorder_window_size,
        late_event_ratio=options.late_event_ratio,
        late_event_max_sec=options.late_event_max_sec,
    )


    # ── replay ──────────────────────────────────────────
    producer = Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
            "retries": 3,
        }
    )
    sent_rows = 0

    started = time.perf_counter()

    delivery_failures: list[str] = []

    def on_delivery(error, message) -> None:
        if error is None:
            return
        delivery_failures.append(f"topic={message.topic()} key={message.key()!r} error={error}")

    for chunk_index, chunk in enumerate(iter_replay_batches(record_batches, options.max_rows), start=1):
        if chunk.empty:
            continue

        chunk = ensure_event_time_column(chunk)
        if options.reorder_window_size > 1:
            chunk = apply_reorder(
                chunk,
                window_size=int(options.reorder_window_size),
                seed_base=int(options.random_seed) + chunk_index,
            )
        if options.late_event_ratio > 0 and options.late_event_max_sec > 0:
            chunk = apply_lateness(
                chunk,
                ratio=float(options.late_event_ratio),
                max_sec=float(options.late_event_max_sec),
                rng=rng,
            )

        for row_index, (_, row) in enumerate(chunk.iterrows(), start=sent_rows):
            record = row.copy()
            record["source_ingest_ts"] = datetime.now(timezone.utc).isoformat()
            record["replay_run_tag"] = options.run_tag
            record = normalize_flow_id(record, row_index=row_index)
            producer.produce(
                topic=topic,
                key=str(record["flow_id"]),
                value=to_json_value(record),
                on_delivery=on_delivery,
            )

        remaining_messages = producer.flush()
        if remaining_messages:
            delivery_failures.append(
                f"flush_incomplete topic={topic} chunk_index={chunk_index} remaining_messages={remaining_messages}"
            )
        if delivery_failures:
            failure_details = "\n".join(delivery_failures[:10])
            raise RuntimeError(
                f"Replay delivery failed after publishing attempt for run_tag={options.run_tag}.\n{failure_details}"
            )
        sent_rows += len(chunk)

        if rate_schedule:
            elapsed = time.perf_counter() - started
            expected = expected_elapsed_for_rows(sent_rows, rate_schedule)
            sleep_seconds = expected - elapsed
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
        elif options.rows_per_sec > 0:
            elapsed = time.perf_counter() - started
            expected = sent_rows / options.rows_per_sec
            sleep_seconds = expected - elapsed
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

        if chunk_index == 1 or chunk_index % 10 == 0:
            _log_replay_event(
                "progress",
                run_tag=options.run_tag,
                topic=topic,
                chunk_index=chunk_index,
                sent_rows=sent_rows,
                elapsed_sec=f"{time.perf_counter() - replay_started_at:.2f}",
            )


    # ── stop ───────────────────────────────────────────
    elapsed = max(time.perf_counter() - started, 1e-9)
    schedule_note = ""
    if rate_schedule:
        schedule_note = f" schedule={options.rate_schedule}"
    elif options.rows_per_sec > 0:
        schedule_note = f" rows_per_sec_target={options.rows_per_sec}"

    variant_note = (
        f" reorder_window={options.reorder_window_size} late_ratio={options.late_event_ratio}"
        if options.reorder_window_size > 1 or options.late_event_ratio > 0
        else ""
    )
    _log_replay_event(
        "done",
        run_tag=options.run_tag,
        topic=topic,
        rows=sent_rows,
        elapsed_sec=f"{elapsed:.3f}",
        rps=f"{sent_rows / elapsed:.1f}",
        trace_col=(trace_col or "none"),
        force_sort_input=bool(options.force_sort_input),
        schedule=(options.rate_schedule if rate_schedule else None),
        rows_per_sec_target=(options.rows_per_sec if options.rows_per_sec > 0 else None),
        reorder_window_size=(options.reorder_window_size if options.reorder_window_size > 1 else None),
        late_event_ratio=(options.late_event_ratio if options.late_event_ratio > 0 else None),
    )
    return 0
