from __future__ import annotations

import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from ids_platform.common.config import load_yaml_mapping
from ids_platform.common.paths import resolve_project_path
from ids_platform.common.subprocess import run_command_or_raise
from ids_platform.streaming.matrices.common import (
    annotate_sut_debug_summary,
    collect_matching_metrics,
    summarize_runtime_metrics,
    write_metrics_timeseries,
    write_summary_rows,
)
from ids_platform.streaming.replay.config import (
    build_replay_command,
)


@dataclass(frozen=True)
class WatermarkMatrixOptions:
    config: str
    model: str
    feature_set: str
    watermark_delays: tuple[str, ...]
    drop_late_events: bool
    trace_input_parquet: str
    trace_order_column: str
    trace_rows_per_sec: float
    trace_rate_schedule: str
    reorder_window_size: int
    late_event_ratio: float
    late_event_max_sec: float
    random_seed: int
    max_rows: int
    batch_size: int
    rows_per_sec: float
    warmup_rows: int
    warmup_rows_per_sec: float
    warmup_rate_schedule: str
    metrics_timeout_sec: int
    metrics_idle_sec: int
    summary_csv: str


def _log_phase(event: str, **fields) -> None:
    parts = [f"[phase] matrix=watermark event={event}"]
    for key, value in fields.items():
        if value is None:
            continue
        parts.append(f"{key}={value}")
    print(" ".join(parts), flush=True)


def _wait_metrics(
    bootstrap_servers: str,
    topic: str,
    run_tag: str,
    timeout_sec: int,
    idle_sec: int,
    start_timestamp_ms: int | None = None,
) -> list[dict]:
    return collect_matching_metrics(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        run_tag=run_tag,
        timeout_sec=timeout_sec,
        idle_sec=idle_sec,
        group_prefix="watermark-metrics",
        start_timestamp_ms=start_timestamp_ms,
    )


def _to_row(run_tag: str, watermark_delay_sec: int, metrics_rows: list[dict]) -> dict:
    row = {
        "run_tag": run_tag,
        "load_profile": f"watermark_delay_{watermark_delay_sec}s",
        "watermark_delay_sec": watermark_delay_sec,
        "drop_late_events": "",
        "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "rows": "",
        "late_event_ratio": "",
        "late_event_ratio_interpretable": "",
        "freshness_signal_ratio": "",
        "event_lateness_p95_ms": "",
        "source_p95_ms": "",
        "ingest_to_emit_p95_ms": "",
        "source_to_emit_p95_ms": "",
        "proc_p95_ms": "",
        "e2e_p95_ms": "",
        "precision": "",
        "recall": "",
        "f1": "",
        "fpr": "",
        "fnr": "",
        "metric_warnings": "",
        "status": "ok" if metrics_rows else "metrics_missing",
    }
    if not metrics_rows:
        return row

    summary = summarize_runtime_metrics(metrics_rows)
    last_payload = summary.get("last_payload") or {}
    row["drop_late_events"] = bool((last_payload.get("system_knobs") or {}).get("drop_late_events", False))
    row["rows"] = summary.get("rows_total", "")
    row["late_event_ratio"] = summary.get("late_event_ratio_weighted", "")
    row["late_event_ratio_interpretable"] = summary.get("late_event_ratio_interpretable_weighted", "")
    row["freshness_signal_ratio"] = summary.get("freshness_signal_ratio_weighted", "")
    row["event_lateness_p95_ms"] = summary.get("event_lateness_p95_ms_max", "")
    row["source_p95_ms"] = summary.get("source_p95_ms_max", "")
    row["ingest_to_emit_p95_ms"] = summary.get("ingest_to_emit_p95_ms_max", "")
    row["source_to_emit_p95_ms"] = summary.get("source_to_emit_p95_ms_max", "")
    row["proc_p95_ms"] = summary.get("proc_p95_ms_max", "")
    row["e2e_p95_ms"] = summary.get("e2e_p95_ms_max", "")
    row["precision"] = summary.get("precision", "")
    row["recall"] = summary.get("recall", "")
    row["f1"] = summary.get("f1", "")
    row["fpr"] = summary.get("fpr", "")
    row["fnr"] = summary.get("fnr", "")
    row["metric_warnings"] = "; ".join(summary.get("metric_warnings") or [])
    return row


def _write_summary(path: Path, rows: list[dict]) -> None:
    write_summary_rows(path, rows)


def run(options: WatermarkMatrixOptions) -> int:

    delays = [int(x) for x in options.watermark_delays]
    cfg = load_yaml_mapping(resolve_project_path(options.config))
    kafka_cfg = cfg.get("kafka") or {}
    bootstrap_servers = str(kafka_cfg.get("bootstrap_servers", "kafka:29092"))
    metrics_topic = str(kafka_cfg.get("metrics_topic", "ids.metrics"))
    python_exe = sys.executable
    rows = []

    if options.trace_rate_schedule and options.trace_rows_per_sec > 0:
        raise ValueError("Use either --trace-rows-per-sec or --trace-rate-schedule, not both")
    if options.warmup_rate_schedule and options.warmup_rows_per_sec > 0:
        raise ValueError("Use either --warmup-rows-per-sec or --warmup-rate-schedule, not both")

    for idx, delay in enumerate(delays, start=1):
        run_started_at = time.time()
        run_start_timestamp_ms = int(time.time() * 1000)
        run_tag = f"watermark_{idx:02d}_{delay}s_{run_start_timestamp_ms}"
        _log_phase(
            "run_start",
            run_tag=run_tag,
            run_index=idx,
            watermark_delay_sec=delay,
            drop_late_events=options.drop_late_events,
            model=options.model,
            feature_set=options.feature_set,
        )

        replay_cmd = build_replay_command(
                python_exe=python_exe,
                config=options.config,
                input_parquet=options.trace_input_parquet,
                trace_order_column=options.trace_order_column,
                run_tag=run_tag,
                max_rows=options.max_rows,
                batch_size=options.batch_size,
                reorder_window_size=options.reorder_window_size,
                late_event_ratio=options.late_event_ratio,
                late_event_max_sec=options.late_event_max_sec,
                random_seed=options.random_seed,
                rate_schedule=str(options.trace_rate_schedule),
                rows_per_sec=(
                    options.trace_rows_per_sec
                    if options.trace_rows_per_sec > 0
                    else options.rows_per_sec
                ),
            )

        if options.warmup_rows > 0:
            warmup_tag = f"{run_tag}_warmup"
            _log_phase("warmup_prepare", run_tag=run_tag, warmup_tag=warmup_tag, warmup_rows=options.warmup_rows)
            warmup_rate_schedule = options.warmup_rate_schedule.strip() or str(options.trace_rate_schedule).strip()
            warmup_rps = options.warmup_rows_per_sec
            if warmup_rps <= 0:
                warmup_rps = options.trace_rows_per_sec if options.trace_rows_per_sec > 0 else options.rows_per_sec

            warmup_replay_cmd = build_replay_command(
                python_exe=python_exe,
                config=options.config,
                input_parquet=options.trace_input_parquet,
                trace_order_column=options.trace_order_column,
                run_tag=warmup_tag,
                max_rows=options.warmup_rows,
                batch_size=options.batch_size,
                reorder_window_size=options.reorder_window_size,
                late_event_ratio=options.late_event_ratio,
                late_event_max_sec=options.late_event_max_sec,
                random_seed=options.random_seed,
                rate_schedule=warmup_rate_schedule,
                rows_per_sec=warmup_rps,
            )
            _log_phase("warmup_replay_start", run_tag=run_tag, warmup_tag=warmup_tag)
            run_command_or_raise(warmup_replay_cmd)
            _log_phase("warmup_replay_done", run_tag=run_tag, warmup_tag=warmup_tag)

            warmup_stream_cmd = [
                python_exe,
                "scripts/streaming/run_structured_streaming.py",
                "--config",
                options.config,
                "--model",
                options.model,
                "--feature-set",
                options.feature_set,
                "--run-tag",
                warmup_tag,
                "--load-profile",
                f"watermark_delay_{delay}s",
                "--input-run-tag",
                warmup_tag,
                "--override-watermark-delay-sec",
                str(delay),
                "--reset-checkpoint",
                "--available-now",
            ]
            if options.drop_late_events:
                warmup_stream_cmd.append("--drop-late-events")
            _log_phase("warmup_stream_start", run_tag=run_tag, warmup_tag=warmup_tag, available_now=True)
            run_command_or_raise(warmup_stream_cmd)
            _log_phase("warmup_stream_stop", run_tag=run_tag, warmup_tag=warmup_tag, available_now=True)

        _log_phase("main_replay_start", run_tag=run_tag)
        run_command_or_raise(replay_cmd)
        _log_phase("main_replay_done", run_tag=run_tag)

        stream_cmd = [
            python_exe,
            "scripts/streaming/run_structured_streaming.py",
            "--config",
            options.config,
            "--model",
            options.model,
            "--feature-set",
            options.feature_set,
            "--run-tag",
            run_tag,
            "--load-profile",
            f"watermark_delay_{delay}s",
            "--input-run-tag",
            run_tag,
            "--override-watermark-delay-sec",
            str(delay),
            "--reset-checkpoint",
            "--available-now",
        ]
        if options.drop_late_events:
            stream_cmd.append("--drop-late-events")
        _log_phase("main_stream_start", run_tag=run_tag, available_now=True)
        run_command_or_raise(stream_cmd)
        _log_phase("main_stream_stop", run_tag=run_tag, available_now=True)

        _log_phase(
            "metrics_collect_start",
            run_tag=run_tag,
            timeout_sec=options.metrics_timeout_sec,
            idle_sec=options.metrics_idle_sec,
        )
        metrics_rows = _wait_metrics(
            bootstrap_servers=bootstrap_servers,
            topic=metrics_topic,
            run_tag=run_tag,
            timeout_sec=options.metrics_timeout_sec,
            idle_sec=options.metrics_idle_sec,
            start_timestamp_ms=max(run_start_timestamp_ms - 30_000, 0),
        )
        _log_phase(
            "metrics_collect_done",
            run_tag=run_tag,
            metrics_rows=len(metrics_rows),
            elapsed_sec=f"{time.time() - run_started_at:.2f}",
        )
        timeseries_path = write_metrics_timeseries(metrics_rows, run_tag=run_tag)
        if timeseries_path is not None:
            _log_phase("timeseries_write_done", run_tag=run_tag, path=timeseries_path)
        legacy_row = _to_row(run_tag, delay, metrics_rows)
        row = annotate_sut_debug_summary(legacy_row)
        rows.append(row)
        _log_phase(
            "run_summary",
            run_tag=run_tag,
            status=row["status"],
            rows=row["rows"],
            late_event_ratio=row["late_event_ratio"],
            fpr=row["fpr"],
            fnr=row["fnr"],
            elapsed_sec=f"{time.time() - run_started_at:.2f}",
        )

    out = resolve_project_path(options.summary_csv)
    _write_summary(out, rows)
    print(f"Saved watermark summary: {out}", flush=True)
    return 0


