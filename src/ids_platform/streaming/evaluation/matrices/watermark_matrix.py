from __future__ import annotations

import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from ids_platform.common.config import load_yaml_mapping
from ids_platform.common.paths import resolve_project_path
from ids_platform.common.subprocess import (
    run_command_or_raise,
    start_background_process,
    stop_background_process,
)
from ids_platform.streaming.evaluation.matrices.common import (
    annotate_sut_debug_summary,
    collect_matching_metrics,
    runtime_log_output_path,
    wait_for_process_startup,
    summarize_runtime_metrics,
    write_metrics_timeseries,
    write_summary_rows,
)
from ids_platform.streaming.evaluation.matrices.throughput.result_helpers import (
    materialize_sut_summary_row,
    persist_summary_rows,
)
from ids_platform.streaming.replay.config import (
    build_replay_command,
    estimate_stream_runtime,
)
from ids_platform.streaming.evaluation.matrices.throughput.matrix_support import (
    current_python_executable,
    parse_trace_and_warmup_schedules,
    reset_kafka_topics,
    validate_schedule_mode,
)
from ids_platform.streaming.evaluation.matrices.throughput.stream_support import (
    build_structured_stream_command,
    run_available_now_sequence,
    run_available_now_warmup_sequence,
    run_trace_stream_sequence,
    run_trace_warmup_sequence,
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
    trace_stream_run_seconds: int
    warmup_stream_run_seconds: int
    trace_startup_wait_sec: int
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


def _append_runtime_command_output(
    *,
    log_path: str | Path,
    command: list[str],
    stdout_text: str = "",
    stderr_text: str = "",
    status: str,
) -> None:
    path = Path(log_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(
            f"\n[{datetime.now(timezone.utc).isoformat(timespec='seconds')}] "
            f"status={status} command={' '.join(command)}\n"
        )
        if stdout_text:
            handle.write("STDOUT:\n")
            handle.write(stdout_text)
            if not stdout_text.endswith("\n"):
                handle.write("\n")
        if stderr_text:
            handle.write("STDERR:\n")
            handle.write(stderr_text)
            if not stderr_text.endswith("\n"):
                handle.write("\n")


def _run_command_with_runtime_log(
    *,
    command: list[str],
    runtime_log_path: str | Path,
) -> None:
    try:
        result = run_command_or_raise(command)
    except Exception as exc:
        _append_runtime_command_output(
            log_path=runtime_log_path,
            command=command,
            stderr_text=str(exc),
            status="error",
        )
        raise

    _append_runtime_command_output(
        log_path=runtime_log_path,
        command=command,
        stdout_text=str(getattr(result, "stdout", "") or ""),
        stderr_text=str(getattr(result, "stderr", "") or ""),
        status="ok",
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


def run(options: WatermarkMatrixOptions) -> int:

    delays = [int(x) for x in options.watermark_delays]
    cfg = load_yaml_mapping(resolve_project_path(options.config))
    kafka_cfg = cfg.get("kafka") or {}
    runtime_cfg = cfg.get("runtime") or {}
    spark_cfg = (runtime_cfg.get("spark") or {})
    bootstrap_servers = str(kafka_cfg.get("bootstrap_servers", "kafka:29092"))
    input_topic = str(kafka_cfg.get("input_topic", "ids.raw.flows"))
    output_topic = str(kafka_cfg.get("output_topic", "ids.predictions.binary"))
    metrics_topic = str(kafka_cfg.get("metrics_topic", "ids.metrics"))
    benchmark_topic_names = [input_topic, output_topic, metrics_topic]
    max_offsets_per_trigger = int(kafka_cfg.get("max_offsets_per_trigger", 20000) or 20000)
    trigger_interval = str(spark_cfg.get("trigger_interval", "10 seconds") or "10 seconds")
    python_exe = current_python_executable()
    rows = []

    if options.trace_rate_schedule and options.trace_rows_per_sec > 0:
        raise ValueError("Use either --trace-rows-per-sec or --trace-rate-schedule, not both")
    if options.warmup_rate_schedule and options.warmup_rows_per_sec > 0:
        raise ValueError("Use either --warmup-rows-per-sec or --warmup-rate-schedule, not both")
    trace_schedule, warmup_schedule = parse_trace_and_warmup_schedules(
        trace_rate_schedule=options.trace_rate_schedule,
        warmup_rate_schedule=options.warmup_rate_schedule,
        trace_error_message="trace rate schedule must be rps:seconds,rps:seconds",
        warmup_error_message="trace rate schedule must be rps:seconds,rps:seconds",
    )
    trace_mode = validate_schedule_mode(
        trace_schedule=trace_schedule,
        trace_rows_per_sec=options.trace_rows_per_sec,
        warmup_schedule=warmup_schedule,
        warmup_rows_per_sec=options.warmup_rows_per_sec,
    )

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
        _log_phase(
            "topic_reset_start",
            run_tag=run_tag,
            topics=",".join(benchmark_topic_names),
        )
        reset_kafka_topics(
            bootstrap_servers=bootstrap_servers,
            topic_names=benchmark_topic_names,
        )
        _log_phase(
            "topic_reset_done",
            run_tag=run_tag,
            topics=",".join(benchmark_topic_names),
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
            if trace_mode:
                warmup_stream_seconds = estimate_stream_runtime(
                    max_rows=options.warmup_rows,
                    trace_rows_per_sec=warmup_rps,
                    trace_schedule=warmup_schedule,
                    override_seconds=options.warmup_stream_run_seconds,
                    max_offsets_per_trigger=max_offsets_per_trigger,
                    trigger_interval=trigger_interval,
                )
                warmup_stream_cmd = build_structured_stream_command(
                    python_exe=python_exe,
                    config=options.config,
                    model=options.model,
                    feature_set=options.feature_set,
                    run_tag=warmup_tag,
                    load_profile=f"watermark_delay_{delay}s",
                    input_run_tag=warmup_tag,
                    starting_offsets="latest",
                    stop_on_input_sentinel=True,
                    run_seconds=warmup_stream_seconds,
                    reset_checkpoint=True,
                )
                warmup_stream_cmd.extend(["--override-watermark-delay-sec", str(delay)])
                if options.drop_late_events:
                    warmup_stream_cmd.append("--drop-late-events")
                run_trace_warmup_sequence(
                    run_tag=run_tag,
                    warmup_tag=warmup_tag,
                    warmup_stream_cmd=warmup_stream_cmd,
                    warmup_replay_cmd=warmup_replay_cmd,
                    warmup_stream_seconds=warmup_stream_seconds,
                    startup_wait_sec=options.trace_startup_wait_sec,
                    runtime_log_output_path_fn=runtime_log_output_path,
                    log_phase_fn=_log_phase,
                    start_background_process_fn=start_background_process,
                    wait_for_process_startup_fn=wait_for_process_startup,
                    run_command_or_raise_fn=run_command_or_raise,
                    stop_background_process_fn=stop_background_process,
                    subprocess_module=subprocess,
                )
            else:
                warmup_stream_cmd = build_structured_stream_command(
                    python_exe=python_exe,
                    config=options.config,
                    model=options.model,
                    feature_set=options.feature_set,
                    run_tag=warmup_tag,
                    load_profile=f"watermark_delay_{delay}s",
                    input_run_tag=warmup_tag,
                    reset_checkpoint=True,
                    available_now=True,
                )
                warmup_stream_cmd.extend(["--override-watermark-delay-sec", str(delay)])
                if options.drop_late_events:
                    warmup_stream_cmd.append("--drop-late-events")
                run_available_now_warmup_sequence(
                    run_tag=run_tag,
                    warmup_tag=warmup_tag,
                    warmup_replay_cmd=warmup_replay_cmd,
                    warmup_stream_cmd=warmup_stream_cmd,
                    log_phase_fn=_log_phase,
                    run_command_or_raise_fn=run_command_or_raise,
                    runtime_log_output_path_fn=runtime_log_output_path,
                )

        if trace_mode:
            stream_seconds = estimate_stream_runtime(
                max_rows=options.max_rows,
                trace_rows_per_sec=(options.trace_rows_per_sec if options.trace_rows_per_sec > 0 else options.rows_per_sec),
                trace_schedule=trace_schedule,
                override_seconds=options.trace_stream_run_seconds,
                max_offsets_per_trigger=max_offsets_per_trigger,
                trigger_interval=trigger_interval,
            )
            stream_cmd = build_structured_stream_command(
                python_exe=python_exe,
                config=options.config,
                model=options.model,
                feature_set=options.feature_set,
                run_tag=run_tag,
                load_profile=f"watermark_delay_{delay}s",
                input_run_tag=run_tag,
                starting_offsets="latest",
                stop_on_input_sentinel=True,
                run_seconds=stream_seconds,
                reset_checkpoint=True,
            )
            stream_cmd.extend(["--override-watermark-delay-sec", str(delay)])
            if options.drop_late_events:
                stream_cmd.append("--drop-late-events")
            run_trace_stream_sequence(
                run_tag=run_tag,
                stream_cmd=stream_cmd,
                replay_cmd=replay_cmd,
                stream_seconds=stream_seconds,
                startup_wait_sec=options.trace_startup_wait_sec,
                runtime_log_output_path_fn=runtime_log_output_path,
                log_phase_fn=_log_phase,
                start_background_process_fn=start_background_process,
                wait_for_process_startup_fn=wait_for_process_startup,
                run_command_or_raise_fn=run_command_or_raise,
                stop_background_process_fn=stop_background_process,
                subprocess_module=subprocess,
            )
        else:
            stream_cmd = build_structured_stream_command(
                python_exe=python_exe,
                config=options.config,
                model=options.model,
                feature_set=options.feature_set,
                run_tag=run_tag,
                load_profile=f"watermark_delay_{delay}s",
                input_run_tag=run_tag,
                reset_checkpoint=True,
                available_now=True,
            )
            stream_cmd.extend(["--override-watermark-delay-sec", str(delay)])
            if options.drop_late_events:
                stream_cmd.append("--drop-late-events")
            run_available_now_sequence(
                run_tag=run_tag,
                replay_cmd=replay_cmd,
                stream_cmd=stream_cmd,
                log_phase_fn=_log_phase,
                run_command_or_raise_fn=run_command_or_raise,
                runtime_log_output_path_fn=runtime_log_output_path,
            )

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
        row, timeseries_path = materialize_sut_summary_row(
            run_tag=run_tag,
            metrics_rows=metrics_rows,
            build_legacy_row_fn=lambda materialized_metrics_rows: _to_row(run_tag, delay, materialized_metrics_rows),
            write_metrics_timeseries_fn=write_metrics_timeseries,
            annotate_sut_debug_summary_fn=annotate_sut_debug_summary,
        )
        if timeseries_path is not None:
            _log_phase("timeseries_write_done", run_tag=run_tag, path=timeseries_path)
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

    persist_summary_rows(
        summary_csv=options.summary_csv,
        rows=rows,
        resolve_project_path_fn=resolve_project_path,
        write_summary_rows_fn=write_summary_rows,
        label="watermark",
    )
    return 0


