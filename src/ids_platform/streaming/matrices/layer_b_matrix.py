from __future__ import annotations

import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from ids_platform.common.config import load_yaml_mapping
from ids_platform.common.paths import PROJECT_ROOT, resolve_project_path
from ids_platform.common.subprocess import (
    run_command_or_raise,
    start_background_process,
    stop_background_process,
)
from ids_platform.streaming.matrices.common import (
    annotate_sut_debug_summary,
    collect_matching_metrics,
    runtime_log_output_path,
    wait_for_process_startup,
    summarize_runtime_metrics,
    write_metrics_timeseries,
    write_summary_rows,
)
from ids_platform.streaming.replay.config import (
    build_replay_command,
    estimate_stream_runtime,
    parse_rate_schedule,
)


@dataclass(frozen=True)
class LayerBMatrixOptions:
    config: str
    models: tuple[str, ...]
    feature_sets: tuple[str, ...]
    model_feature_pairs: tuple[str, ...]
    max_rows: int
    batch_size: int
    repeats: int
    run_prefix: str
    metrics_timeout_sec: int
    metrics_idle_sec: int
    trace_input_parquet: str
    trace_order_column: str
    trace_rows_per_sec: float
    trace_rate_schedule: str
    trace_stream_run_seconds: int
    trace_startup_wait_sec: int
    warmup_rows: int
    warmup_rows_per_sec: float
    warmup_rate_schedule: str
    warmup_stream_run_seconds: int
    summary_csv: str


def _log_phase(event: str, **fields) -> None:
    parts = [f"[phase] matrix=layer_b event={event}"]
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
        group_prefix="layer-b-metrics",
        start_timestamp_ms=start_timestamp_ms,
    )


def _flatten_metrics(
    run_tag: str,
    repeat_index: int,
    model: str,
    feature_set: str,
    metrics_rows: list[dict],
) -> dict:
    row = {
        "run_tag": run_tag,
        "repeat_index": repeat_index,
        "model": model,
        "feature_set": feature_set,
        "load_profile": f"{model}:{feature_set}",
        "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "rows": "",
        "source_p95_ms": "",
        "ingest_to_emit_p95_ms": "",
        "source_to_emit_p95_ms": "",
        "proc_p95_ms": "",
        "e2e_p95_ms": "",
        "avg_prediction_score": "",
        "attack_ratio": "",
        "precision": "",
        "recall": "",
        "f1": "",
        "fpr": "",
        "fnr": "",
        "late_event_ratio": "",
        "late_event_ratio_interpretable": "",
        "freshness_signal_ratio": "",
        "watermark_delay_sec": "",
        "kafka_lag_records": "",
        "driver_cpu_percent": "",
        "driver_rss_mb": "",
        "executor_mem_util_avg": "",
        "executor_mem_util_p95": "",
        "executor_count": "",
        "metric_warnings": "",
        "status": "ok" if metrics_rows else "metrics_missing",
    }

    if not metrics_rows:
        return row

    summary = summarize_runtime_metrics(metrics_rows)
    last_payload = summary.get("last_payload") or {}

    row.update(
        {
            "rows": summary.get("rows_total", ""),
            "source_p95_ms": summary.get("source_p95_ms_max", ""),
            "ingest_to_emit_p95_ms": summary.get("ingest_to_emit_p95_ms_max", ""),
            "source_to_emit_p95_ms": summary.get("source_to_emit_p95_ms_max", ""),
            "proc_p95_ms": summary.get("proc_p95_ms_max", ""),
            "e2e_p95_ms": summary.get("e2e_p95_ms_max", ""),
            "avg_prediction_score": summary.get("avg_prediction_score_weighted", ""),
            "attack_ratio": summary.get("attack_ratio_weighted", ""),
        }
    )
    row["precision"] = summary.get("precision", "")
    row["recall"] = summary.get("recall", "")
    row["f1"] = summary.get("f1", "")
    row["fpr"] = summary.get("fpr", "")
    row["fnr"] = summary.get("fnr", "")
    row["late_event_ratio"] = summary.get("late_event_ratio_weighted", "")
    row["late_event_ratio_interpretable"] = summary.get("late_event_ratio_interpretable_weighted", "")
    row["freshness_signal_ratio"] = summary.get("freshness_signal_ratio_weighted", "")
    row["watermark_delay_sec"] = summary.get("watermark_delay_sec", "")
    row["kafka_lag_records"] = summary.get("kafka_lag_records_max", "")
    row["driver_cpu_percent"] = summary.get("driver_cpu_percent_avg", "")
    row["driver_rss_mb"] = summary.get("driver_rss_mb_avg", "")
    row["executor_mem_util_avg"] = summary.get("executor_mem_util_avg", "")
    row["executor_mem_util_p95"] = summary.get("executor_mem_util_p95_avg", "")
    row["executor_count"] = summary.get("executor_count_max", "")
    if not row["driver_cpu_percent"]:
        row["driver_cpu_percent"] = ((last_payload.get("system") or {}).get("driver_cpu_percent", ""))
    if not row["driver_rss_mb"]:
        row["driver_rss_mb"] = ((last_payload.get("system") or {}).get("driver_rss_mb", ""))
    if not row["executor_mem_util_avg"]:
        row["executor_mem_util_avg"] = ((last_payload.get("system") or {}).get("executor_mem_util_avg", ""))
    if not row["executor_mem_util_p95"]:
        row["executor_mem_util_p95"] = ((last_payload.get("system") or {}).get("executor_mem_util_p95", ""))
    if not row["executor_count"]:
        row["executor_count"] = ((last_payload.get("system") or {}).get("executor_count", ""))
    row["metric_warnings"] = "; ".join(summary.get("metric_warnings") or [])
    return row


def _write_summary(path: Path, rows: list[dict]) -> None:
    write_summary_rows(path, rows)


def _build_model_feature_pairs(
    raw_pairs: list[str],
    models: list[str],
    feature_sets: list[str],
) -> list[tuple[str, str]]:
    if raw_pairs:
        pairs: list[tuple[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for raw in raw_pairs:
            parts = [x.strip() for x in str(raw).split(":")]
            if len(parts) != 2:
                raise ValueError("model-feature pair must be model:feature_set")
            model = parts[0]
            feature_set = parts[1]
            if not model or not feature_set:
                raise ValueError("model-feature pair requires non-empty model and feature_set")
            pair = (model, feature_set)
            if pair in seen:
                continue
            seen.add(pair)
            pairs.append(pair)
        return pairs

    return [(model, feature_set) for model in models for feature_set in feature_sets]


def run(options: LayerBMatrixOptions) -> int:
    repeats = max(int(options.repeats), 1)
    model_feature_pairs = _build_model_feature_pairs(
        raw_pairs=list(options.model_feature_pairs),
        models=list(options.models),
        feature_sets=list(options.feature_sets),
    )

    cfg = load_yaml_mapping(resolve_project_path(options.config))
    kafka_cfg = cfg.get("kafka") or {}
    runtime_cfg = cfg.get("runtime") or {}
    spark_cfg = runtime_cfg.get("spark") or {}

    bootstrap_servers = str(kafka_cfg.get("bootstrap_servers", "kafka:29092"))
    metrics_topic = str(kafka_cfg.get("metrics_topic", "ids.metrics"))
    max_offsets_per_trigger = int(kafka_cfg.get("max_offsets_per_trigger", 20000) or 20000)
    trigger_interval = str(spark_cfg.get("trigger_interval", "10 seconds") or "10 seconds")
    python_exe = sys.executable
    summary_rows: list[dict] = []
    trace_schedule = parse_rate_schedule(
        options.trace_rate_schedule,
        error_message="trace rate schedule must be rps:seconds,rps:seconds",
    )
    warmup_schedule = parse_rate_schedule(
        options.warmup_rate_schedule,
        error_message="trace rate schedule must be rps:seconds,rps:seconds",
    )
    trace_mode = bool(trace_schedule) or options.trace_rows_per_sec > 0
    if trace_schedule and options.trace_rows_per_sec > 0:
        raise ValueError("Use either --trace-rows-per-sec or --trace-rate-schedule, not both")
    if warmup_schedule and options.warmup_rows_per_sec > 0:
        raise ValueError("Use either --warmup-rows-per-sec or --warmup-rate-schedule, not both")

    run_index = 0
    for repeat_index in range(1, repeats + 1):
        for model, feature_set in model_feature_pairs:
            run_index += 1
            run_started_at = time.time()
            run_start_timestamp_ms = int(time.time() * 1000)
            run_tag = (
                f"{options.run_prefix}_r{repeat_index:02d}_{run_index:02d}_{model}_{feature_set}_"
                f"{run_start_timestamp_ms}"
            )
            _log_phase(
                "run_start",
                run_tag=run_tag,
                repeat_index=repeat_index,
                run_index=run_index,
                model=model,
                feature_set=feature_set,
                trace_mode=trace_mode,
                max_rows=options.max_rows,
                batch_size=options.batch_size,
            )

            replay_cmd = build_replay_command(
                python_exe=python_exe,
                config=options.config,
                input_parquet=options.trace_input_parquet,
                trace_order_column=options.trace_order_column,
                run_tag=run_tag,
                max_rows=options.max_rows,
                batch_size=options.batch_size,
                rows_per_sec=options.trace_rows_per_sec,
                rate_schedule=options.trace_rate_schedule,
            )

            if options.warmup_rows > 0:
                warmup_tag = f"{run_tag}_warmup"
                _log_phase(
                    "warmup_prepare",
                    run_tag=run_tag,
                    warmup_tag=warmup_tag,
                    warmup_rows=options.warmup_rows,
                )
                warmup_rps = options.warmup_rows_per_sec if options.warmup_rows_per_sec > 0 else options.trace_rows_per_sec
                warmup_schedule_raw = options.warmup_rate_schedule.strip() or options.trace_rate_schedule.strip()
                warmup_replay_cmd = build_replay_command(
                    python_exe=python_exe,
                    config=options.config,
                    input_parquet=options.trace_input_parquet,
                    trace_order_column=options.trace_order_column,
                    run_tag=warmup_tag,
                    max_rows=options.warmup_rows,
                    batch_size=options.batch_size,
                    rows_per_sec=warmup_rps,
                    rate_schedule=warmup_schedule_raw,
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
                    warmup_stream_cmd = [
                        python_exe,
                        "scripts/streaming/run_structured_streaming.py",
                        "--config",
                        options.config,
                        "--model",
                        model,
                        "--feature-set",
                        feature_set,
                        "--run-tag",
                        warmup_tag,
                        "--load-profile",
                        f"{model}:{feature_set}",
                        "--input-run-tag",
                        warmup_tag,
                        "--override-starting-offsets",
                        "latest",
                        "--stop-on-input-sentinel",
                        "--run-seconds",
                        str(warmup_stream_seconds),
                        "--reset-checkpoint",
                    ]

                    _log_phase(
                        "warmup_stream_start",
                        run_tag=run_tag,
                        warmup_tag=warmup_tag,
                        run_seconds=warmup_stream_seconds,
                    )
                    warmup_stream_proc = start_background_process(
                        warmup_stream_cmd,
                        stdout_path=runtime_log_output_path(run_tag=warmup_tag),
                    )
                    try:
                        if not wait_for_process_startup(
                            warmup_stream_proc,
                            startup_wait_sec=options.trace_startup_wait_sec,
                        ):
                            raise RuntimeError("structured streaming process exited before warmup replay")
                        _log_phase("warmup_replay_start", run_tag=run_tag, warmup_tag=warmup_tag)
                        run_command_or_raise(warmup_replay_cmd)
                        _log_phase("warmup_replay_done", run_tag=run_tag, warmup_tag=warmup_tag)
                        warmup_wait_timeout_sec = max(warmup_stream_seconds + 60, 120)
                        _log_phase(
                            "warmup_stream_wait_start",
                            run_tag=run_tag,
                            warmup_tag=warmup_tag,
                            wait_timeout_sec=warmup_wait_timeout_sec,
                        )
                        try:
                            warmup_stream_proc.wait(timeout=warmup_wait_timeout_sec)
                            _log_phase("warmup_stream_wait_done", run_tag=run_tag, warmup_tag=warmup_tag)
                        except subprocess.TimeoutExpired:
                            _log_phase("warmup_stream_wait_timeout", run_tag=run_tag, warmup_tag=warmup_tag)
                            pass
                    finally:
                        stop_background_process(warmup_stream_proc)
                        _log_phase("warmup_stream_stop", run_tag=run_tag, warmup_tag=warmup_tag)
                else:
                    _log_phase("warmup_replay_start", run_tag=run_tag, warmup_tag=warmup_tag)
                    run_command_or_raise(warmup_replay_cmd)
                    _log_phase("warmup_replay_done", run_tag=run_tag, warmup_tag=warmup_tag)
                    warmup_stream_cmd = [
                        python_exe,
                        "scripts/streaming/run_structured_streaming.py",
                        "--config",
                        options.config,
                        "--model",
                        model,
                        "--feature-set",
                        feature_set,
                        "--run-tag",
                        warmup_tag,
                        "--load-profile",
                        f"{model}:{feature_set}",
                        "--input-run-tag",
                        warmup_tag,
                        "--reset-checkpoint",
                        "--available-now",
                    ]
                    _log_phase("warmup_stream_start", run_tag=run_tag, warmup_tag=warmup_tag, available_now=True)
                    run_command_or_raise(warmup_stream_cmd)
                    _log_phase("warmup_stream_stop", run_tag=run_tag, warmup_tag=warmup_tag, available_now=True)

            if trace_mode:
                stream_seconds = estimate_stream_runtime(
                    max_rows=options.max_rows,
                    trace_rows_per_sec=options.trace_rows_per_sec,
                    trace_schedule=trace_schedule,
                    override_seconds=options.trace_stream_run_seconds,
                    max_offsets_per_trigger=max_offsets_per_trigger,
                    trigger_interval=trigger_interval,
                )
                stream_cmd = [
                    python_exe,
                    "scripts/streaming/run_structured_streaming.py",
                    "--config",
                    options.config,
                    "--model",
                    model,
                    "--feature-set",
                    feature_set,
                    "--run-tag",
                    run_tag,
                    "--load-profile",
                    f"{model}:{feature_set}",
                    "--input-run-tag",
                    run_tag,
                    "--override-starting-offsets",
                    "latest",
                    "--stop-on-input-sentinel",
                    "--run-seconds",
                    str(stream_seconds),
                    "--reset-checkpoint",
                ]

                _log_phase("main_stream_start", run_tag=run_tag, run_seconds=stream_seconds)
                stream_proc = start_background_process(
                    stream_cmd,
                    stdout_path=runtime_log_output_path(run_tag=run_tag),
                )
                try:
                    if not wait_for_process_startup(
                        stream_proc,
                        startup_wait_sec=options.trace_startup_wait_sec,
                    ):
                        raise RuntimeError("structured streaming process exited before replay")
                    _log_phase("main_replay_start", run_tag=run_tag)
                    run_command_or_raise(replay_cmd)
                    _log_phase("main_replay_done", run_tag=run_tag)
                    stream_wait_timeout_sec = max(stream_seconds + 60, 120)
                    _log_phase(
                        "main_stream_wait_start",
                        run_tag=run_tag,
                        wait_timeout_sec=stream_wait_timeout_sec,
                    )
                    try:
                        stream_proc.wait(timeout=stream_wait_timeout_sec)
                        _log_phase("main_stream_wait_done", run_tag=run_tag)
                    except subprocess.TimeoutExpired:
                        _log_phase("main_stream_wait_timeout", run_tag=run_tag)
                        pass
                finally:
                    stop_background_process(stream_proc)
                    _log_phase("main_stream_stop", run_tag=run_tag)
            else:
                _log_phase("main_replay_start", run_tag=run_tag)
                run_command_or_raise(replay_cmd)
                _log_phase("main_replay_done", run_tag=run_tag)
                stream_cmd = [
                    python_exe,
                    "scripts/streaming/run_structured_streaming.py",
                    "--config",
                    options.config,
                    "--model",
                    model,
                    "--feature-set",
                    feature_set,
                    "--run-tag",
                    run_tag,
                    "--load-profile",
                    f"{model}:{feature_set}",
                    "--input-run-tag",
                    run_tag,
                    "--reset-checkpoint",
                    "--available-now",
                ]
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
            legacy_row = _flatten_metrics(run_tag, repeat_index, model, feature_set, metrics_rows)
            row = annotate_sut_debug_summary(legacy_row)
            summary_rows.append(row)
            _log_phase(
                "run_summary",
                run_tag=run_tag,
                status=row["status"],
                rows=row["rows"],
                precision=row["precision"],
                recall=row["recall"],
                fpr=row["fpr"],
                fnr=row["fnr"],
                elapsed_sec=f"{time.time() - run_started_at:.2f}",
            )

    summary_path = resolve_project_path(options.summary_csv)
    _write_summary(summary_path, summary_rows)
    print(f"Saved Layer B summary: {summary_path}", flush=True)
    return 0


