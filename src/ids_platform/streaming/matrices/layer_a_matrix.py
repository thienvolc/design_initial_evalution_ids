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
from ids_platform.streaming.matrices.common import (
    annotate_sut_debug_summary,
    collect_matching_metrics,
    summarize_runtime_metrics,
    wait_for_process_startup,
    write_summary_rows,
)
from ids_platform.streaming.replay.config import (
    build_replay_command,
    estimate_stream_runtime,
    parse_rate_schedule,
)


@dataclass(frozen=True)
class LayerAMatrixOptions:
    config: str
    model: str
    feature_set: str
    profiles: tuple[str, ...]
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
    parts = [f"[phase] matrix=layer_a event={event}"]
    for key, value in fields.items():
        if value is None:
            continue
        parts.append(f"{key}={value}")
    print(" ".join(parts), flush=True)


def _parse_profiles(raw_profiles: list[str]) -> list[dict]:
    profiles = []
    for raw in raw_profiles:
        parts = [part.strip() for part in raw.split(":")]
        if len(parts) < 3:
            raise ValueError(
                "Invalid profile format. Use "
                "name:max_offsets_per_trigger:shuffle_partitions[:trigger_interval]"
            )

        name = parts[0]
        max_offsets = int(parts[1])
        shuffle_partitions = int(parts[2])
        trigger_interval = ":".join(parts[3:]).strip() if len(parts) > 3 else ""
        if max_offsets <= 0 or shuffle_partitions <= 0:
            raise ValueError("Profile values max_offsets and shuffle_partitions must be > 0")

        profiles.append(
            {
                "name": name,
                "max_offsets_per_trigger": max_offsets,
                "shuffle_partitions": shuffle_partitions,
                "trigger_interval": trigger_interval,
            }
        )
    return profiles


def _collect_metrics(
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
        group_prefix="layer-a-metrics",
        start_timestamp_ms=start_timestamp_ms,
    )


def _aggregate_row(
    run_tag: str,
    repeat_index: int,
    model: str,
    feature_set: str,
    profile: dict,
    metrics_rows: list[dict],
) -> dict:
    row = {
        "run_tag": run_tag,
        "repeat_index": repeat_index,
        "model": model,
        "feature_set": feature_set,
        "profile": profile["name"],
        "max_offsets_per_trigger": profile["max_offsets_per_trigger"],
        "shuffle_partitions": profile["shuffle_partitions"],
        "trigger_interval": profile.get("trigger_interval") or "",
        "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "batches": 0,
        "rows_total": 0,
        "rows_per_sec_avg": "",
        "batch_wall_ms_avg": "",
        "source_p95_ms_max": "",
        "proc_p95_ms_max": "",
        "e2e_p95_ms_max": "",
        "avg_prediction_score": "",
        "attack_ratio": "",
        "precision_avg": "",
        "recall_avg": "",
        "f1_avg": "",
        "fpr_avg": "",
        "fnr_avg": "",
        "late_event_ratio_avg": "",
        "kafka_lag_records_max": "",
        "driver_rss_mb_avg": "",
        "executor_mem_util_avg": "",
        "status": "ok" if metrics_rows else "metrics_missing",
    }

    if not metrics_rows:
        return row

    summary = summarize_runtime_metrics(metrics_rows)
    row["batches"] = int(summary.get("batch_count") or 0)
    row["rows_total"] = summary.get("rows_total", "")
    row["rows_per_sec_avg"] = summary.get("rows_per_sec_avg", "")
    row["batch_wall_ms_avg"] = summary.get("batch_wall_ms_avg", "")
    row["source_p95_ms_max"] = summary.get("source_p95_ms_max", "")
    row["proc_p95_ms_max"] = summary.get("proc_p95_ms_max", "")
    row["e2e_p95_ms_max"] = summary.get("e2e_p95_ms_max", "")
    row["avg_prediction_score"] = summary.get("avg_prediction_score_weighted", "")
    row["attack_ratio"] = summary.get("attack_ratio_weighted", "")
    row["precision_avg"] = summary.get("precision", "")
    row["recall_avg"] = summary.get("recall", "")
    row["f1_avg"] = summary.get("f1", "")
    row["fpr_avg"] = summary.get("fpr", "")
    row["fnr_avg"] = summary.get("fnr", "")
    row["late_event_ratio_avg"] = summary.get("late_event_ratio_weighted", "")
    row["kafka_lag_records_max"] = summary.get("kafka_lag_records_max", "")
    row["driver_rss_mb_avg"] = summary.get("driver_rss_mb_avg", "")
    row["executor_mem_util_avg"] = summary.get("executor_mem_util_avg", "")
    return row


def _write_summary(path: Path, rows: list[dict]) -> None:
    write_summary_rows(path, rows)


def run(options: LayerAMatrixOptions) -> int:

    # ── profiles ────────────────────────────────────────
    profiles = _parse_profiles(list(options.profiles))
    repeats = max(int(options.repeats), 1)

    # ── config ──────────────────────────────────────────
    cfg = load_yaml_mapping(resolve_project_path(options.config))
    kafka_cfg = cfg.get("kafka") or {}
    bootstrap_servers = str(kafka_cfg.get("bootstrap_servers", "kafka:29092"))
    metrics_topic = str(kafka_cfg.get("metrics_topic", "ids.metrics"))
    # ── runtime ────────────────────────────────────────
    python_exe = sys.executable


    # ── warmup ──────────────────────────────────────────
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

    # ── run ─────────────────────────────────────────────
    for repeat_index in range(1, repeats + 1):
        for index, profile in enumerate(profiles, start=1):
            run_started_at = time.time()
            run_start_timestamp_ms = int(time.time() * 1000)
            run_tag = (
                f"{options.run_prefix}_r{repeat_index:02d}_{index:02d}_{profile['name']}_"
                f"{run_start_timestamp_ms}"
            )
            _log_phase(
                "run_start",
                run_tag=run_tag,
                repeat_index=repeat_index,
                run_index=index,
                model=options.model,
                feature_set=options.feature_set,
                profile=profile["name"],
                max_offsets=profile["max_offsets_per_trigger"],
                shuffle=profile["shuffle_partitions"],
                trigger=(profile.get("trigger_interval") or "config_default"),
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
                        max_offsets_per_trigger=int(profile["max_offsets_per_trigger"]),
                        trigger_interval=str(profile.get("trigger_interval", "")),
                    )
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
                        "--input-run-tag",
                        warmup_tag,
                        "--override-starting-offsets",
                        "latest",
                        "--override-max-offsets",
                        str(profile["max_offsets_per_trigger"]),
                        "--override-shuffle-partitions",
                        str(profile["shuffle_partitions"]),
                        "--run-seconds",
                        str(warmup_stream_seconds),
                        "--reset-checkpoint",
                    ]
                    if profile.get("trigger_interval"):
                        warmup_stream_cmd.extend(["--override-trigger-interval", str(profile["trigger_interval"])])

                    _log_phase(
                        "warmup_stream_start",
                        run_tag=run_tag,
                        warmup_tag=warmup_tag,
                        run_seconds=warmup_stream_seconds,
                    )
                    warmup_stream_proc = start_background_process(warmup_stream_cmd)
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
                        options.model,
                        "--feature-set",
                        options.feature_set,
                        "--run-tag",
                        warmup_tag,
                        "--input-run-tag",
                        warmup_tag,
                        "--override-max-offsets",
                        str(profile["max_offsets_per_trigger"]),
                        "--override-shuffle-partitions",
                        str(profile["shuffle_partitions"]),
                        "--reset-checkpoint",
                        "--available-now",
                    ]
                    if profile.get("trigger_interval"):
                        warmup_stream_cmd.extend(["--override-trigger-interval", str(profile["trigger_interval"])])
                    _log_phase("warmup_stream_start", run_tag=run_tag, warmup_tag=warmup_tag, available_now=True)
                    run_command_or_raise(warmup_stream_cmd)
                    _log_phase("warmup_stream_stop", run_tag=run_tag, warmup_tag=warmup_tag, available_now=True)

            if trace_mode:
                stream_seconds = estimate_stream_runtime(
                    max_rows=options.max_rows,
                    trace_rows_per_sec=options.trace_rows_per_sec,
                    trace_schedule=trace_schedule,
                    override_seconds=options.trace_stream_run_seconds,
                    max_offsets_per_trigger=int(profile["max_offsets_per_trigger"]),
                    trigger_interval=str(profile.get("trigger_interval", "")),
                )
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
                    "--input-run-tag",
                    run_tag,
                    "--override-starting-offsets",
                    "latest",
                    "--override-max-offsets",
                    str(profile["max_offsets_per_trigger"]),
                    "--override-shuffle-partitions",
                    str(profile["shuffle_partitions"]),
                    "--run-seconds",
                    str(stream_seconds),
                    "--reset-checkpoint",
                ]
                if profile.get("trigger_interval"):
                    stream_cmd.extend(["--override-trigger-interval", str(profile["trigger_interval"])])

                _log_phase("main_stream_start", run_tag=run_tag, run_seconds=stream_seconds)
                stream_proc = start_background_process(stream_cmd)
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
                    options.model,
                    "--feature-set",
                    options.feature_set,
                    "--run-tag",
                    run_tag,
                    "--input-run-tag",
                    run_tag,
                    "--override-max-offsets",
                    str(profile["max_offsets_per_trigger"]),
                    "--override-shuffle-partitions",
                    str(profile["shuffle_partitions"]),
                    "--reset-checkpoint",
                    "--available-now",
                ]
                if profile.get("trigger_interval"):
                    stream_cmd.extend(["--override-trigger-interval", str(profile["trigger_interval"])])
                _log_phase("main_stream_start", run_tag=run_tag, available_now=True)
                run_command_or_raise(stream_cmd)
                _log_phase("main_stream_stop", run_tag=run_tag, available_now=True)

            _log_phase(
                "metrics_collect_start",
                run_tag=run_tag,
                timeout_sec=options.metrics_timeout_sec,
                idle_sec=options.metrics_idle_sec,
            )
            metrics_rows = _collect_metrics(
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

            legacy_summary = _aggregate_row(
                run_tag=run_tag,
                repeat_index=repeat_index,
                model=options.model,
                feature_set=options.feature_set,
                profile=profile,
                metrics_rows=metrics_rows,
            )
            summary = annotate_sut_debug_summary(legacy_summary)
            summary_rows.append(summary)
            _log_phase(
                "run_summary",
                run_tag=run_tag,
                profile=profile["name"],
                status=summary["status"],
                batches=summary["batches"],
                rows_total=summary["rows_total"],
                fpr_avg=summary["fpr_avg"],
                fnr_avg=summary["fnr_avg"],
                elapsed_sec=f"{time.time() - run_started_at:.2f}",
            )

    summary_path = resolve_project_path(options.summary_csv)
    _write_summary(summary_path, summary_rows)
    print(f"Saved Layer A summary: {summary_path}", flush=True)
    return 0

