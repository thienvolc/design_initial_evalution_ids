from __future__ import annotations

import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from ids_platform.common.config import load_yaml_mapping
from ids_platform.common.paths import PROJECT_ROOT, resolve_project_path
from ids_platform.common.subprocess import run_command_or_raise, start_background_process, stop_background_process
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
    parse_rate_schedule,
    schedule_total_seconds,
)


@dataclass(frozen=True)
class LoadQualityMatrixOptions:
    config: str
    model: str
    feature_set: str
    load_profiles: tuple[str, ...]
    trace_rate_schedule: str
    trace_max_rows: int
    trace_profile_name: str
    trace_stream_run_seconds: int
    trace_startup_wait_sec: int
    warmup_rows: int
    warmup_rows_per_sec: float
    warmup_rate_schedule: str
    warmup_stream_run_seconds: int
    warmup_startup_wait_sec: int
    batch_size: int
    metrics_timeout_sec: int
    metrics_idle_sec: int
    summary_csv: str


def _log_phase(event: str, **fields) -> None:
    parts = [f"[phase] matrix=load_quality event={event}"]
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
        group_prefix="load-quality-metrics",
        start_timestamp_ms=start_timestamp_ms,
    )


def _parse_profiles(raw_profiles: list[str]) -> list[dict]:
    out = []
    for raw in raw_profiles:
        parts = [x.strip() for x in raw.split(":")]
        if len(parts) != 3:
            raise ValueError("load profile must be name:rows_per_sec:max_rows")
        out.append({"name": parts[0], "rows_per_sec": float(parts[1]), "max_rows": int(parts[2])})
    return out


def _average_rps(schedule: list[tuple[float, float]]) -> float:
    if not schedule:
        return 0.0
    total_rows = sum(rps * sec for rps, sec in schedule)
    total_sec = sum(sec for _, sec in schedule)
    return total_rows / total_sec if total_sec > 0 else 0.0


def _start_stream_process(
    config: str,
    model: str,
    feature_set: str,
    run_tag: str,
    run_seconds: int,
) -> subprocess.Popen:
    if shutil.which("docker"):
        cmd = [
            "docker",
            "compose",
            "exec",
            "-T",
            "ids-dev",
            "python",
            "scripts/streaming/run_structured_streaming.py",
            "--config",
            config,
            "--model",
            model,
            "--feature-set",
            feature_set,
            "--run-tag",
            run_tag,
            "--input-run-tag",
            run_tag,
            "--override-starting-offsets",
            "latest",
            "--load-profile",
            "trace_schedule",
            "--stop-on-input-sentinel",
            "--run-seconds",
            str(max(run_seconds, 1)),
            "--reset-checkpoint",
        ]
    else:
        cmd = [
            sys.executable,
            "scripts/streaming/run_structured_streaming.py",
            "--config",
            config,
            "--model",
            model,
            "--feature-set",
            feature_set,
            "--run-tag",
            run_tag,
            "--input-run-tag",
            run_tag,
            "--override-starting-offsets",
            "latest",
            "--load-profile",
            "trace_schedule",
            "--stop-on-input-sentinel",
            "--run-seconds",
            str(max(run_seconds, 1)),
            "--reset-checkpoint",
        ]
    return start_background_process(
        cmd,
        cwd=PROJECT_ROOT,
        stdout_path=runtime_log_output_path(run_tag=run_tag),
    )


def _to_row(run_tag: str, profile: dict, metrics_rows: list[dict], rate_schedule: str = "") -> dict:
    row = {
        "run_tag": run_tag,
        "load_profile": profile["name"],
        "rows_per_sec_target": profile["rows_per_sec"],
        "max_rows": profile["max_rows"],
        "rate_schedule": rate_schedule,
        "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "rows": "",
        "source_p95_ms": "",
        "ingest_to_emit_p95_ms": "",
        "source_to_emit_p95_ms": "",
        "proc_p95_ms": "",
        "e2e_p95_ms": "",
        "rows_per_sec_actual": "",
        "precision": "",
        "recall": "",
        "f1": "",
        "fpr": "",
        "fnr": "",
        "kafka_lag_records": "",
        "late_event_ratio": "",
        "late_event_ratio_interpretable": "",
        "freshness_signal_ratio": "",
        "metric_warnings": "",
        "status": "ok" if metrics_rows else "metrics_missing",
    }
    if not metrics_rows:
        return row

    summary = summarize_runtime_metrics(metrics_rows)
    row["rows"] = summary.get("rows_total", "")
    row["rows_per_sec_actual"] = summary.get("rows_per_sec_avg", "")
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
    row["kafka_lag_records"] = summary.get("kafka_lag_records_max", "")
    row["late_event_ratio"] = summary.get("late_event_ratio_weighted", "")
    row["late_event_ratio_interpretable"] = summary.get("late_event_ratio_interpretable_weighted", "")
    row["freshness_signal_ratio"] = summary.get("freshness_signal_ratio_weighted", "")
    row["metric_warnings"] = "; ".join(summary.get("metric_warnings") or [])
    return row


def _write_summary(path: Path, rows: list[dict]) -> None:
    write_summary_rows(path, rows)


def run(options: LoadQualityMatrixOptions) -> int:
    schedule_pairs = parse_rate_schedule(
        options.trace_rate_schedule,
        error_message="trace rate schedule must be rps:seconds,rps:seconds",
    )
    warmup_schedule_pairs = parse_rate_schedule(
        options.warmup_rate_schedule,
        error_message="trace rate schedule must be rps:seconds,rps:seconds",
    )
    if schedule_pairs:
        if options.trace_max_rows <= 0:
            raise ValueError("--trace-max-rows must be > 0 when --trace-rate-schedule is provided")
        profiles = [
            {
                "name": options.trace_profile_name,
                "rows_per_sec": _average_rps(schedule_pairs),
                "max_rows": int(options.trace_max_rows),
            }
        ]
    else:
        profiles = _parse_profiles(list(options.load_profiles))
    if warmup_schedule_pairs and options.warmup_rows_per_sec > 0:
        raise ValueError("Use either --warmup-rows-per-sec or --warmup-rate-schedule, not both")

    cfg = load_yaml_mapping(resolve_project_path(options.config))
    kafka_cfg = cfg.get("kafka") or {}
    bootstrap_servers = str(kafka_cfg.get("bootstrap_servers", "kafka:29092"))
    metrics_topic = str(kafka_cfg.get("metrics_topic", "ids.metrics"))
    rows = []
    python_exe = sys.executable

    for idx, profile in enumerate(profiles, start=1):
        run_started_at = time.time()
        run_start_timestamp_ms = int(time.time() * 1000)
        run_tag = f"load_{idx:02d}_{profile['name']}_{run_start_timestamp_ms}"
        _log_phase(
            "run_start",
            run_tag=run_tag,
            run_index=idx,
            load_profile=profile["name"],
            rows_per_sec_target=profile["rows_per_sec"],
            max_rows=profile["max_rows"],
            schedule_mode=bool(schedule_pairs),
        )

        replay_cmd = build_replay_command(
                python_exe=python_exe,
                config=options.config,
                run_tag=run_tag,
                max_rows=profile["max_rows"],
                batch_size=options.batch_size,
                rows_per_sec=profile["rows_per_sec"],
                rate_schedule=options.trace_rate_schedule if schedule_pairs else "",
            )

        if options.warmup_rows > 0:
            warmup_tag = f"{run_tag}_warmup"
            _log_phase("warmup_prepare", run_tag=run_tag, warmup_tag=warmup_tag, warmup_rows=options.warmup_rows)
            warmup_rps = options.warmup_rows_per_sec if options.warmup_rows_per_sec > 0 else min(profile["rows_per_sec"], 5000.0)
            warmup_schedule_raw = options.warmup_rate_schedule.strip()

            warmup_replay_cmd = build_replay_command(
                python_exe=python_exe,
                config=options.config,
                run_tag=warmup_tag,
                max_rows=options.warmup_rows,
                batch_size=options.batch_size,
                rows_per_sec=warmup_rps,
                rate_schedule=warmup_schedule_raw,
            )

            if schedule_pairs:
                warmup_total_sec = schedule_total_seconds(warmup_schedule_pairs)
                warmup_stream_seconds = (
                    options.warmup_stream_run_seconds
                    if options.warmup_stream_run_seconds > 0
                    else int(
                        warmup_total_sec + 180
                        if warmup_schedule_pairs
                        else (options.warmup_rows / max(warmup_rps, 1e-9)) + 180
                    )
                )
                warmup_stream_proc = _start_stream_process(
                    config=options.config,
                    model=options.model,
                    feature_set=options.feature_set,
                    run_tag=warmup_tag,
                    run_seconds=warmup_stream_seconds,
                )
                _log_phase("warmup_stream_start", run_tag=run_tag, warmup_tag=warmup_tag, run_seconds=warmup_stream_seconds)
                try:
                    if not wait_for_process_startup(
                        warmup_stream_proc,
                        startup_wait_sec=options.warmup_startup_wait_sec,
                    ):
                        raise RuntimeError("structured streaming process exited before warmup replay")
                    _log_phase("warmup_replay_start", run_tag=run_tag, warmup_tag=warmup_tag)
                    run_command_or_raise(warmup_replay_cmd)
                    _log_phase("warmup_replay_done", run_tag=run_tag, warmup_tag=warmup_tag)
                    warmup_wait_timeout_sec = max(warmup_stream_seconds + 60, 120)
                    _log_phase("warmup_stream_wait_start", run_tag=run_tag, warmup_tag=warmup_tag, wait_timeout_sec=warmup_wait_timeout_sec)
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
                    "--load-profile",
                    f"{profile['name']}_warmup",
                    "--reset-checkpoint",
                    "--available-now",
                ]
                _log_phase("warmup_stream_start", run_tag=run_tag, warmup_tag=warmup_tag, available_now=True)
                run_command_or_raise(warmup_stream_cmd)
                _log_phase("warmup_stream_stop", run_tag=run_tag, warmup_tag=warmup_tag, available_now=True)

        if schedule_pairs:
            total_sec = schedule_total_seconds(schedule_pairs)
            stream_run_seconds = (
                options.trace_stream_run_seconds
                if options.trace_stream_run_seconds > 0
                else int(total_sec + 180)
            )

            stream_proc = _start_stream_process(
                config=options.config,
                model=options.model,
                feature_set=options.feature_set,
                run_tag=run_tag,
                run_seconds=stream_run_seconds,
            )
            _log_phase("main_stream_start", run_tag=run_tag, run_seconds=stream_run_seconds)
            try:
                if not wait_for_process_startup(
                    stream_proc,
                    startup_wait_sec=options.trace_startup_wait_sec,
                ):
                    raise RuntimeError("structured streaming process exited before replay")
                _log_phase("main_replay_start", run_tag=run_tag)
                run_command_or_raise(replay_cmd)
                _log_phase("main_replay_done", run_tag=run_tag)
                stream_wait_timeout_sec = max(stream_run_seconds + 60, 120)
                _log_phase("main_stream_wait_start", run_tag=run_tag, wait_timeout_sec=stream_wait_timeout_sec)
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
                "--load-profile",
                profile["name"],
                "--reset-checkpoint",
                "--available-now",
            ]
            _log_phase("main_stream_start", run_tag=run_tag, available_now=True)
            run_command_or_raise(stream_cmd)
            _log_phase("main_stream_stop", run_tag=run_tag, available_now=True)

        _log_phase("metrics_collect_start", run_tag=run_tag, timeout_sec=options.metrics_timeout_sec, idle_sec=options.metrics_idle_sec)
        metrics_rows = _wait_metrics(
            bootstrap_servers=bootstrap_servers,
            topic=metrics_topic,
            run_tag=run_tag,
            timeout_sec=options.metrics_timeout_sec,
            idle_sec=options.metrics_idle_sec,
            start_timestamp_ms=max(run_start_timestamp_ms - 30_000, 0),
        )
        _log_phase("metrics_collect_done", run_tag=run_tag, metrics_rows=len(metrics_rows), elapsed_sec=f"{time.time() - run_started_at:.2f}")
        timeseries_path = write_metrics_timeseries(metrics_rows, run_tag=run_tag)
        if timeseries_path is not None:
            _log_phase("timeseries_write_done", run_tag=run_tag, path=timeseries_path)
        legacy_row = _to_row(
            run_tag,
            profile,
            metrics_rows,
            rate_schedule=options.trace_rate_schedule if schedule_pairs else "",
        )
        row = annotate_sut_debug_summary(legacy_row)
        rows.append(row)
        _log_phase(
            "run_summary",
            run_tag=run_tag,
            load_profile=profile["name"],
            status=row["status"],
            e2e_p95_ms=row["e2e_p95_ms"],
            f1=row["f1"],
            lag=row["kafka_lag_records"],
            elapsed_sec=f"{time.time() - run_started_at:.2f}",
        )

    out = resolve_project_path(options.summary_csv)
    _write_summary(out, rows)
    print(f"Saved load quality summary: {out}", flush=True)
    return 0


