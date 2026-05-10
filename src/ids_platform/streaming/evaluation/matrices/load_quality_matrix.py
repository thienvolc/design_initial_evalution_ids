from __future__ import annotations

import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone

from ids_platform.common.paths import resolve_project_path
from ids_platform.common.subprocess import run_command_or_raise, start_background_process, stop_background_process
from ids_platform.streaming.evaluation.matrices.common import (
    annotate_sut_debug_summary,
    collect_matching_metrics,
    runtime_log_output_path,
    summarize_runtime_metrics,
    wait_for_log_patterns,
    wait_for_process_startup,
    write_metrics_timeseries,
    write_summary_rows,
)
from ids_platform.streaming.evaluation.matrices.throughput.matrix_support import (
    collect_metrics_with_logging,
    current_python_executable,
    parse_trace_and_warmup_schedules,
    reset_kafka_topics,
    resolve_streaming_metrics_context,
    validate_schedule_mode,
)
from ids_platform.streaming.evaluation.matrices.throughput.result_helpers import (
    materialize_sut_summary_row,
    persist_summary_rows,
)
from ids_platform.streaming.evaluation.matrices.throughput.stream_support import (
    build_structured_stream_command,
    run_available_now_sequence,
    run_available_now_warmup_sequence,
    run_trace_warmup_sequence,
)
from ids_platform.streaming.replay.config import (
    build_replay_command,
    estimate_stream_runtime,
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


def _run_trace_stream_sequence_graceful(
    *,
    run_tag: str,
    stream_cmd: list[str],
    replay_cmd: list[str],
    stream_seconds: int,
    startup_wait_sec: int,
    runtime_log_output_path_fn,
    log_phase_fn,
    start_background_process_fn,
    wait_for_process_startup_fn,
    run_command_or_raise_fn,
    stop_background_process_fn,
    wait_for_log_patterns_fn,
    subprocess_module,
) -> None:
    runtime_log_path = runtime_log_output_path_fn(run_tag=run_tag)
    log_phase_fn("main_stream_start", run_tag=run_tag, run_seconds=stream_seconds)
    log_phase_fn("stream_runtime_log", run_tag=run_tag, path=runtime_log_path)
    stream_proc = start_background_process_fn(
        stream_cmd,
        stdout_path=runtime_log_path,
    )
    try:
        if not wait_for_process_startup_fn(
            stream_proc,
            startup_wait_sec=startup_wait_sec,
        ):
            raise RuntimeError("structured streaming process exited before replay")
        log_phase_fn("main_replay_start", run_tag=run_tag)
        run_command_or_raise_fn(replay_cmd)
        log_phase_fn("main_replay_done", run_tag=run_tag)
        stream_wait_timeout_sec = max(stream_seconds + 60, 120)
        log_phase_fn(
            "main_stream_wait_start",
            run_tag=run_tag,
            wait_timeout_sec=stream_wait_timeout_sec,
        )
        graceful_marker = wait_for_log_patterns_fn(
            process=stream_proc,
            log_path=str(runtime_log_path),
            patterns=[
                f"[stream] event=input_sentinel_seen run_tag={run_tag}",
                f"[stream] event=stop_condition_met run_tag={run_tag} reason=input_sentinel",
                f"[stream] event=job_stop run_tag={run_tag}",
            ],
            timeout_sec=stream_wait_timeout_sec,
            poll_seconds=0.5,
        )
        if graceful_marker:
            log_phase_fn("main_stream_graceful_marker_seen", run_tag=run_tag, marker=graceful_marker)
            if "job_stop" not in graceful_marker:
                job_stop_marker = wait_for_log_patterns_fn(
                    process=stream_proc,
                    log_path=str(runtime_log_path),
                    patterns=[f"[stream] event=job_stop run_tag={run_tag}"],
                    timeout_sec=30,
                    poll_seconds=0.5,
                )
                if job_stop_marker:
                    log_phase_fn("main_stream_job_stop_seen", run_tag=run_tag, marker=job_stop_marker)
        try:
            stream_proc.wait(timeout=30)
            log_phase_fn("main_stream_wait_done", run_tag=run_tag)
        except subprocess_module.TimeoutExpired:
            try:
                stream_proc.wait(timeout=stream_wait_timeout_sec)
                log_phase_fn("main_stream_wait_done", run_tag=run_tag)
            except subprocess_module.TimeoutExpired:
                log_phase_fn("main_stream_wait_timeout", run_tag=run_tag)
    finally:
        stop_background_process_fn(stream_proc)
        log_phase_fn("main_stream_stop", run_tag=run_tag)


def run(options: LoadQualityMatrixOptions) -> int:
    schedule_pairs, warmup_schedule_pairs = parse_trace_and_warmup_schedules(
        trace_rate_schedule=options.trace_rate_schedule,
        warmup_rate_schedule=options.warmup_rate_schedule,
        trace_error_message="trace rate schedule must be rps:seconds,rps:seconds",
        warmup_error_message="trace rate schedule must be rps:seconds,rps:seconds",
    )
    trace_mode = validate_schedule_mode(
        trace_schedule=schedule_pairs,
        trace_rows_per_sec=0.0,
        warmup_schedule=warmup_schedule_pairs,
        warmup_rows_per_sec=options.warmup_rows_per_sec,
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

    bootstrap_servers, metrics_topic, cfg, _, spark_cfg = resolve_streaming_metrics_context(options.config)
    kafka_cfg = cfg.get("kafka") or {}
    input_topic = str(kafka_cfg.get("input_topic", "ids.raw.flows"))
    output_topic = str(kafka_cfg.get("output_topic", "ids.predictions.binary"))
    max_offsets_per_trigger = int(kafka_cfg.get("max_offsets_per_trigger", 20000) or 20000)
    trigger_interval = str(spark_cfg.get("trigger_interval", "10 seconds") or "10 seconds")
    benchmark_topic_names = [input_topic, output_topic, metrics_topic]

    rows = []
    python_exe = current_python_executable()

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
            trace_mode=trace_mode,
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
            run_tag=run_tag,
            max_rows=profile["max_rows"],
            batch_size=options.batch_size,
            rows_per_sec=profile["rows_per_sec"],
            rate_schedule=options.trace_rate_schedule if schedule_pairs else "",
        )

        if options.warmup_rows > 0:
            warmup_tag = f"{run_tag}_warmup"
            _log_phase(
                "warmup_prepare",
                run_tag=run_tag,
                warmup_tag=warmup_tag,
                warmup_rows=options.warmup_rows,
            )
            warmup_schedule_raw = options.warmup_rate_schedule.strip()
            warmup_rps = (
                options.warmup_rows_per_sec
                if options.warmup_rows_per_sec > 0
                else min(profile["rows_per_sec"], 5000.0)
            )
            warmup_replay_cmd = build_replay_command(
                python_exe=python_exe,
                config=options.config,
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
                    trace_schedule=warmup_schedule_pairs,
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
                    load_profile=f"{profile['name']}_warmup",
                    input_run_tag=warmup_tag,
                    starting_offsets="latest",
                    stop_on_input_sentinel=True,
                    run_seconds=warmup_stream_seconds,
                    reset_checkpoint=True,
                )
                run_trace_warmup_sequence(
                    run_tag=run_tag,
                    warmup_tag=warmup_tag,
                    warmup_stream_cmd=warmup_stream_cmd,
                    warmup_replay_cmd=warmup_replay_cmd,
                    warmup_stream_seconds=warmup_stream_seconds,
                    startup_wait_sec=options.warmup_startup_wait_sec,
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
                    load_profile=f"{profile['name']}_warmup",
                    input_run_tag=warmup_tag,
                    reset_checkpoint=True,
                    available_now=True,
                )
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
            stream_run_seconds = estimate_stream_runtime(
                max_rows=profile["max_rows"],
                trace_rows_per_sec=profile["rows_per_sec"],
                trace_schedule=schedule_pairs,
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
                load_profile=profile["name"],
                input_run_tag=run_tag,
                starting_offsets="latest",
                stop_on_input_sentinel=True,
                run_seconds=stream_run_seconds,
                reset_checkpoint=True,
            )
            _run_trace_stream_sequence_graceful(
                run_tag=run_tag,
                stream_cmd=stream_cmd,
                replay_cmd=replay_cmd,
                stream_seconds=stream_run_seconds,
                startup_wait_sec=options.trace_startup_wait_sec,
                runtime_log_output_path_fn=runtime_log_output_path,
                log_phase_fn=_log_phase,
                start_background_process_fn=start_background_process,
                wait_for_process_startup_fn=wait_for_process_startup,
                run_command_or_raise_fn=run_command_or_raise,
                stop_background_process_fn=stop_background_process,
                wait_for_log_patterns_fn=wait_for_log_patterns,
                subprocess_module=subprocess,
            )
        else:
            stream_cmd = build_structured_stream_command(
                python_exe=python_exe,
                config=options.config,
                model=options.model,
                feature_set=options.feature_set,
                run_tag=run_tag,
                load_profile=profile["name"],
                input_run_tag=run_tag,
                reset_checkpoint=True,
                available_now=True,
            )
            run_available_now_sequence(
                run_tag=run_tag,
                replay_cmd=replay_cmd,
                stream_cmd=stream_cmd,
                log_phase_fn=_log_phase,
                run_command_or_raise_fn=run_command_or_raise,
                runtime_log_output_path_fn=runtime_log_output_path,
            )

        metrics_rows = collect_metrics_with_logging(
            run_tag=run_tag,
            timeout_sec=options.metrics_timeout_sec,
            idle_sec=options.metrics_idle_sec,
            run_started_at=run_started_at,
            run_start_timestamp_ms=run_start_timestamp_ms,
            bootstrap_servers=bootstrap_servers,
            metrics_topic=metrics_topic,
            collect_metrics_fn=_wait_metrics,
            log_phase_fn=_log_phase,
        )
        row, timeseries_path = materialize_sut_summary_row(
            run_tag=run_tag,
            metrics_rows=metrics_rows,
            build_legacy_row_fn=lambda materialized_metrics_rows: _to_row(
                run_tag,
                profile,
                materialized_metrics_rows,
                rate_schedule=options.trace_rate_schedule if schedule_pairs else "",
            ),
            write_metrics_timeseries_fn=write_metrics_timeseries,
            annotate_sut_debug_summary_fn=annotate_sut_debug_summary,
        )
        if timeseries_path is not None:
            _log_phase("timeseries_write_done", run_tag=run_tag, path=timeseries_path)
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

    persist_summary_rows(
        summary_csv=options.summary_csv,
        rows=rows,
        resolve_project_path_fn=resolve_project_path,
        write_summary_rows_fn=write_summary_rows,
        label="load quality",
    )
    return 0
