from __future__ import annotations

import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone

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
    summarize_runtime_metrics,
    wait_for_process_startup,
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
    build_run_context,
    collect_metrics_with_logging,
    current_python_executable,
    log_layer_a_run_start,
    parse_trace_and_warmup_schedules,
    resolve_streaming_metrics_context,
    validate_schedule_mode,
)
from ids_platform.streaming.evaluation.matrices.throughput.stream_support import (
    build_structured_stream_command,
    run_available_now_sequence,
    run_available_now_warmup_sequence,
    run_trace_stream_sequence,
    run_trace_warmup_sequence,
)
from ids_platform.streaming.evaluation.matrices.throughput.row_builders import (
    build_layer_a_summary_row,
)


@dataclass(frozen=True)
class LayerAMatrixOptions:
    config: str
    model: str
    feature_set: str
    baseline_mode: str
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
    return build_layer_a_summary_row(
        run_tag=run_tag,
        repeat_index=repeat_index,
        model=model,
        feature_set=feature_set,
        profile=profile,
        metrics_rows=metrics_rows,
        summarize_runtime_metrics_fn=summarize_runtime_metrics,
    )


def _reported_model_label(*, model: str, baseline_mode: str) -> str:
    return "pass_through" if str(baseline_mode).strip().lower() == "pass_through" else model


def run(options: LayerAMatrixOptions) -> int:

    # ── profiles ────────────────────────────────────────
    profiles = _parse_profiles(list(options.profiles))
    repeats = max(int(options.repeats), 1)

    # ── config ──────────────────────────────────────────
    bootstrap_servers, metrics_topic, _cfg, _runtime_cfg, _spark_cfg = resolve_streaming_metrics_context(options.config)
    # ── runtime ────────────────────────────────────────
    python_exe = current_python_executable()


    # ── warmup ──────────────────────────────────────────
    summary_rows: list[dict] = []
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

    # ── run ─────────────────────────────────────────────
    for repeat_index in range(1, repeats + 1):
        for index, profile in enumerate(profiles, start=1):
            run_context = build_run_context(
                run_prefix=options.run_prefix,
                tag_parts=[
                    f"r{repeat_index:02d}",
                    f"{index:02d}",
                    profile["name"],
                ],
            )
            run_tag = run_context.run_tag
            run_started_at = run_context.run_started_at
            run_start_timestamp_ms = run_context.run_start_timestamp_ms
            log_layer_a_run_start(
                log_phase_fn=_log_phase,
                run_tag=run_tag,
                repeat_index=repeat_index,
                run_index=index,
                model=_reported_model_label(model=options.model, baseline_mode=options.baseline_mode),
                feature_set=options.feature_set,
                profile=profile,
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
                    warmup_stream_cmd = build_structured_stream_command(
                        python_exe=python_exe,
                        config=options.config,
                        model=options.model,
                        feature_set=options.feature_set,
                        baseline_mode=options.baseline_mode,
                        run_tag=warmup_tag,
                        load_profile=profile["name"],
                        input_run_tag=warmup_tag,
                        starting_offsets="latest",
                        max_offsets_per_trigger=int(profile["max_offsets_per_trigger"]),
                        shuffle_partitions=int(profile["shuffle_partitions"]),
                        trigger_interval=str(profile.get("trigger_interval", "")),
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
                        baseline_mode=options.baseline_mode,
                        run_tag=warmup_tag,
                        load_profile=profile["name"],
                        input_run_tag=warmup_tag,
                        max_offsets_per_trigger=int(profile["max_offsets_per_trigger"]),
                        shuffle_partitions=int(profile["shuffle_partitions"]),
                        trigger_interval=str(profile.get("trigger_interval", "")),
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
                stream_seconds = estimate_stream_runtime(
                    max_rows=options.max_rows,
                    trace_rows_per_sec=options.trace_rows_per_sec,
                    trace_schedule=trace_schedule,
                    override_seconds=options.trace_stream_run_seconds,
                    max_offsets_per_trigger=int(profile["max_offsets_per_trigger"]),
                    trigger_interval=str(profile.get("trigger_interval", "")),
                )
                stream_cmd = build_structured_stream_command(
                    python_exe=python_exe,
                    config=options.config,
                    model=options.model,
                    feature_set=options.feature_set,
                    baseline_mode=options.baseline_mode,
                    run_tag=run_tag,
                    load_profile=profile["name"],
                    input_run_tag=run_tag,
                    starting_offsets="latest",
                    max_offsets_per_trigger=int(profile["max_offsets_per_trigger"]),
                    shuffle_partitions=int(profile["shuffle_partitions"]),
                    trigger_interval=str(profile.get("trigger_interval", "")),
                    stop_on_input_sentinel=True,
                    run_seconds=stream_seconds,
                    reset_checkpoint=True,
                )
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
                    baseline_mode=options.baseline_mode,
                    run_tag=run_tag,
                    load_profile=profile["name"],
                    input_run_tag=run_tag,
                    max_offsets_per_trigger=int(profile["max_offsets_per_trigger"]),
                    shuffle_partitions=int(profile["shuffle_partitions"]),
                    trigger_interval=str(profile.get("trigger_interval", "")),
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
                collect_metrics_fn=_collect_metrics,
                log_phase_fn=_log_phase,
            )
            summary, timeseries_path = materialize_sut_summary_row(
                run_tag=run_tag,
                metrics_rows=metrics_rows,
                build_legacy_row_fn=lambda materialized_metrics_rows: _aggregate_row(
                    run_tag=run_tag,
                    repeat_index=repeat_index,
                    model=_reported_model_label(model=options.model, baseline_mode=options.baseline_mode),
                    feature_set=options.feature_set,
                    profile=profile,
                    metrics_rows=materialized_metrics_rows,
                ),
                write_metrics_timeseries_fn=write_metrics_timeseries,
                annotate_sut_debug_summary_fn=annotate_sut_debug_summary,
            )
            if timeseries_path is not None:
                _log_phase("timeseries_write_done", run_tag=run_tag, path=timeseries_path)
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

    persist_summary_rows(
        summary_csv=options.summary_csv,
        rows=summary_rows,
        resolve_project_path_fn=resolve_project_path,
        write_summary_rows_fn=write_summary_rows,
        label="Layer A",
    )
    return 0

