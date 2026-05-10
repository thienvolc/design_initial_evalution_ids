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
    build_run_context,
    collect_metrics_with_logging,
    current_python_executable,
    log_layer_b_run_start,
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
    build_layer_b_summary_row,
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
    return build_layer_b_summary_row(
        run_tag=run_tag,
        repeat_index=repeat_index,
        model=model,
        feature_set=feature_set,
        metrics_rows=metrics_rows,
        summarize_runtime_metrics_fn=summarize_runtime_metrics,
    )


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

    bootstrap_servers, metrics_topic, _cfg, runtime_cfg, spark_cfg = resolve_streaming_metrics_context(options.config)
    kafka_cfg = (_cfg.get("kafka") or {})
    max_offsets_per_trigger = int(kafka_cfg.get("max_offsets_per_trigger", 20000) or 20000)
    trigger_interval = str(spark_cfg.get("trigger_interval", "10 seconds") or "10 seconds")
    python_exe = current_python_executable()
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

    run_index = 0
    for repeat_index in range(1, repeats + 1):
        for model, feature_set in model_feature_pairs:
            run_index += 1
            run_context = build_run_context(
                run_prefix=options.run_prefix,
                tag_parts=[
                    f"r{repeat_index:02d}",
                    f"{run_index:02d}",
                    model,
                    feature_set,
                ],
            )
            run_tag = run_context.run_tag
            run_started_at = run_context.run_started_at
            run_start_timestamp_ms = run_context.run_start_timestamp_ms
            log_layer_b_run_start(
                log_phase_fn=_log_phase,
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
                    warmup_stream_cmd = build_structured_stream_command(
                        python_exe=python_exe,
                        config=options.config,
                        model=model,
                        feature_set=feature_set,
                        run_tag=warmup_tag,
                        load_profile=f"{model}:{feature_set}",
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
                        model=model,
                        feature_set=feature_set,
                        run_tag=warmup_tag,
                        load_profile=f"{model}:{feature_set}",
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
                stream_seconds = estimate_stream_runtime(
                    max_rows=options.max_rows,
                    trace_rows_per_sec=options.trace_rows_per_sec,
                    trace_schedule=trace_schedule,
                    override_seconds=options.trace_stream_run_seconds,
                    max_offsets_per_trigger=max_offsets_per_trigger,
                    trigger_interval=trigger_interval,
                )
                stream_cmd = build_structured_stream_command(
                    python_exe=python_exe,
                    config=options.config,
                    model=model,
                    feature_set=feature_set,
                    run_tag=run_tag,
                    load_profile=f"{model}:{feature_set}",
                    input_run_tag=run_tag,
                    starting_offsets="latest",
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
                    model=model,
                    feature_set=feature_set,
                    run_tag=run_tag,
                    load_profile=f"{model}:{feature_set}",
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
                build_legacy_row_fn=lambda materialized_metrics_rows: _flatten_metrics(
                    run_tag,
                    repeat_index,
                    model,
                    feature_set,
                    materialized_metrics_rows,
                ),
                write_metrics_timeseries_fn=write_metrics_timeseries,
                annotate_sut_debug_summary_fn=annotate_sut_debug_summary,
            )
            if timeseries_path is not None:
                _log_phase("timeseries_write_done", run_tag=run_tag, path=timeseries_path)
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

    persist_summary_rows(
        summary_csv=options.summary_csv,
        rows=summary_rows,
        resolve_project_path_fn=resolve_project_path,
        write_summary_rows_fn=write_summary_rows,
        label="Layer B",
    )
    return 0


