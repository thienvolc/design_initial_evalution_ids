from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone

from ids_platform.common.paths import resolve_project_path
from ids_platform.streaming.matrices.common import annotate_sut_debug_summary, wait_for_process_startup, write_summary_rows
from ids_platform.streaming.orchestration.fault_matrix import (
    cleanup_stream_processes,
    fetch_metric,
    normalize_rate_schedule,
    parse_iso_datetime,
    replay_with_retries,
    restart_service,
    safe_float,
    scenario_row_template,
    start_stream_process,
    stop_stream_process,
)


@dataclass(frozen=True)
class LayerCFaultMatrixOptions:
    config: str
    model: str
    feature_set: str
    scenarios: tuple[str, ...]
    warmup_rows: int
    warmup_rows_per_sec: float
    warmup_rate_schedule: str
    fault_delay_sec: int
    post_fault_rows: int
    post_fault_rows_per_sec: float
    batch_size: int
    trace_input_parquet: str
    trace_order_column: str
    slowdown_rows_per_sec: float
    producer_restart_pause_sec: int
    replay_retries: int
    replay_retry_wait_sec: int
    stream_run_seconds: int
    startup_wait_sec: int
    metrics_timeout_sec: int
    summary_csv: str


def _log_phase(event: str, **fields) -> None:
    parts = [f"[phase] matrix=layer_c event={event}"]
    for key, value in fields.items():
        if value is None:
            continue
        parts.append(f"{key}={value}")
    print(" ".join(parts), flush=True)


def _execute_scenario(
    options: LayerCFaultMatrixOptions,
    scenario: str,
    index: int,
    warmup_rate_schedule: str,
) -> dict:
    scenario_started_at = time.time()
    run_tag = f"layerC_{index:02d}_{scenario}_{int(time.time())}"
    _log_phase("scenario_start", run_tag=run_tag, scenario=scenario, run_index=index)
    row = scenario_row_template(
        run_tag=run_tag,
        scenario=scenario,
        model=options.model,
        feature_set=options.feature_set,
    )

    stream_process = None
    try:
        cleanup_stream_processes(run_tag=run_tag)
        time.sleep(2)

        _log_phase("stream_start", run_tag=run_tag, scenario=scenario, reset_checkpoint=True)
        stream_process = start_stream_process(
            config=options.config,
            model=options.model,
            feature_set=options.feature_set,
            run_tag=run_tag,
            run_seconds=options.stream_run_seconds,
            reset_checkpoint=True,
        )
        if not wait_for_process_startup(
            stream_process,
            startup_wait_sec=options.startup_wait_sec,
        ):
            row["notes"] = "stream process exited before warmup"
            _log_phase("stream_start_failed", run_tag=run_tag, scenario=scenario, notes=row["notes"])
            return annotate_sut_debug_summary(row)

        _log_phase("warmup_replay_start", run_tag=run_tag, scenario=scenario, warmup_rows=options.warmup_rows)
        replay_with_retries(
            config=options.config,
            run_tag=run_tag,
            rows=options.warmup_rows,
            batch_size=options.batch_size,
            input_parquet=options.trace_input_parquet,
            trace_order_column=options.trace_order_column,
            rows_per_sec=max(float(options.warmup_rows_per_sec), 0.0),
            rate_schedule=warmup_rate_schedule,
            retries=options.replay_retries,
            retry_wait_sec=options.replay_retry_wait_sec,
        )
        _log_phase("warmup_replay_done", run_tag=run_tag, scenario=scenario)

        _log_phase("warmup_metric_wait_start", run_tag=run_tag, scenario=scenario, timeout_sec=options.metrics_timeout_sec)
        warmup_metric = fetch_metric(
            config=options.config,
            run_tag=run_tag,
            timeout_sec=options.metrics_timeout_sec,
        )
        if warmup_metric is None:
            row["notes"] = "warmup metric not found"
            _log_phase("warmup_metric_missing", run_tag=run_tag, scenario=scenario)
            return annotate_sut_debug_summary(row)
        _log_phase("warmup_metric_found", run_tag=run_tag, scenario=scenario)

        if options.fault_delay_sec > 0:
            _log_phase("fault_delay_wait_start", run_tag=run_tag, scenario=scenario, fault_delay_sec=options.fault_delay_sec)
            time.sleep(max(int(options.fault_delay_sec), 1))

        fault_time = datetime.now(timezone.utc)
        fault_epoch_ms = int(time.time() * 1000)
        row["fault_ts_utc"] = fault_time.isoformat(timespec="seconds")
        _log_phase("fault_inject_start", run_tag=run_tag, scenario=scenario, fault_ts_utc=row["fault_ts_utc"])

        if scenario == "kafka_restart":
            restart_service("kafka")
            _log_phase("fault_inject_done", run_tag=run_tag, scenario=scenario, action="restart_service:kafka")
        elif scenario == "spark_process_restart":
            stop_stream_process(stream_process)
            _log_phase("fault_inject_done", run_tag=run_tag, scenario=scenario, action="stop_stream_process")
            _log_phase("stream_restart_start", run_tag=run_tag, scenario=scenario)
            stream_process = start_stream_process(
                config=options.config,
                model=options.model,
                feature_set=options.feature_set,
                run_tag=run_tag,
                run_seconds=options.stream_run_seconds,
                reset_checkpoint=False,
            )
            if not wait_for_process_startup(
                stream_process,
                startup_wait_sec=options.startup_wait_sec,
            ):
                row["notes"] = "stream process exited after restart"
                _log_phase("stream_restart_failed", run_tag=run_tag, scenario=scenario, notes=row["notes"])
                return annotate_sut_debug_summary(row)
            _log_phase("stream_restart_done", run_tag=run_tag, scenario=scenario)
        elif scenario not in {"producer_restart", "network_slowdown"}:
            row["notes"] = f"unsupported scenario={scenario}"
            _log_phase("fault_inject_unsupported", run_tag=run_tag, scenario=scenario, notes=row["notes"])
            return annotate_sut_debug_summary(row)
        else:
            _log_phase("fault_inject_done", run_tag=run_tag, scenario=scenario, action=scenario)

        if stream_process.poll() is not None:
            row["notes"] = "stream process exited after fault"
            _log_phase("stream_exited_after_fault", run_tag=run_tag, scenario=scenario, notes=row["notes"])
            return annotate_sut_debug_summary(row)

        if scenario == "producer_restart":
            first_rows = max(options.post_fault_rows // 2, 1)
            second_rows = max(options.post_fault_rows - first_rows, 1)
            _log_phase("post_fault_replay_start", run_tag=run_tag, scenario=scenario, rows=first_rows, segment="first")
            replay_with_retries(
                config=options.config,
                run_tag=run_tag,
                rows=first_rows,
                batch_size=options.batch_size,
                input_parquet=options.trace_input_parquet,
                trace_order_column=options.trace_order_column,
                rows_per_sec=max(float(options.post_fault_rows_per_sec), 0.0),
                retries=options.replay_retries,
                retry_wait_sec=options.replay_retry_wait_sec,
            )
            _log_phase("post_fault_replay_done", run_tag=run_tag, scenario=scenario, rows=first_rows, segment="first")
            time.sleep(max(options.producer_restart_pause_sec, 1))
            _log_phase("producer_pause_done", run_tag=run_tag, scenario=scenario, pause_sec=max(options.producer_restart_pause_sec, 1))
            _log_phase("post_fault_replay_start", run_tag=run_tag, scenario=scenario, rows=second_rows, segment="second")
            replay_with_retries(
                config=options.config,
                run_tag=run_tag,
                rows=second_rows,
                batch_size=options.batch_size,
                input_parquet=options.trace_input_parquet,
                trace_order_column=options.trace_order_column,
                rows_per_sec=max(float(options.post_fault_rows_per_sec), 0.0),
                retries=options.replay_retries,
                retry_wait_sec=options.replay_retry_wait_sec,
            )
            _log_phase("post_fault_replay_done", run_tag=run_tag, scenario=scenario, rows=second_rows, segment="second")
        elif scenario == "network_slowdown":
            _log_phase("post_fault_replay_start", run_tag=run_tag, scenario=scenario, rows=options.post_fault_rows, rows_per_sec=max(float(options.slowdown_rows_per_sec), 1.0))
            replay_with_retries(
                config=options.config,
                run_tag=run_tag,
                rows=options.post_fault_rows,
                batch_size=options.batch_size,
                input_parquet=options.trace_input_parquet,
                trace_order_column=options.trace_order_column,
                rows_per_sec=max(float(options.slowdown_rows_per_sec), 1.0),
                retries=options.replay_retries,
                retry_wait_sec=options.replay_retry_wait_sec,
            )
            _log_phase("post_fault_replay_done", run_tag=run_tag, scenario=scenario, rows=options.post_fault_rows)
        else:
            _log_phase("post_fault_replay_start", run_tag=run_tag, scenario=scenario, rows=options.post_fault_rows, rows_per_sec=max(float(options.post_fault_rows_per_sec), 0.0))
            replay_with_retries(
                config=options.config,
                run_tag=run_tag,
                rows=options.post_fault_rows,
                batch_size=options.batch_size,
                input_parquet=options.trace_input_parquet,
                trace_order_column=options.trace_order_column,
                rows_per_sec=max(float(options.post_fault_rows_per_sec), 0.0),
                retries=options.replay_retries,
                retry_wait_sec=options.replay_retry_wait_sec,
            )
            _log_phase("post_fault_replay_done", run_tag=run_tag, scenario=scenario, rows=options.post_fault_rows)

        _log_phase("recovery_wait_start", run_tag=run_tag, scenario=scenario, timeout_sec=options.metrics_timeout_sec)
        recovered_metric = fetch_metric(
            config=options.config,
            run_tag=run_tag,
            timeout_sec=options.metrics_timeout_sec,
            after_ts_utc=row["fault_ts_utc"],
            after_epoch_ms=fault_epoch_ms,
        )
        if recovered_metric is None:
            row["notes"] = "post-fault metric not found"
            _log_phase("recovery_wait_timeout", run_tag=run_tag, scenario=scenario, notes=row["notes"])
            return annotate_sut_debug_summary(row)
        _log_phase("recovery_wait_done", run_tag=run_tag, scenario=scenario)

        recover_ts_utc = str(recovered_metric.get("ts_utc", ""))
        if recover_ts_utc:
            recover_time = parse_iso_datetime(recover_ts_utc)
            row["recover_ts_utc"] = recover_ts_utc
            row["recovery_seconds"] = max((recover_time - fault_time).total_seconds(), 0.0)

        row["rows_after_fault"] = int(recovered_metric.get("rows") or 0)
        row["rows_per_sec_after_fault"] = safe_float(recovered_metric.get("rows_per_sec"))

        latency = recovered_metric.get("latency_ms") or {}
        processing = latency.get("processing") or {}
        end_to_end = latency.get("end_to_end") or {}
        row["proc_p95_ms_after_fault"] = safe_float(processing.get("p95"))
        row["e2e_p95_ms_after_fault"] = safe_float(end_to_end.get("p95"))
        row["status"] = "ok"
        _log_phase(
            "scenario_summary",
            run_tag=run_tag,
            scenario=scenario,
            status=row["status"],
            recovery_seconds=row["recovery_seconds"],
            rows_after_fault=row["rows_after_fault"],
            elapsed_sec=f"{time.time() - scenario_started_at:.2f}",
        )
        return annotate_sut_debug_summary(row)
    except Exception as exc:
        row["notes"] = str(exc)
        _log_phase(
            "scenario_error",
            run_tag=run_tag,
            scenario=scenario,
            error=type(exc).__name__,
            elapsed_sec=f"{time.time() - scenario_started_at:.2f}",
        )
        return annotate_sut_debug_summary(row)
    finally:
        stop_stream_process(stream_process)
        cleanup_stream_processes(run_tag=run_tag)
        _log_phase("scenario_cleanup_done", run_tag=run_tag, scenario=scenario, elapsed_sec=f"{time.time() - scenario_started_at:.2f}")


def run(options: LayerCFaultMatrixOptions) -> int:
    warmup_rate_schedule = normalize_rate_schedule(options.warmup_rate_schedule)
    if warmup_rate_schedule and options.warmup_rows_per_sec > 0:
        raise ValueError("Use either --warmup-rows-per-sec or --warmup-rate-schedule, not both")

    summary_rows: list[dict] = []
    for index, scenario in enumerate(options.scenarios, start=1):
        row = _execute_scenario(
            options,
            scenario,
            index,
            warmup_rate_schedule,
        )
        summary_rows.append(row)
        _log_phase("run_summary", run_tag=row["run_tag"], scenario=scenario, status=row["status"], recovery_seconds=row["recovery_seconds"], notes=row["notes"])

    summary_path = resolve_project_path(options.summary_csv)
    write_summary_rows(summary_path, summary_rows)
    print(f"Saved Layer C summary: {summary_path}", flush=True)
    return 0
