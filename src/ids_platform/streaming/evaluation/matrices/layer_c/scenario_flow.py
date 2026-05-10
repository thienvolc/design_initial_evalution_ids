from __future__ import annotations

import time
from datetime import datetime, timezone

from ids_platform.common.paths import resolve_project_path
from ids_platform.streaming.evaluation.matrices.common import (
    write_metrics_timeseries,
    write_summary_rows,
)
from ids_platform.streaming.evaluation.orchestration.fault_matrix import (
    cleanup_stream_processes,
    normalize_rate_schedule,
    scenario_row_template,
)

from .kafka_restart import handle_kafka_restart_fault
from .logging_utils import (
    _finalize_scenario_row,
    _log_phase,
    _mark_row_failed,
    _next_run_tag,
)
from .recovery import collect_scenario_metrics, wait_for_post_fault_recovery
from .replay import run_scenario_post_fault_replay, run_warmup_and_validate_metric
from .stream_lifecycle import (
    ensure_stream_started_for_warmup,
    finalize_scenario_cleanup,
    handle_spark_process_restart_fault,
    start_layer_c_stream,
)
from .types import (
    LayerCFaultInjectionResult,
    LayerCFaultMatrixOptions,
    LayerCPreRecoveryResult,
)


def scenario_requires_real_network_fault(scenario: str) -> bool:
    return str(scenario).strip().lower() == "network_slowdown"


def inject_scenario_fault(
    *,
    options: LayerCFaultMatrixOptions,
    run_tag: str,
    scenario: str,
    row: dict,
    stream_process,
    warmup_rate_schedule: str,
) -> LayerCFaultInjectionResult:
    if scenario == "kafka_restart":
        if not handle_kafka_restart_fault(
            options=options,
            run_tag=run_tag,
            scenario=scenario,
            row=row,
        ):
            return LayerCFaultInjectionResult(ok=False, stream_process=stream_process)
        return LayerCFaultInjectionResult(ok=True, stream_process=stream_process)

    if scenario == "spark_process_restart":
        restarted_process = handle_spark_process_restart_fault(
            options=options,
            run_tag=run_tag,
            scenario=scenario,
            row=row,
            stream_process=stream_process,
            warmup_rate_schedule=warmup_rate_schedule,
        )
        return LayerCFaultInjectionResult(
            ok=restarted_process is not None,
            stream_process=restarted_process,
        )

    if scenario == "producer_restart":
        _log_phase("fault_inject_done", run_tag=run_tag, scenario=scenario, action=scenario)
        return LayerCFaultInjectionResult(ok=True, stream_process=stream_process)

    if scenario == "network_slowdown":
        _log_phase(
            "fault_inject_done",
            run_tag=run_tag,
            scenario=scenario,
            action="simulate_network_slowdown_via_replay_throttle",
            slowdown_rows_per_sec=max(float(options.slowdown_rows_per_sec), 0.0),
        )
        return LayerCFaultInjectionResult(ok=True, stream_process=stream_process)

    _mark_row_failed(row, f"unsupported scenario={scenario}")
    _log_phase("fault_inject_unsupported", run_tag=run_tag, scenario=scenario, notes=row["notes"])
    return LayerCFaultInjectionResult(ok=False, stream_process=stream_process)


def prepare_scenario_for_recovery(
    *,
    options: LayerCFaultMatrixOptions,
    run_tag: str,
    scenario: str,
    row: dict,
    warmup_rate_schedule: str,
) -> LayerCPreRecoveryResult:
    cleanup_stream_processes(run_tag=run_tag, execution_mode=options.execution_mode)
    time.sleep(2)

    stream_process = start_layer_c_stream(
        options=options,
        run_tag=run_tag,
        scenario=scenario,
        warmup_rate_schedule=warmup_rate_schedule,
    )
    if not ensure_stream_started_for_warmup(
        options=options,
        run_tag=run_tag,
        scenario=scenario,
        row=row,
        stream_process=stream_process,
    ):
        return LayerCPreRecoveryResult(
            ok=False,
            stream_process=stream_process,
            fault_time=None,
            fault_epoch_ms=0,
        )

    if not run_warmup_and_validate_metric(
        options=options,
        run_tag=run_tag,
        scenario=scenario,
        row=row,
        warmup_rate_schedule=warmup_rate_schedule,
    ):
        return LayerCPreRecoveryResult(
            ok=False,
            stream_process=stream_process,
            fault_time=None,
            fault_epoch_ms=0,
        )

    if options.fault_delay_sec > 0:
        _log_phase("fault_delay_wait_start", run_tag=run_tag, scenario=scenario, fault_delay_sec=options.fault_delay_sec)
        time.sleep(max(int(options.fault_delay_sec), 1))

    fault_time = datetime.now(timezone.utc)
    fault_epoch_ms = int(time.time() * 1000)
    row["fault_ts_utc"] = fault_time.isoformat(timespec="seconds")
    _log_phase("fault_inject_start", run_tag=run_tag, scenario=scenario, fault_ts_utc=row["fault_ts_utc"])

    fault_result = inject_scenario_fault(
        options=options,
        run_tag=run_tag,
        scenario=scenario,
        row=row,
        stream_process=stream_process,
        warmup_rate_schedule=warmup_rate_schedule,
    )
    stream_process = fault_result.stream_process
    if not fault_result.ok:
        return LayerCPreRecoveryResult(
            ok=False,
            stream_process=stream_process,
            fault_time=fault_time,
            fault_epoch_ms=fault_epoch_ms,
        )

    if stream_process.poll() is not None:
        _mark_row_failed(row, "stream process exited after fault")
        _log_phase("stream_exited_after_fault", run_tag=run_tag, scenario=scenario, notes=row["notes"])
        return LayerCPreRecoveryResult(
            ok=False,
            stream_process=stream_process,
            fault_time=fault_time,
            fault_epoch_ms=fault_epoch_ms,
        )

    return LayerCPreRecoveryResult(
        ok=True,
        stream_process=stream_process,
        fault_time=fault_time,
        fault_epoch_ms=fault_epoch_ms,
    )


def execute_scenario(
    options: LayerCFaultMatrixOptions,
    scenario: str,
    index: int,
    warmup_rate_schedule: str,
) -> dict:
    scenario_started_at = time.time()
    run_tag = _next_run_tag(index=index, scenario=scenario)
    _log_phase("scenario_start", run_tag=run_tag, scenario=scenario, run_index=index)
    row = scenario_row_template(
        run_tag=run_tag,
        scenario=scenario,
        model=options.model,
        feature_set=options.feature_set,
    )

    stream_process = None
    expect_natural_shutdown = False
    try:
        pre_recovery = prepare_scenario_for_recovery(
            options=options,
            run_tag=run_tag,
            scenario=scenario,
            row=row,
            warmup_rate_schedule=warmup_rate_schedule,
        )
        stream_process = pre_recovery.stream_process
        if not pre_recovery.ok or pre_recovery.fault_time is None:
            return _finalize_scenario_row(row)

        run_scenario_post_fault_replay(
            options=options,
            run_tag=run_tag,
            scenario=scenario,
        )
        expect_natural_shutdown = True

        if not wait_for_post_fault_recovery(
            options=options,
            run_tag=run_tag,
            scenario=scenario,
            row=row,
            fault_time=pre_recovery.fault_time,
            fault_epoch_ms=pre_recovery.fault_epoch_ms,
        ):
            return _finalize_scenario_row(row)
        _log_phase(
            "scenario_summary",
            run_tag=run_tag,
            scenario=scenario,
            status=row["status"],
            recovery_seconds=row["recovery_seconds"],
            rows_after_fault=row["rows_after_fault"],
            elapsed_sec=f"{time.time() - scenario_started_at:.2f}",
        )
        return _finalize_scenario_row(row)
    except Exception as exc:
        _mark_row_failed(row, str(exc))
        _log_phase(
            "scenario_error",
            run_tag=run_tag,
            scenario=scenario,
            error=type(exc).__name__,
            elapsed_sec=f"{time.time() - scenario_started_at:.2f}",
        )
        return _finalize_scenario_row(row)
    finally:
        finalize_scenario_cleanup(
            options=options,
            run_tag=run_tag,
            scenario=scenario,
            scenario_started_at=scenario_started_at,
            stream_process=stream_process,
            expect_natural_shutdown=expect_natural_shutdown,
        )


def run_layer_c_fault_matrix(options: LayerCFaultMatrixOptions) -> int:
    warmup_rate_schedule = normalize_rate_schedule(options.warmup_rate_schedule)
    if warmup_rate_schedule and options.warmup_rows_per_sec > 0:
        raise ValueError("Use either --warmup-rows-per-sec or --warmup-rate-schedule, not both")

    summary_rows: list[dict] = []
    for index, scenario in enumerate(options.scenarios, start=1):
        scenario_run_start_ms = int(time.time() * 1000)
        row = execute_scenario(
            options,
            scenario,
            index,
            warmup_rate_schedule,
        )
        metrics_rows = collect_scenario_metrics(
            config=options.config,
            run_tag=str(row["run_tag"]),
            timeout_sec=options.metrics_timeout_sec,
            start_timestamp_ms=scenario_run_start_ms,
            bootstrap_servers_override=options.bootstrap_servers,
        )
        timeseries_path = write_metrics_timeseries(metrics_rows, run_tag=str(row["run_tag"]))
        if timeseries_path is not None:
            _log_phase("timeseries_write_done", run_tag=row["run_tag"], scenario=scenario, path=timeseries_path)
        summary_rows.append(row)
        _log_phase("run_summary", run_tag=row["run_tag"], scenario=scenario, status=row["status"], recovery_seconds=row["recovery_seconds"], notes=row["notes"])

    summary_path = resolve_project_path(options.summary_csv)
    write_summary_rows(summary_path, summary_rows)
    print(f"Saved Layer C summary: {summary_path}", flush=True)
    return 0
