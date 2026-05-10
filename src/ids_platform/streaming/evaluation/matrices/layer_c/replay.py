from __future__ import annotations

import time

from .logging_utils import _log_phase, _mark_row_failed
from .runtime import validate_runtime_metric
from .types import LayerCFaultMatrixOptions


def _entrypoint_module():
    from ids_platform.streaming.evaluation.matrices import layer_c_fault_matrix

    return layer_c_fault_matrix


def run_post_fault_replay(
    *,
    options: LayerCFaultMatrixOptions,
    run_tag: str,
    rows: int,
    rows_per_sec: float,
    emit_input_sentinel: bool,
) -> None:
    _entrypoint_module().replay_with_retries(
        config=options.config,
        run_tag=run_tag,
        rows=rows,
        batch_size=options.batch_size,
        input_parquet=options.trace_input_parquet,
        trace_order_column=options.trace_order_column,
        rows_per_sec=rows_per_sec,
        retries=options.replay_retries,
        retry_wait_sec=options.replay_retry_wait_sec,
        emit_input_sentinel=emit_input_sentinel,
        execution_mode=options.execution_mode,
        python_executable=options.python_executable,
        bootstrap_servers=options.bootstrap_servers,
    )


def handle_producer_restart_replay(
    *,
    options: LayerCFaultMatrixOptions,
    run_tag: str,
    scenario: str,
) -> bool:
    first_rows = max(options.post_fault_rows // 2, 1)
    second_rows = max(options.post_fault_rows - first_rows, 1)
    replay_rows_per_sec = max(float(options.post_fault_rows_per_sec), 0.0)

    _log_phase("post_fault_replay_start", run_tag=run_tag, scenario=scenario, rows=first_rows, segment="first")
    run_post_fault_replay(
        options=options,
        run_tag=run_tag,
        rows=first_rows,
        rows_per_sec=replay_rows_per_sec,
        emit_input_sentinel=False,
    )
    _log_phase("post_fault_replay_done", run_tag=run_tag, scenario=scenario, rows=first_rows, segment="first")

    pause_sec = max(options.producer_restart_pause_sec, 1)
    time.sleep(pause_sec)
    _log_phase("producer_pause_done", run_tag=run_tag, scenario=scenario, pause_sec=pause_sec)

    _log_phase("post_fault_replay_start", run_tag=run_tag, scenario=scenario, rows=second_rows, segment="second")
    run_post_fault_replay(
        options=options,
        run_tag=run_tag,
        rows=second_rows,
        rows_per_sec=replay_rows_per_sec,
        emit_input_sentinel=True,
    )
    _log_phase("post_fault_replay_done", run_tag=run_tag, scenario=scenario, rows=second_rows, segment="second")
    return True


def run_warmup_and_validate_metric(
    *,
    options: LayerCFaultMatrixOptions,
    run_tag: str,
    scenario: str,
    row: dict,
    warmup_rate_schedule: str,
) -> bool:
    _log_phase("warmup_replay_start", run_tag=run_tag, scenario=scenario, warmup_rows=options.warmup_rows)
    _entrypoint_module().replay_with_retries(
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
        emit_input_sentinel=False,
        execution_mode=options.execution_mode,
        python_executable=options.python_executable,
        bootstrap_servers=options.bootstrap_servers,
    )
    _log_phase("warmup_replay_done", run_tag=run_tag, scenario=scenario)

    _log_phase("warmup_metric_wait_start", run_tag=run_tag, scenario=scenario, timeout_sec=options.metrics_timeout_sec)
    warmup_metric = _entrypoint_module().fetch_metric(
        config=options.config,
        run_tag=run_tag,
        timeout_sec=options.metrics_timeout_sec,
        execution_mode=options.execution_mode,
        python_executable=options.python_executable,
        bootstrap_servers=options.bootstrap_servers,
    )
    if warmup_metric is None:
        _mark_row_failed(row, "warmup metric not found")
        _log_phase("warmup_metric_missing", run_tag=run_tag, scenario=scenario)
        return False

    warmup_metric_ok, warmup_metric_reason = validate_runtime_metric(warmup_metric)
    if not warmup_metric_ok:
        _mark_row_failed(row, f"warmup metric invalid: {warmup_metric_reason}")
        _log_phase("warmup_metric_invalid", run_tag=run_tag, scenario=scenario, notes=row["notes"])
        return False

    _log_phase("warmup_metric_found", run_tag=run_tag, scenario=scenario)
    return True


def run_scenario_post_fault_replay(
    *,
    options: LayerCFaultMatrixOptions,
    run_tag: str,
    scenario: str,
) -> bool:
    if scenario == "producer_restart":
        return handle_producer_restart_replay(
            options=options,
            run_tag=run_tag,
            scenario=scenario,
        )

    replay_rows_per_sec = max(float(options.post_fault_rows_per_sec), 0.0)
    if scenario == "network_slowdown":
        replay_rows_per_sec = max(float(options.slowdown_rows_per_sec), 0.0)

    _log_phase(
        "post_fault_replay_start",
        run_tag=run_tag,
        scenario=scenario,
        rows=options.post_fault_rows,
        rows_per_sec=replay_rows_per_sec,
    )
    run_post_fault_replay(
        options=options,
        run_tag=run_tag,
        rows=options.post_fault_rows,
        rows_per_sec=replay_rows_per_sec,
        emit_input_sentinel=True,
    )
    _log_phase("post_fault_replay_done", run_tag=run_tag, scenario=scenario, rows=options.post_fault_rows)
    return True
