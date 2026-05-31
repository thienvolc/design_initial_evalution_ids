from __future__ import annotations

import time

from .logging_utils import _log_phase, _mark_row_failed
from .runtime import (
    SPARK_DRAIN_IDLE_SEC,
    SPARK_DRAIN_MAX_TIMEOUT_SEC,
    SPARK_DRAIN_MIN_TIMEOUT_SEC,
    SPARK_DRAIN_POLL_SEC,
    STREAM_EXIT_WAIT_TIMEOUT_SEC,
    STREAM_SHUTDOWN_POLL_SEC,
    STREAM_SHUTDOWN_SETTLE_SEC,
    STREAM_SHUTDOWN_TIMEOUT_SEC,
)
from .types import LayerCFaultMatrixOptions, LayerCShutdownResult


def _entrypoint_module():
    from ids_platform.streaming.evaluation.matrices import layer_c_fault_matrix

    return layer_c_fault_matrix


def handle_spark_process_restart_fault(
    *,
    options: LayerCFaultMatrixOptions,
    run_tag: str,
    scenario: str,
    row: dict,
    stream_process,
    warmup_rate_schedule: str,
):
    restart_log_path = str(getattr(stream_process, "ids_log_path", "") or "")
    _log_phase("stream_drain_wait_start", run_tag=run_tag, scenario=scenario)
    drained = _entrypoint_module().wait_for_log_quiescence(
        process=stream_process,
        log_path=restart_log_path,
        idle_sec=SPARK_DRAIN_IDLE_SEC,
        timeout_sec=min(max(options.metrics_timeout_sec, SPARK_DRAIN_MIN_TIMEOUT_SEC), SPARK_DRAIN_MAX_TIMEOUT_SEC),
        poll_seconds=SPARK_DRAIN_POLL_SEC,
    )
    if not drained:
        _mark_row_failed(
            row,
            f"stream did not quiesce before restart; log={restart_log_path}"
            if restart_log_path else "stream did not quiesce before restart",
        )
        _log_phase("stream_drain_wait_timeout", run_tag=run_tag, scenario=scenario, notes=row["notes"])
        return None

    _log_phase("stream_drain_wait_done", run_tag=run_tag, scenario=scenario)
    _entrypoint_module().stop_stream_process(stream_process)
    _log_phase("fault_inject_done", run_tag=run_tag, scenario=scenario, action="stop_stream_process")
    _log_phase("stream_restart_start", run_tag=run_tag, scenario=scenario)

    restarted_process = _entrypoint_module().start_stream_process(
        config=options.config,
        model=options.model,
        feature_set=options.feature_set,
        run_tag=run_tag,
        load_profile=scenario,
        reset_checkpoint=False,
        execution_mode=options.execution_mode,
        python_executable=options.python_executable,
        bootstrap_servers=options.bootstrap_servers,
    )
    _log_phase(
        "stream_runtime_log",
        run_tag=run_tag,
        scenario=scenario,
        path=str(getattr(restarted_process, "ids_log_path", "") or ""),
    )
    if not _entrypoint_module().wait_for_process_startup(
        restarted_process,
        startup_wait_sec=options.startup_wait_sec,
        ready_log_path=str(getattr(restarted_process, "ids_log_path", "") or ""),
        ready_pattern="",
        require_ready_marker=False,
    ):
        log_path = str(getattr(restarted_process, "ids_log_path", "") or "")
        startup_debug = _entrypoint_module().describe_process_startup_state(
            restarted_process,
            log_path=log_path,
            ready_pattern="",
        )
        _mark_row_failed(
            row,
            f"stream process not ready after restart; log={log_path}; {startup_debug}"
            if log_path else "stream process not ready after restart",
        )
        _log_phase("stream_restart_failed", run_tag=run_tag, scenario=scenario, notes=row["notes"])
        return None

    _log_phase("stream_restart_done", run_tag=run_tag, scenario=scenario)
    return restarted_process


def await_expected_graceful_shutdown(
    *,
    options: LayerCFaultMatrixOptions,
    run_tag: str,
    scenario: str,
    stream_process,
    expect_natural_shutdown: bool,
) -> LayerCShutdownResult:
    graceful_shutdown_complete = False
    if not expect_natural_shutdown or stream_process is None or stream_process.poll() is not None:
        return LayerCShutdownResult(
            graceful_shutdown_complete=False,
            stream_process=stream_process,
        )

    log_path = str(getattr(stream_process, "ids_log_path", "") or "")
    graceful_shutdown_complete = _entrypoint_module().wait_for_stream_shutdown(
        run_tag=run_tag,
        execution_mode=options.execution_mode,
        timeout_sec=STREAM_SHUTDOWN_TIMEOUT_SEC,
        poll_sec=STREAM_SHUTDOWN_POLL_SEC,
        settle_sec=STREAM_SHUTDOWN_SETTLE_SEC,
    )
    if graceful_shutdown_complete and _entrypoint_module().wait_for_process_exit(
        stream_process,
        timeout_sec=STREAM_EXIT_WAIT_TIMEOUT_SEC,
    ):
        _entrypoint_module().stop_stream_process(stream_process)
        stream_process = None

    return LayerCShutdownResult(
        graceful_shutdown_complete=graceful_shutdown_complete,
        stream_process=stream_process,
    )


def ensure_stream_process_stopped(
    *,
    stream_process,
    graceful_shutdown_complete: bool,
) -> None:
    if stream_process is None:
        return
    if stream_process.poll() is not None:
        _entrypoint_module().stop_stream_process(stream_process)
        return
    if not graceful_shutdown_complete:
        _entrypoint_module().stop_stream_process(stream_process)


def finalize_scenario_cleanup(
    *,
    options: LayerCFaultMatrixOptions,
    run_tag: str,
    scenario: str,
    scenario_started_at: float,
    stream_process,
    expect_natural_shutdown: bool,
) -> None:
    shutdown_result = await_expected_graceful_shutdown(
        options=options,
        run_tag=run_tag,
        scenario=scenario,
        stream_process=stream_process,
        expect_natural_shutdown=expect_natural_shutdown,
    )
    ensure_stream_process_stopped(
        stream_process=shutdown_result.stream_process,
        graceful_shutdown_complete=shutdown_result.graceful_shutdown_complete,
    )
    _log_phase("scenario_cleanup_done", run_tag=run_tag, scenario=scenario, elapsed_sec=f"{time.time() - scenario_started_at:.2f}")


def start_layer_c_stream(
    *,
    options: LayerCFaultMatrixOptions,
    run_tag: str,
    scenario: str,
    warmup_rate_schedule: str,
):
    _log_phase("stream_start", run_tag=run_tag, scenario=scenario, reset_checkpoint=True)
    stream_process = _entrypoint_module().start_stream_process(
        config=options.config,
        model=options.model,
        feature_set=options.feature_set,
        run_tag=run_tag,
        load_profile=scenario,
        reset_checkpoint=True,
        execution_mode=options.execution_mode,
        python_executable=options.python_executable,
        bootstrap_servers=options.bootstrap_servers,
    )
    _log_phase(
        "stream_runtime_log",
        run_tag=run_tag,
        scenario=scenario,
        path=str(getattr(stream_process, "ids_log_path", "") or ""),
    )
    return stream_process


def ensure_stream_started_for_warmup(
    *,
    options: LayerCFaultMatrixOptions,
    run_tag: str,
    scenario: str,
    row: dict,
    stream_process,
) -> bool:
    log_path = str(getattr(stream_process, "ids_log_path", "") or "")
    if _entrypoint_module().wait_for_process_startup(
        stream_process,
        startup_wait_sec=options.startup_wait_sec,
        ready_log_path=log_path,
        ready_pattern="",
        require_ready_marker=False,
    ):
        return True

    startup_debug = _entrypoint_module().describe_process_startup_state(
        stream_process,
        log_path=log_path,
        ready_pattern="",
    )
    _mark_row_failed(
        row,
        f"stream process not ready before warmup; log={log_path}; {startup_debug}"
        if log_path else "stream process not ready before warmup",
    )
    _log_phase("stream_start_failed", run_tag=run_tag, scenario=scenario, notes=row["notes"])
    return False
