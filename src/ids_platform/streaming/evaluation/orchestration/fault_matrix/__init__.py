from __future__ import annotations

import time

from ids_platform.common.paths import PROJECT_ROOT
from ids_platform.common.subprocess import (
    run_command,
    run_command_or_raise,
    start_background_process,
    stop_background_process,
)
from ids_platform.streaming.evaluation.orchestration.fault_matrix.kafka import (
    docker_describe_kafka_topics_state,
    docker_wait_for_kafka_bootstrap_ready,
    docker_wait_for_kafka_topics_ready,
    host_describe_kafka_topics_state,
    host_wait_for_kafka_bootstrap_ready,
    host_wait_for_kafka_topics_ready,
)
from ids_platform.streaming.evaluation.orchestration.fault_matrix.process import (
    cleanup_docker_stream_processes,
    cleanup_host_stream_processes,
    iter_docker_stream_processes,
    iter_host_stream_processes,
    wait_for_process_exit as process_wait_for_process_exit,
    wait_for_stream_shutdown as process_wait_for_stream_shutdown,
)
from ids_platform.streaming.evaluation.orchestration.fault_matrix.runtime import (
    fetch_metric as runtime_fetch_metric,
    replay_with_retries as runtime_replay_with_retries,
    start_stream_process as runtime_start_stream_process,
)
from ids_platform.streaming.evaluation.orchestration.fault_matrix.services import (
    describe_kafka_topics_state as services_describe_kafka_topics_state,
    restart_service as services_restart_service,
    wait_for_kafka_bootstrap_ready as services_wait_for_kafka_bootstrap_ready,
    wait_for_kafka_topics_ready as services_wait_for_kafka_topics_ready,
)
from ids_platform.streaming.evaluation.orchestration.fault_matrix.shutdown import (
    stop_stream_process as shutdown_stop_stream_process,
)
from ids_platform.streaming.evaluation.orchestration.fault_matrix.support import (
    build_stream_command as support_build_stream_command,
    child_env as support_child_env,
    normalized_execution_mode as support_normalized_execution_mode,
    parse_iso_datetime,
    safe_float,
    scenario_row_template,
    script_command as support_script_command,
)
from ids_platform.streaming.replay.config import parse_rate_schedule
from ids_platform.streaming.runtime.control import write_shutdown_request


def normalize_rate_schedule(raw: str) -> str:
    text = (raw or "").strip()
    if not text:
        return ""
    parse_rate_schedule(text, error_message="warmup rate schedule must be rps:seconds,rps:seconds")
    return text


def _normalized_execution_mode(execution_mode: str) -> str:
    return support_normalized_execution_mode(execution_mode)


def _child_env(*, execution_mode: str, bootstrap_servers: str = "") -> dict[str, str]:
    return support_child_env(execution_mode=execution_mode, bootstrap_servers=bootstrap_servers)


def _script_command(
    *,
    execution_mode: str,
    python_executable: str,
    script_path: str,
    script_args: list[str],
) -> list[str]:
    return support_script_command(
        execution_mode=execution_mode,
        python_executable=python_executable,
        script_path=script_path,
        script_args=script_args,
    )


def build_stream_command(
    *,
    config: str,
    model: str,
    feature_set: str,
    run_tag: str,
    load_profile: str,
    run_seconds: int,
    reset_checkpoint: bool,
    stop_on_input_sentinel: bool = False,
    execution_mode: str = "host",
    python_executable: str = "python",
) -> list[str]:
    return support_build_stream_command(
        config=config,
        model=model,
        feature_set=feature_set,
        run_tag=run_tag,
        load_profile=load_profile,
        run_seconds=run_seconds,
        reset_checkpoint=reset_checkpoint,
        stop_on_input_sentinel=stop_on_input_sentinel,
        execution_mode=execution_mode,
        python_executable=python_executable,
    )


def start_stream_process(
    *,
    config: str,
    model: str,
    feature_set: str,
    run_tag: str,
    load_profile: str,
    run_seconds: int,
    reset_checkpoint: bool,
    stop_on_input_sentinel: bool = False,
    execution_mode: str = "host",
    python_executable: str = "python",
    bootstrap_servers: str = "",
):
    return runtime_start_stream_process(
        config=config,
        model=model,
        feature_set=feature_set,
        run_tag=run_tag,
        load_profile=load_profile,
        run_seconds=run_seconds,
        reset_checkpoint=reset_checkpoint,
        stop_on_input_sentinel=stop_on_input_sentinel,
        execution_mode=execution_mode,
        python_executable=python_executable,
        bootstrap_servers=bootstrap_servers,
        project_root=PROJECT_ROOT,
        start_background_process_fn=start_background_process,
        build_stream_command_fn=build_stream_command,
        child_env_fn=_child_env,
        normalized_execution_mode_fn=_normalized_execution_mode,
    )


def cleanup_stream_processes(
    *,
    run_tag: str,
    execution_mode: str = "host",
) -> None:
    normalized_run_tag = str(run_tag).strip()
    if not normalized_run_tag:
        return

    if _normalized_execution_mode(execution_mode) == "docker":
        cleanup_docker_stream_processes(
            run_command_fn=run_command,
            project_root=PROJECT_ROOT,
            run_tag=normalized_run_tag,
        )
        return

    cleanup_host_stream_processes(
        iter_host_stream_processes_fn=_iter_host_stream_processes,
        run_tag=normalized_run_tag,
    )


def _iter_docker_stream_processes(run_tag: str) -> list[dict[str, object]]:
    return iter_docker_stream_processes(
        run_command_fn=run_command,
        project_root=PROJECT_ROOT,
        run_tag=run_tag,
    )


def _iter_host_stream_processes(run_tag: str) -> list[dict[str, object]]:
    return iter_host_stream_processes(
        run_command_fn=run_command,
        project_root=PROJECT_ROOT,
        run_tag=run_tag,
    )


def wait_for_stream_shutdown(
    *,
    run_tag: str,
    execution_mode: str = "host",
    timeout_sec: int = 30,
    poll_sec: float = 0.5,
    settle_sec: float = 2.0,
) -> bool:
    normalized_run_tag = str(run_tag).strip()
    if not normalized_run_tag:
        return True

    def _active_processes() -> list[dict[str, object]]:
        if _normalized_execution_mode(execution_mode) == "docker":
            return _iter_docker_stream_processes(normalized_run_tag)
        return _iter_host_stream_processes(normalized_run_tag)

    return process_wait_for_stream_shutdown(
        active_processes_fn=_active_processes,
        sleep_fn=time.sleep,
        time_module=time,
        timeout_sec=timeout_sec,
        poll_sec=poll_sec,
        settle_sec=settle_sec,
    )


def stop_stream_process(process) -> None:
    shutdown_stop_stream_process(
        process,
        write_shutdown_request_fn=write_shutdown_request,
        time_module=time,
        wait_for_stream_shutdown_fn=wait_for_stream_shutdown,
        cleanup_stream_processes_fn=cleanup_stream_processes,
        wait_for_process_exit_fn=wait_for_process_exit,
        stop_background_process_fn=stop_background_process,
    )


def wait_for_process_exit(process, *, timeout_sec: int) -> bool:
    return process_wait_for_process_exit(process, timeout_sec=timeout_sec)


def replay_with_retries(
    *,
    config: str,
    run_tag: str,
    rows: int,
    batch_size: int,
    input_parquet: str,
    trace_order_column: str,
    rows_per_sec: float = 0.0,
    rate_schedule: str = "",
    retries: int = 1,
    retry_wait_sec: int = 0,
    emit_input_sentinel: bool = True,
    execution_mode: str = "host",
    python_executable: str = "python",
    bootstrap_servers: str = "",
) -> None:
    runtime_replay_with_retries(
        config=config,
        run_tag=run_tag,
        rows=rows,
        batch_size=batch_size,
        input_parquet=input_parquet,
        trace_order_column=trace_order_column,
        rows_per_sec=rows_per_sec,
        rate_schedule=rate_schedule,
        retries=retries,
        retry_wait_sec=retry_wait_sec,
        emit_input_sentinel=emit_input_sentinel,
        execution_mode=execution_mode,
        python_executable=python_executable,
        bootstrap_servers=bootstrap_servers,
        project_root=PROJECT_ROOT,
        script_command_fn=_script_command,
        child_env_fn=_child_env,
        run_command_fn=run_command,
        sleep_fn=time.sleep,
    )


def fetch_metric(
    *,
    config: str,
    run_tag: str,
    timeout_sec: int,
    after_ts_utc: str = "",
    after_epoch_ms: int = 0,
    execution_mode: str = "host",
    python_executable: str = "python",
    bootstrap_servers: str = "",
) -> dict | None:
    return runtime_fetch_metric(
        config=config,
        run_tag=run_tag,
        timeout_sec=timeout_sec,
        after_ts_utc=after_ts_utc,
        after_epoch_ms=after_epoch_ms,
        execution_mode=execution_mode,
        python_executable=python_executable,
        bootstrap_servers=bootstrap_servers,
        project_root=PROJECT_ROOT,
        script_command_fn=_script_command,
        child_env_fn=_child_env,
        run_command_fn=run_command,
    )


def restart_service(
    name: str,
    *,
    execution_mode: str = "host",
) -> None:
    services_restart_service(
        name,
        project_root=PROJECT_ROOT,
        run_command_or_raise_fn=run_command_or_raise,
    )


def wait_for_kafka_topics_ready(
    *,
    bootstrap_servers: str,
    topic_names: list[str],
    timeout_sec: int = 60,
    poll_sec: float = 2.0,
    execution_mode: str = "host",
    consecutive_successes: int = 2,
) -> bool:
    return services_wait_for_kafka_topics_ready(
        bootstrap_servers=bootstrap_servers,
        topic_names=topic_names,
        timeout_sec=timeout_sec,
        poll_sec=poll_sec,
        execution_mode=execution_mode,
        consecutive_successes=consecutive_successes,
        normalized_execution_mode_fn=_normalized_execution_mode,
        docker_wait_for_kafka_topics_ready_fn=docker_wait_for_kafka_topics_ready,
        host_wait_for_kafka_topics_ready_fn=host_wait_for_kafka_topics_ready,
        run_command_fn=run_command,
        sleep_fn=time.sleep,
        project_root=PROJECT_ROOT,
        time_module=time,
    )


def describe_kafka_topics_state(
    *,
    bootstrap_servers: str,
    topic_names: list[str],
    execution_mode: str = "host",
) -> str:
    return services_describe_kafka_topics_state(
        bootstrap_servers=bootstrap_servers,
        topic_names=topic_names,
        execution_mode=execution_mode,
        normalized_execution_mode_fn=_normalized_execution_mode,
        docker_describe_kafka_topics_state_fn=docker_describe_kafka_topics_state,
        host_describe_kafka_topics_state_fn=host_describe_kafka_topics_state,
        run_command_fn=run_command,
        project_root=PROJECT_ROOT,
    )


def wait_for_kafka_bootstrap_ready(
    *,
    bootstrap_servers: str,
    timeout_sec: int = 60,
    poll_sec: float = 2.0,
    consecutive_successes: int = 2,
    execution_mode: str = "host",
) -> bool:
    return services_wait_for_kafka_bootstrap_ready(
        bootstrap_servers=bootstrap_servers,
        timeout_sec=timeout_sec,
        poll_sec=poll_sec,
        consecutive_successes=consecutive_successes,
        execution_mode=execution_mode,
        normalized_execution_mode_fn=_normalized_execution_mode,
        docker_wait_for_kafka_bootstrap_ready_fn=docker_wait_for_kafka_bootstrap_ready,
        host_wait_for_kafka_bootstrap_ready_fn=host_wait_for_kafka_bootstrap_ready,
        run_command_fn=run_command,
        sleep_fn=time.sleep,
        project_root=PROJECT_ROOT,
        time_module=time,
    )
