from __future__ import annotations

import json
from pathlib import Path


def start_stream_process(
    *,
    config: str,
    model: str,
    feature_set: str,
    run_tag: str,
    load_profile: str,
    run_seconds: int,
    reset_checkpoint: bool,
    stop_on_input_sentinel: bool,
    execution_mode: str,
    python_executable: str,
    bootstrap_servers: str,
    project_root: Path,
    start_background_process_fn,
    build_stream_command_fn,
    child_env_fn,
    normalized_execution_mode_fn,
):
    log_path = project_root / "logs" / "streaming" / "runtime" / f"{Path(run_tag).name}.log"
    proc = start_background_process_fn(
        build_stream_command_fn(
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
        ),
        cwd=project_root,
        env=child_env_fn(execution_mode=execution_mode, bootstrap_servers=bootstrap_servers),
        stdout_path=log_path,
    )
    setattr(proc, "ids_execution_mode", normalized_execution_mode_fn(execution_mode))
    setattr(proc, "ids_run_tag", str(run_tag).strip())
    return proc


def replay_with_retries(
    *,
    config: str,
    run_tag: str,
    rows: int,
    batch_size: int,
    input_parquet: str,
    trace_order_column: str,
    rows_per_sec: float,
    rate_schedule: str,
    retries: int,
    retry_wait_sec: int,
    emit_input_sentinel: bool,
    execution_mode: str,
    python_executable: str,
    bootstrap_servers: str,
    project_root: Path,
    script_command_fn,
    child_env_fn,
    run_command_fn,
    sleep_fn,
) -> None:
    command = script_command_fn(
        execution_mode=execution_mode,
        python_executable=python_executable,
        script_path="scripts/streaming/official/replay_parquet_to_kafka.py",
        script_args=[
            "--config",
            config,
            "--input-parquet",
            input_parquet,
            "--trace-order-column",
            trace_order_column,
            "--run-tag",
            run_tag,
            "--max-rows",
            str(rows),
            "--batch-size",
            str(batch_size),
        ],
    )
    if rate_schedule:
        command.extend(["--rate-schedule", rate_schedule])
    elif rows_per_sec > 0:
        command.extend(["--rows-per-sec", str(rows_per_sec)])
    if not emit_input_sentinel:
        command.append("--no-input-sentinel")

    env = child_env_fn(execution_mode=execution_mode, bootstrap_servers=bootstrap_servers)
    attempts = max(int(retries), 1)
    for attempt in range(1, attempts + 1):
        result = run_command_fn(command, cwd=project_root, env=env)
        if result.returncode == 0:
            return

        stderr = (result.stderr or "").lower()
        stdout = (result.stdout or "").lower()
        combined = f"{stdout}\n{stderr}"
        transient = (
            "connection refused" in combined
            or "invalid replication factor" in combined
            or "leader not available" in combined
            or "unknown topic or partition" in combined
            or "unknowntopicorpartitionexception" in combined
            or "failed to resolve" in combined
            or "disconnected: connection closed by peer" in combined
            or "pollhup" in combined
        )
        if (not transient) or attempt >= attempts:
            raise RuntimeError(
                "Command failed:\n"
                + " ".join(command)
                + f"\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
            )
        sleep_fn(max(int(retry_wait_sec), 1))


def fetch_metric(
    *,
    config: str,
    run_tag: str,
    timeout_sec: int,
    after_ts_utc: str,
    after_epoch_ms: int,
    execution_mode: str,
    python_executable: str,
    bootstrap_servers: str,
    project_root: Path,
    script_command_fn,
    child_env_fn,
    run_command_fn,
) -> dict | None:
    command = script_command_fn(
        execution_mode=execution_mode,
        python_executable=python_executable,
        script_path="scripts/streaming/official/read_metrics_for_run.py",
        script_args=[
            "--config",
            config,
            "--run-tag",
            run_tag,
            "--timeout-sec",
            str(timeout_sec),
        ],
    )
    if after_ts_utc.strip():
        command.extend(["--after-ts-utc", after_ts_utc])
    if after_epoch_ms > 0:
        command.extend(["--after-epoch-ms", str(after_epoch_ms)])

    result = run_command_fn(
        command,
        cwd=project_root,
        timeout=timeout_sec + 15,
        env=child_env_fn(execution_mode=execution_mode, bootstrap_servers=bootstrap_servers),
    )
    if result.returncode != 0:
        return None

    output = (result.stdout or "").strip()
    if not output:
        return None

    try:
        return json.loads(output.splitlines()[-1].strip())
    except Exception:
        return None
