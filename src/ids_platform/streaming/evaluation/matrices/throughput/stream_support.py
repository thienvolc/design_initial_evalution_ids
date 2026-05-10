from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path


def _append_runtime_command_output(
    *,
    log_path: str | Path,
    command: list[str],
    stdout_text: str = "",
    stderr_text: str = "",
    status: str,
) -> None:
    path = Path(log_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(
            f"\n[{datetime.now(timezone.utc).isoformat(timespec='seconds')}] "
            f"status={status} command={' '.join(command)}\n"
        )
        if stdout_text:
            handle.write("STDOUT:\n")
            handle.write(stdout_text)
            if not stdout_text.endswith("\n"):
                handle.write("\n")
        if stderr_text:
            handle.write("STDERR:\n")
            handle.write(stderr_text)
            if not stderr_text.endswith("\n"):
                handle.write("\n")


def _run_command_with_runtime_log(
    *,
    command: list[str],
    run_command_or_raise_fn,
    runtime_log_path: str | Path,
) -> None:
    try:
        result = run_command_or_raise_fn(command)
    except Exception as exc:
        _append_runtime_command_output(
            log_path=runtime_log_path,
            command=command,
            stderr_text=str(exc),
            status="error",
        )
        raise

    _append_runtime_command_output(
        log_path=runtime_log_path,
        command=command,
        stdout_text=str(getattr(result, "stdout", "") or ""),
        stderr_text=str(getattr(result, "stderr", "") or ""),
        status="ok",
    )


def build_structured_stream_command(
    *,
    python_exe: str,
    config: str,
    model: str,
    feature_set: str,
    baseline_mode: str = "",
    run_tag: str,
    load_profile: str,
    input_run_tag: str,
    reset_checkpoint: bool = True,
    starting_offsets: str = "",
    max_offsets_per_trigger: int | None = None,
    shuffle_partitions: int | None = None,
    trigger_interval: str = "",
    stop_on_input_sentinel: bool = False,
    run_seconds: int | None = None,
    available_now: bool = False,
) -> list[str]:
    command = [
        python_exe,
        "scripts/streaming/official/run_structured_streaming.py",
        "--config",
        config,
        "--model",
        model,
        "--feature-set",
        feature_set,
        "--run-tag",
        run_tag,
        "--load-profile",
        load_profile,
        "--input-run-tag",
        input_run_tag,
    ]
    if baseline_mode:
        command.extend(["--baseline-mode", baseline_mode])
    if starting_offsets:
        command.extend(["--override-starting-offsets", starting_offsets])
    if max_offsets_per_trigger is not None:
        command.extend(["--override-max-offsets", str(max_offsets_per_trigger)])
    if shuffle_partitions is not None:
        command.extend(["--override-shuffle-partitions", str(shuffle_partitions)])
    if trigger_interval:
        command.extend(["--override-trigger-interval", str(trigger_interval)])
    if stop_on_input_sentinel:
        command.append("--stop-on-input-sentinel")
    if run_seconds is not None:
        command.extend(["--run-seconds", str(run_seconds)])
    if reset_checkpoint:
        command.append("--reset-checkpoint")
    if available_now:
        command.append("--available-now")
    return command


def run_trace_stream_sequence(
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
        try:
            stream_proc.wait(timeout=stream_wait_timeout_sec)
            log_phase_fn("main_stream_wait_done", run_tag=run_tag)
        except subprocess_module.TimeoutExpired:
            log_phase_fn("main_stream_wait_timeout", run_tag=run_tag)
    finally:
        stop_background_process_fn(stream_proc)
        log_phase_fn("main_stream_stop", run_tag=run_tag)


def run_available_now_sequence(
    *,
    run_tag: str,
    replay_cmd: list[str],
    stream_cmd: list[str],
    log_phase_fn,
    run_command_or_raise_fn,
    runtime_log_output_path_fn=None,
) -> None:
    log_phase_fn("main_replay_start", run_tag=run_tag)
    run_command_or_raise_fn(replay_cmd)
    log_phase_fn("main_replay_done", run_tag=run_tag)
    log_phase_fn("main_stream_start", run_tag=run_tag, available_now=True)
    if runtime_log_output_path_fn is not None:
        runtime_log_path = runtime_log_output_path_fn(run_tag=run_tag)
        log_phase_fn("stream_runtime_log", run_tag=run_tag, path=runtime_log_path)
        _run_command_with_runtime_log(
            command=stream_cmd,
            run_command_or_raise_fn=run_command_or_raise_fn,
            runtime_log_path=runtime_log_path,
        )
    else:
        run_command_or_raise_fn(stream_cmd)
    log_phase_fn("main_stream_stop", run_tag=run_tag, available_now=True)


def run_trace_warmup_sequence(
    *,
    run_tag: str,
    warmup_tag: str,
    warmup_stream_cmd: list[str],
    warmup_replay_cmd: list[str],
    warmup_stream_seconds: int,
    startup_wait_sec: int,
    runtime_log_output_path_fn,
    log_phase_fn,
    start_background_process_fn,
    wait_for_process_startup_fn,
    run_command_or_raise_fn,
    stop_background_process_fn,
    subprocess_module,
) -> None:
    runtime_log_path = runtime_log_output_path_fn(run_tag=warmup_tag)
    log_phase_fn(
        "warmup_stream_start",
        run_tag=run_tag,
        warmup_tag=warmup_tag,
        run_seconds=warmup_stream_seconds,
    )
    log_phase_fn("stream_runtime_log", run_tag=run_tag, warmup_tag=warmup_tag, path=runtime_log_path)
    warmup_stream_proc = start_background_process_fn(
        warmup_stream_cmd,
        stdout_path=runtime_log_path,
    )
    try:
        if not wait_for_process_startup_fn(
            warmup_stream_proc,
            startup_wait_sec=startup_wait_sec,
        ):
            raise RuntimeError("structured streaming process exited before warmup replay")
        log_phase_fn("warmup_replay_start", run_tag=run_tag, warmup_tag=warmup_tag)
        run_command_or_raise_fn(warmup_replay_cmd)
        log_phase_fn("warmup_replay_done", run_tag=run_tag, warmup_tag=warmup_tag)
        warmup_wait_timeout_sec = max(warmup_stream_seconds + 60, 120)
        log_phase_fn(
            "warmup_stream_wait_start",
            run_tag=run_tag,
            warmup_tag=warmup_tag,
            wait_timeout_sec=warmup_wait_timeout_sec,
        )
        try:
            warmup_stream_proc.wait(timeout=warmup_wait_timeout_sec)
            log_phase_fn("warmup_stream_wait_done", run_tag=run_tag, warmup_tag=warmup_tag)
        except subprocess_module.TimeoutExpired:
            log_phase_fn("warmup_stream_wait_timeout", run_tag=run_tag, warmup_tag=warmup_tag)
    finally:
        stop_background_process_fn(warmup_stream_proc)
        log_phase_fn("warmup_stream_stop", run_tag=run_tag, warmup_tag=warmup_tag)


def run_available_now_warmup_sequence(
    *,
    run_tag: str,
    warmup_tag: str,
    warmup_replay_cmd: list[str],
    warmup_stream_cmd: list[str],
    log_phase_fn,
    run_command_or_raise_fn,
    runtime_log_output_path_fn=None,
) -> None:
    log_phase_fn("warmup_replay_start", run_tag=run_tag, warmup_tag=warmup_tag)
    run_command_or_raise_fn(warmup_replay_cmd)
    log_phase_fn("warmup_replay_done", run_tag=run_tag, warmup_tag=warmup_tag)
    log_phase_fn("warmup_stream_start", run_tag=run_tag, warmup_tag=warmup_tag, available_now=True)
    if runtime_log_output_path_fn is not None:
        runtime_log_path = runtime_log_output_path_fn(run_tag=warmup_tag)
        log_phase_fn("stream_runtime_log", run_tag=run_tag, warmup_tag=warmup_tag, path=runtime_log_path)
        _run_command_with_runtime_log(
            command=warmup_stream_cmd,
            run_command_or_raise_fn=run_command_or_raise_fn,
            runtime_log_path=runtime_log_path,
        )
    else:
        run_command_or_raise_fn(warmup_stream_cmd)
    log_phase_fn("warmup_stream_stop", run_tag=run_tag, warmup_tag=warmup_tag, available_now=True)
