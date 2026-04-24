from __future__ import annotations

import os
import subprocess
from collections.abc import Mapping
from pathlib import Path

from ids_platform.common.paths import PROJECT_ROOT


def run_command(
    command: list[str],
    *,
    cwd: Path = PROJECT_ROOT,
    timeout: int | None = None,
    stream_output: bool = False,
    env: Mapping[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    merged_env = os.environ.copy()
    if env:
        merged_env.update({str(key): str(value) for key, value in env.items()})

    if stream_output:
        return subprocess.run(
            command,
            cwd=cwd,
            env=merged_env,
            text=True,
            timeout=timeout if timeout and timeout > 0 else None,
            check=False,
        )

    return subprocess.run(
        command,
        cwd=cwd,
        env=merged_env,
        capture_output=True,
        text=True,
        timeout=timeout if timeout and timeout > 0 else None,
        check=False,
    )


def run_command_or_raise(
    command: list[str],
    *,
    cwd: Path = PROJECT_ROOT,
    timeout: int | None = None,
    stream_output: bool = False,
    env: Mapping[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    result = run_command(command, cwd=cwd, timeout=timeout, stream_output=stream_output, env=env)
    if result.returncode != 0:
        raise RuntimeError(
            "Command failed:\n"
            + " ".join(command)
            + f"\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )
    return result


def start_background_process(
    command: list[str],
    *,
    cwd: Path = PROJECT_ROOT,
    env: Mapping[str, str] | None = None,
    stdout_path: str | Path | None = None,
) -> subprocess.Popen:
    merged_env = os.environ.copy()
    if env:
        merged_env.update({str(key): str(value) for key, value in env.items()})

    stdout_handle = None
    stdout_target = subprocess.DEVNULL
    stderr_target = subprocess.DEVNULL
    log_start_offset = 0
    if stdout_path is not None:
        log_path = Path(stdout_path)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        if log_path.exists():
            try:
                log_start_offset = log_path.stat().st_size
            except OSError:
                log_start_offset = 0
        stdout_handle = log_path.open("a", encoding="utf-8")
        stdout_target = stdout_handle
        stderr_target = subprocess.STDOUT

    proc = subprocess.Popen(
        command,
        cwd=cwd,
        env=merged_env,
        stdout=stdout_target,
        stderr=stderr_target,
    )
    setattr(proc, "ids_log_path", str(stdout_path) if stdout_path is not None else "")
    setattr(proc, "ids_log_handle", stdout_handle)
    setattr(proc, "ids_log_start_offset", int(log_start_offset))
    return proc


def stop_background_process(proc: subprocess.Popen | None) -> None:
    if proc is None or proc.poll() is not None:
        handle = getattr(proc, "ids_log_handle", None) if proc is not None else None
        if handle is not None:
            handle.close()
        return
    proc.terminate()
    try:
        proc.wait(timeout=20)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=10)
    handle = getattr(proc, "ids_log_handle", None)
    if handle is not None:
        handle.close()
