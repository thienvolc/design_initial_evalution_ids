from __future__ import annotations

import subprocess
from pathlib import Path

from ids_platform.common.paths import PROJECT_ROOT


def run_command(
    command: list[str],
    *,
    cwd: Path = PROJECT_ROOT,
    timeout: int | None = None,
    stream_output: bool = False,
) -> subprocess.CompletedProcess[str]:
    if stream_output:
        return subprocess.run(
            command,
            cwd=cwd,
            text=True,
            timeout=timeout if timeout and timeout > 0 else None,
            check=False,
        )

    return subprocess.run(
        command,
        cwd=cwd,
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
) -> subprocess.CompletedProcess[str]:
    result = run_command(command, cwd=cwd, timeout=timeout, stream_output=stream_output)
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
) -> subprocess.Popen:
    return subprocess.Popen(
        command,
        cwd=cwd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def stop_background_process(proc: subprocess.Popen | None) -> None:
    if proc is None or proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=20)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=10)
