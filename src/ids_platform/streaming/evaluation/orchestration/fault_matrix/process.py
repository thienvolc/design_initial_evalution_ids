from __future__ import annotations

import json
import os
import signal
import subprocess


def cleanup_docker_stream_processes(
    *,
    run_command_fn,
    project_root,
    run_tag: str,
) -> None:
    run_command_fn(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "ids-dev",
            "python",
            "-c",
            (
                "import os,signal,subprocess,sys;"
                "out=subprocess.check_output(['ps','-ef'], text=True);"
                "matches=[];"
                "for line in out.splitlines():"
                "    if 'run_structured_streaming.py' not in line or 'python -c' in line:"
                "        continue;"
                "    parts=line.split();"
                "    if len(parts) > 1:"
                "        matches.append(int(parts[1]));"
                "[os.kill(pid, signal.SIGTERM) for pid in matches if pid != os.getpid()];"
                "print(f'killed={len(matches)}')"
            ),
            run_tag,
        ],
        cwd=project_root,
        timeout=30,
    )


def cleanup_host_stream_processes(
    *,
    iter_host_stream_processes_fn,
    run_tag: str,
) -> None:
    current_pid = os.getpid()
    for proc in iter_host_stream_processes_fn(run_tag):
        pid = proc.get("pid")
        if not isinstance(pid, int) or pid == current_pid:
            continue
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError:
            continue


def iter_docker_stream_processes(
    *,
    run_command_fn,
    project_root,
    run_tag: str,
) -> list[dict[str, object]]:
    result = run_command_fn(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "ids-dev",
            "python",
            "-c",
            (
                "import json,subprocess,sys;"
                "out=subprocess.check_output(['ps','-ef'], text=True);"
                "rows=[];"
                "for line in out.splitlines():"
                "    if 'run_structured_streaming.py' not in line or 'python -c' in line:"
                "        continue;"
                "    parts=line.split();"
                "    if len(parts) <= 1:"
                "        continue;"
                "    try:"
                "        pid=int(parts[1]);"
                "    except Exception:"
                "        continue;"
                "    rows.append({'pid': pid, 'command_line': line});"
                "print(json.dumps(rows))"
            ),
            str(run_tag).strip(),
        ],
        cwd=project_root,
        timeout=30,
    )
    if result.returncode != 0 or not (result.stdout or "").strip():
        return []
    try:
        payload = json.loads(result.stdout)
    except Exception:
        return []
    if not isinstance(payload, list):
        return []
    return [row for row in payload if isinstance(row, dict)]


def iter_host_stream_processes(
    *,
    run_command_fn,
    project_root,
    run_tag: str,
) -> list[dict[str, object]]:
    ps_script = (
        "Get-CimInstance Win32_Process | "
        "Select-Object ProcessId,CommandLine | "
        "ConvertTo-Json -Compress"
    )
    result = run_command_fn(
        ["powershell", "-NoProfile", "-Command", ps_script],
        cwd=project_root,
        timeout=30,
    )
    if result.returncode != 0 or not (result.stdout or "").strip():
        return []
    try:
        payload = json.loads(result.stdout)
    except Exception:
        return []

    rows = payload if isinstance(payload, list) else [payload]
    matches: list[dict[str, object]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        command_line = str(row.get("CommandLine") or "")
        if "run_structured_streaming.py" not in command_line:
            continue
        try:
            pid = int(row.get("ProcessId"))
        except Exception:
            continue
        matches.append({"pid": pid, "command_line": command_line})
    return matches


def wait_for_stream_shutdown(
    *,
    active_processes_fn,
    sleep_fn,
    time_module,
    timeout_sec: int = 30,
    poll_sec: float = 0.5,
    settle_sec: float = 2.0,
) -> bool:
    end_time = time_module.time() + max(int(timeout_sec), 1)
    while time_module.time() < end_time:
        active = active_processes_fn()
        if not active:
            if settle_sec > 0:
                sleep_fn(float(settle_sec))
                if active_processes_fn():
                    sleep_fn(max(float(poll_sec), 0.1))
                    continue
            return True
        sleep_fn(max(float(poll_sec), 0.1))
    return False


def wait_for_process_exit(process, *, timeout_sec: int) -> bool:
    if process is None:
        return True
    try:
        process.wait(timeout=max(int(timeout_sec), 1))
        return True
    except subprocess.TimeoutExpired:
        return False
    except Exception:
        return process.poll() is not None
