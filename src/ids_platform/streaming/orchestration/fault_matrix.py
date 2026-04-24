from __future__ import annotations

import json
import os
import signal
import time
from datetime import datetime
from pathlib import Path

from ids_platform.common.paths import PROJECT_ROOT
from ids_platform.common.subprocess import (
    run_command,
    run_command_or_raise,
    start_background_process,
    stop_background_process,
)
import subprocess
from ids_platform.streaming.config import resolve_kafka_bootstrap_servers
from ids_platform.streaming.replay.config import parse_rate_schedule
from ids_platform.streaming.runtime.control import write_shutdown_request


def normalize_rate_schedule(raw: str) -> str:
    text = (raw or "").strip()
    if not text:
        return ""
    parse_rate_schedule(text, error_message="warmup rate schedule must be rps:seconds,rps:seconds")
    return text


def _normalized_execution_mode(execution_mode: str) -> str:
    mode = str(execution_mode or "").strip().lower()
    if mode in {"", "host"}:
        return "host"
    if mode == "docker":
        return mode
    raise ValueError("execution_mode must be 'host' or 'docker'")


def _child_env(*, execution_mode: str, bootstrap_servers: str = "") -> dict[str, str]:
    mode = _normalized_execution_mode(execution_mode)
    env = {"IDS_EXECUTION_MODE": mode}
    if bootstrap_servers.strip():
        env["KAFKA_BOOTSTRAP_SERVERS"] = resolve_kafka_bootstrap_servers(
            bootstrap_servers,
            execution_mode=mode,
        )
    return env


def _script_command(
    *,
    execution_mode: str,
    python_executable: str,
    script_path: str,
    script_args: list[str],
) -> list[str]:
    mode = _normalized_execution_mode(execution_mode)
    if mode == "docker":
        return ["docker", "compose", "exec", "-T", "ids-dev", "python", script_path, *script_args]
    return [python_executable, script_path, *script_args]


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
    command = _script_command(
        execution_mode=execution_mode,
        python_executable=python_executable,
        script_path="scripts/streaming/run_structured_streaming.py",
        script_args=[
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
            run_tag,
            "--override-starting-offsets",
            "latest",
            "--run-seconds",
            str(run_seconds),
        ],
    )
    if reset_checkpoint:
        command.append("--reset-checkpoint")
    if stop_on_input_sentinel:
        command.append("--stop-on-input-sentinel")
    return command


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
    log_path = PROJECT_ROOT / "logs" / "streaming" / "runtime" / f"{Path(run_tag).name}.log"
    proc = start_background_process(
        build_stream_command(
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
        cwd=PROJECT_ROOT,
        env=_child_env(execution_mode=execution_mode, bootstrap_servers=bootstrap_servers),
        stdout_path=log_path,
    )
    setattr(proc, "ids_execution_mode", _normalized_execution_mode(execution_mode))
    setattr(proc, "ids_run_tag", str(run_tag).strip())
    return proc


def cleanup_stream_processes(
    *,
    run_tag: str,
    execution_mode: str = "host",
) -> None:
    normalized_run_tag = str(run_tag).strip()
    if not normalized_run_tag:
        return

    if _normalized_execution_mode(execution_mode) == "docker":
        run_command(
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
                    "run_tag=sys.argv[1];"
                    "out=subprocess.check_output(['ps','-ef'], text=True);"
                    "matches=[];"
                    "for line in out.splitlines():"
                    "    if 'run_structured_streaming.py' not in line or 'python -c' in line or run_tag not in line:"
                    "        continue;"
                    "    parts=line.split();"
                    "    if len(parts) > 1:"
                    "        matches.append(int(parts[1]));"
                    "[os.kill(pid, signal.SIGTERM) for pid in matches if pid != os.getpid()];"
                    "print(f'killed={len(matches)}')"
                ),
                normalized_run_tag,
            ],
            cwd=PROJECT_ROOT,
            timeout=30,
        )
        return

    current_pid = os.getpid()
    for proc in _iter_host_stream_processes(normalized_run_tag):
        pid = proc.get("pid")
        if not isinstance(pid, int) or pid == current_pid:
            continue
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError:
            continue


def _iter_docker_stream_processes(run_tag: str) -> list[dict[str, object]]:
    result = run_command(
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
                "run_tag=sys.argv[1];"
                "out=subprocess.check_output(['ps','-ef'], text=True);"
                "rows=[];"
                "for line in out.splitlines():"
                "    if 'run_structured_streaming.py' not in line or 'python -c' in line or run_tag not in line:"
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
        cwd=PROJECT_ROOT,
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


def _iter_host_stream_processes(run_tag: str) -> list[dict[str, object]]:
    ps_script = (
        "Get-CimInstance Win32_Process | "
        "Select-Object ProcessId,CommandLine | "
        "ConvertTo-Json -Compress"
    )
    result = run_command(
        ["powershell", "-NoProfile", "-Command", ps_script],
        cwd=PROJECT_ROOT,
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
        if "run_structured_streaming.py" not in command_line or run_tag not in command_line:
            continue
        try:
            pid = int(row.get("ProcessId"))
        except Exception:
            continue
        matches.append({"pid": pid, "command_line": command_line})
    return matches


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

    end_time = time.time() + max(int(timeout_sec), 1)
    while time.time() < end_time:
        if _normalized_execution_mode(execution_mode) == "docker":
            active = _iter_docker_stream_processes(normalized_run_tag)
        else:
            active = _iter_host_stream_processes(normalized_run_tag)
        if not active:
            if settle_sec > 0:
                time.sleep(float(settle_sec))
            return True
        time.sleep(max(float(poll_sec), 0.1))
    return False


def stop_stream_process(process) -> None:
    if process is None:
        return

    execution_mode = str(getattr(process, "ids_execution_mode", "") or "").strip().lower()
    run_tag = str(getattr(process, "ids_run_tag", "") or "").strip()

    if execution_mode == "docker" and run_tag:
        write_shutdown_request(run_tag)
        graceful_deadline = time.time() + 45.0
        while time.time() < graceful_deadline:
            if process.poll() is not None:
                stop_background_process(process)
                return
            if wait_for_stream_shutdown(
                run_tag=run_tag,
                execution_mode="docker",
                timeout_sec=1,
                poll_sec=0.25,
                settle_sec=0.0,
            ):
                break
            time.sleep(0.25)
        else:
            cleanup_stream_processes(run_tag=run_tag, execution_mode="docker")
            wait_for_stream_shutdown(
                run_tag=run_tag,
                execution_mode="docker",
                timeout_sec=45,
                poll_sec=0.5,
                settle_sec=2.0,
            )

        if wait_for_process_exit(process, timeout_sec=45):
            stop_background_process(process)
            return

    stop_background_process(process)


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
    command = _script_command(
        execution_mode=execution_mode,
        python_executable=python_executable,
        script_path="scripts/streaming/replay_parquet_to_kafka.py",
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

    env = _child_env(execution_mode=execution_mode, bootstrap_servers=bootstrap_servers)
    attempts = max(int(retries), 1)
    for attempt in range(1, attempts + 1):
        result = run_command(command, cwd=PROJECT_ROOT, env=env)
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
        time.sleep(max(int(retry_wait_sec), 1))


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
    command = _script_command(
        execution_mode=execution_mode,
        python_executable=python_executable,
        script_path="scripts/streaming/read_metrics_for_run.py",
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

    result = run_command(
        command,
        cwd=PROJECT_ROOT,
        timeout=timeout_sec + 15,
        env=_child_env(execution_mode=execution_mode, bootstrap_servers=bootstrap_servers),
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


def restart_service(
    name: str,
    *,
    execution_mode: str = "host",
) -> None:
    run_command_or_raise(["docker", "compose", "restart", name], cwd=PROJECT_ROOT, timeout=180)


def wait_for_kafka_topics_ready(
    *,
    bootstrap_servers: str,
    topic_names: list[str],
    timeout_sec: int = 60,
    poll_sec: float = 2.0,
) -> bool:
    try:
        from confluent_kafka.admin import AdminClient
    except Exception:
        return False

    normalized_bootstrap = str(bootstrap_servers or "").strip()
    normalized_topics = [str(topic).strip() for topic in topic_names if str(topic).strip()]
    if not normalized_bootstrap or not normalized_topics:
        return False

    deadline = time.time() + max(int(timeout_sec), 1)
    while time.time() < deadline:
        try:
            admin_client = AdminClient({"bootstrap.servers": normalized_bootstrap})
            metadata = admin_client.list_topics(timeout=10.0)
            all_ready = True
            for topic_name in normalized_topics:
                topic_metadata = metadata.topics.get(topic_name)
                if topic_metadata is None:
                    all_ready = False
                    break
                partitions = getattr(topic_metadata, "partitions", {}) or {}
                if not partitions:
                    all_ready = False
                    break
                for partition_metadata in partitions.values():
                    leader = getattr(partition_metadata, "leader", -1)
                    if leader is None or int(leader) < 0:
                        all_ready = False
                        break
                if not all_ready:
                    break
            if all_ready:
                return True
        except Exception:
            pass
        time.sleep(max(float(poll_sec), 0.25))
    return False


def wait_for_kafka_bootstrap_ready(
    *,
    bootstrap_servers: str,
    timeout_sec: int = 60,
    poll_sec: float = 2.0,
    consecutive_successes: int = 2,
    execution_mode: str = "host",
) -> bool:
    normalized_bootstrap = str(bootstrap_servers or "").strip()
    if not normalized_bootstrap:
        return False

    if _normalized_execution_mode(execution_mode) == "docker":
        probe_code = (
            "import sys,time;"
            "from confluent_kafka.admin import AdminClient;"
            "bootstrap=sys.argv[1];"
            "admin=AdminClient({'bootstrap.servers': bootstrap});"
            "md=admin.list_topics(timeout=10.0);"
            "brokers=getattr(md,'brokers',{}) or {};"
            "sys.exit(0 if brokers else 1)"
        )
        deadline = time.time() + max(int(timeout_sec), 1)
        success_count = 0
        required_successes = max(int(consecutive_successes), 1)
        while time.time() < deadline:
            result = run_command(
                [
                    "docker",
                    "compose",
                    "exec",
                    "-T",
                    "ids-dev",
                    "python",
                    "-c",
                    probe_code,
                    normalized_bootstrap,
                ],
                cwd=PROJECT_ROOT,
                timeout=20,
            )
            if result.returncode == 0:
                success_count += 1
                if success_count >= required_successes:
                    return True
            else:
                success_count = 0
            time.sleep(max(float(poll_sec), 0.25))
        return False

    try:
        from confluent_kafka.admin import AdminClient
    except Exception:
        return False

    deadline = time.time() + max(int(timeout_sec), 1)
    success_count = 0
    required_successes = max(int(consecutive_successes), 1)
    while time.time() < deadline:
        try:
            admin_client = AdminClient({"bootstrap.servers": normalized_bootstrap})
            metadata = admin_client.list_topics(timeout=10.0)
            brokers = getattr(metadata, "brokers", {}) or {}
            if brokers:
                success_count += 1
                if success_count >= required_successes:
                    return True
            else:
                success_count = 0
        except Exception:
            success_count = 0
        time.sleep(max(float(poll_sec), 0.25))
    return False


def parse_iso_datetime(raw: str) -> datetime:
    return datetime.fromisoformat(raw.replace("Z", "+00:00"))


def safe_float(value) -> float | str:
    try:
        return float(value)
    except Exception:
        return ""


def scenario_row_template(*, run_tag: str, scenario: str, model: str, feature_set: str) -> dict:
    return {
        "run_tag": run_tag,
        "load_profile": scenario,
        "scenario": scenario,
        "model": model,
        "feature_set": feature_set,
        "fault_ts_utc": "",
        "recover_ts_utc": "",
        "recovery_seconds": "",
        "rows_after_fault": "",
        "rows_per_sec_after_fault": "",
        "ingest_to_emit_p95_ms_after_fault": "",
        "source_to_emit_p95_ms_after_fault": "",
        "proc_p95_ms_after_fault": "",
        "e2e_p95_ms_after_fault": "",
        "metric_warnings": "",
        "status": "failed",
        "notes": "",
    }
