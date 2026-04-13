from __future__ import annotations

import json
import time
from datetime import datetime

from ids_platform.common.paths import PROJECT_ROOT
from ids_platform.common.subprocess import run_command, run_command_or_raise, start_background_process, stop_background_process
from ids_platform.streaming.replay.config import parse_rate_schedule


def normalize_rate_schedule(raw: str) -> str:
    text = (raw or "").strip()
    if not text:
        return ""
    parse_rate_schedule(text, error_message="warmup rate schedule must be rps:seconds,rps:seconds")
    return text


def build_stream_command(
    *,
    config: str,
    model: str,
    feature_set: str,
    run_tag: str,
    run_seconds: int,
    reset_checkpoint: bool,
) -> list[str]:
    command = [
        "docker",
        "compose",
        "exec",
        "-T",
        "ids-dev",
        "python",
        "scripts/streaming/run_structured_streaming.py",
        "--config",
        config,
        "--model",
        model,
        "--feature-set",
        feature_set,
        "--run-tag",
        run_tag,
        "--input-run-tag",
        run_tag,
        "--override-starting-offsets",
        "latest",
        "--run-seconds",
        str(run_seconds),
    ]
    if reset_checkpoint:
        command.append("--reset-checkpoint")
    return command


def start_stream_process(
    *,
    config: str,
    model: str,
    feature_set: str,
    run_tag: str,
    run_seconds: int,
    reset_checkpoint: bool,
):
    return start_background_process(
        build_stream_command(
            config=config,
            model=model,
            feature_set=feature_set,
            run_tag=run_tag,
            run_seconds=run_seconds,
            reset_checkpoint=reset_checkpoint,
        ),
        cwd=PROJECT_ROOT,
    )


def cleanup_stream_processes(*, run_tag: str) -> None:
    normalized_run_tag = str(run_tag).strip()
    if not normalized_run_tag:
        return

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


def stop_stream_process(process) -> None:
    stop_background_process(process)


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
) -> None:
    command = [
        "docker",
        "compose",
        "exec",
        "-T",
        "ids-dev",
        "python",
        "scripts/streaming/replay_parquet_to_kafka.py",
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
    ]
    if rate_schedule:
        command.extend(["--rate-schedule", rate_schedule])
    elif rows_per_sec > 0:
        command.extend(["--rows-per-sec", str(rows_per_sec)])

    attempts = max(int(retries), 1)
    for attempt in range(1, attempts + 1):
        result = run_command(command, cwd=PROJECT_ROOT)
        if result.returncode == 0:
            return

        stderr = (result.stderr or "").lower()
        transient = (
            "connection refused" in stderr
            or "invalid replication factor" in stderr
            or "leader not available" in stderr
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
) -> dict | None:
    command = [
        "docker",
        "compose",
        "exec",
        "-T",
        "ids-dev",
        "python",
        "scripts/streaming/read_metrics_for_run.py",
        "--config",
        config,
        "--run-tag",
        run_tag,
        "--timeout-sec",
        str(timeout_sec),
    ]
    if after_ts_utc.strip():
        command.extend(["--after-ts-utc", after_ts_utc])
    if after_epoch_ms > 0:
        command.extend(["--after-epoch-ms", str(after_epoch_ms)])

    result = run_command(command, cwd=PROJECT_ROOT, timeout=timeout_sec + 15)
    if result.returncode != 0:
        return None

    output = (result.stdout or "").strip()
    if not output:
        return None

    try:
        return json.loads(output.splitlines()[-1].strip())
    except Exception:
        return None


def restart_service(name: str) -> None:
    run_command_or_raise(["docker", "compose", "restart", name], cwd=PROJECT_ROOT, timeout=180)


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
        "scenario": scenario,
        "model": model,
        "feature_set": feature_set,
        "fault_ts_utc": "",
        "recover_ts_utc": "",
        "recovery_seconds": "",
        "rows_after_fault": "",
        "rows_per_sec_after_fault": "",
        "proc_p95_ms_after_fault": "",
        "e2e_p95_ms_after_fault": "",
        "status": "failed",
        "notes": "",
    }

