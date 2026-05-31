from __future__ import annotations

from datetime import datetime

from ids_platform.streaming.core.config import resolve_kafka_bootstrap_servers


def normalized_execution_mode(execution_mode: str) -> str:
    mode = str(execution_mode or "").strip().lower()
    if mode in {"", "host"}:
        return "host"
    if mode == "docker":
        return mode
    raise ValueError("execution_mode must be 'host' or 'docker'")


def child_env(*, execution_mode: str, bootstrap_servers: str = "") -> dict[str, str]:
    mode = normalized_execution_mode(execution_mode)
    env = {"IDS_EXECUTION_MODE": mode}
    if bootstrap_servers.strip():
        env["KAFKA_BOOTSTRAP_SERVERS"] = resolve_kafka_bootstrap_servers(
            bootstrap_servers,
            execution_mode=mode,
        )
    return env


def script_command(
    *,
    execution_mode: str,
    python_executable: str,
    script_path: str,
    script_args: list[str],
) -> list[str]:
    mode = normalized_execution_mode(execution_mode)
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
    reset_checkpoint: bool,
    execution_mode: str = "host",
    python_executable: str = "python",
) -> list[str]:
    command = script_command(
        execution_mode=execution_mode,
        python_executable=python_executable,
        script_path="scripts/streaming/official/run_structured_streaming.py",
        script_args=[],
    )
    return command


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
