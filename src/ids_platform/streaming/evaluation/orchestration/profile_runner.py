from __future__ import annotations

from pathlib import Path

from ids_platform.common.paths import PROJECT_ROOT, resolve_project_path
from ids_platform.streaming.evaluation.orchestration.profile_runner_gates import (
    evaluate_pass_fail as _support_evaluate_pass_fail,
    evaluate_profile_gates as _support_evaluate_profile_gates,
    extract_series as _support_extract_series,
    load_csv_rows as _support_load_csv_rows,
    sum_rows_total as _support_sum_rows_total,
    to_float as _support_to_float,
)
from ids_platform.streaming.evaluation.orchestration.profile_runner_support import (
    append_arg as _support_append_arg,
    build_command as _support_build_command,
    deep_merge as _support_deep_merge,
    format_layer_a_profile as _support_format_layer_a_profile,
    include_profile_by_filters as _support_include_profile_by_filters,
    normalize_args_for_script as _support_normalize_args_for_script,
    profile_mode as _support_profile_mode,
    profile_resource_class as _support_profile_resource_class,
    resolve_allowed_script_path as _support_resolve_allowed_script_path,
    resolve_profile as _support_resolve_profile,
    to_flag as _support_to_flag,
)

_ALLOWED_SCRIPT_ROOTS = (
    PROJECT_ROOT / "scripts" / "streaming",
    PROJECT_ROOT / "scripts" / "offline",
)


def _log_gate_event(event: str, **fields) -> None:
    parts = [f"[gate] event={event}"]
    for key, value in fields.items():
        if value is None:
            continue
        parts.append(f"{key}={value}")
    print(" ".join(parts), flush=True)


def deep_merge(parent: dict, child: dict) -> dict:
    return _support_deep_merge(parent, child)


def resolve_profile(name: str, profiles: dict, stack: set[str] | None = None) -> dict:
    return _support_resolve_profile(name, profiles, stack)


def to_flag(name: str) -> str:
    return _support_to_flag(name)


def append_arg(command: list[str], key: str, value) -> None:
    _support_append_arg(command, key, value)


def format_layer_a_profile(value: dict) -> str:
    return _support_format_layer_a_profile(value)


def normalize_args_for_script(script: str, arguments: dict) -> dict:
    return _support_normalize_args_for_script(script, arguments)


def _resolve_allowed_script_path(script: str) -> str:
    return _support_resolve_allowed_script_path(
        script,
        resolve_project_path=resolve_project_path,
        project_root=PROJECT_ROOT,
        allowed_script_roots=_ALLOWED_SCRIPT_ROOTS,
    )


def build_command(profile: dict, python_executable: str) -> list[str]:
    return _support_build_command(
        profile,
        python_executable,
        resolve_project_path=resolve_project_path,
        project_root=PROJECT_ROOT,
        allowed_script_roots=_ALLOWED_SCRIPT_ROOTS,
    )


def profile_mode(profile: dict) -> str:
    return _support_profile_mode(profile)


def profile_resource_class(profile: dict) -> str:
    return _support_profile_resource_class(profile)


def include_profile_by_filters(profile: dict, args) -> bool:
    return _support_include_profile_by_filters(profile, args)


def to_float(value) -> float | None:
    return _support_to_float(value)


def load_csv_rows(path: Path) -> list[dict]:
    return _support_load_csv_rows(path)


def extract_series(rows: list[dict], candidates: list[str]) -> tuple[list[float], str]:
    return _support_extract_series(rows, candidates, to_float_fn=to_float)


def sum_rows_total(rows: list[dict]) -> tuple[float, str]:
    return _support_sum_rows_total(rows, to_float_fn=to_float)


def evaluate_pass_fail(rows: list[dict], pass_fail: dict) -> tuple[bool, list[str]]:
    return _support_evaluate_pass_fail(rows, pass_fail, to_float_fn=to_float)


def evaluate_profile_gates(profile_name: str, resolved_profile: dict) -> tuple[int, list[str]]:
    return _support_evaluate_profile_gates(
        profile_name,
        resolved_profile,
        resolve_project_path=resolve_project_path,
        log_gate_event=_log_gate_event,
        load_csv_rows_fn=load_csv_rows,
        evaluate_pass_fail_fn=evaluate_pass_fail,
    )
