from __future__ import annotations

import sys
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Callable

from ids_platform.common.config import load_yaml_mapping
from ids_platform.common.paths import PROJECT_ROOT, resolve_project_path
from ids_platform.common.subprocess import run_command
from ids_platform.streaming.evaluation.orchestration.profile_runner import (
    build_command,
    evaluate_profile_gates,
    include_profile_by_filters,
    profile_mode,
    profile_resource_class,
    resolve_profile,
)


@dataclass(frozen=True)
class ProfileListFilters:
    official_only: bool = False
    ablation_only: bool = False
    light_only: bool = False
    heavy_only: bool = False


@dataclass(frozen=True)
class ProfileListItem:
    name: str
    description: str
    mode: str
    resource_class: str


@dataclass(frozen=True)
class ProfileExecutionOptions:
    profile_config_path: str = "experiments/streaming/profiles/local_profiles.yaml"
    profile_name: str = ""
    python_executable: str = sys.executable
    allow_heavy: bool = False
    skip_gates: bool = False
    gate_only: bool = False
    dry_run: bool = False


@dataclass(frozen=True)
class ProfileExecutionResult:
    exit_code: int
    command: list[str]
    resolved_profile: dict
    gate_lines: list[str]
    blocked_reason: str = ""
    executed: bool = False


RunCommandFn = Callable[..., object]


def _log_profile_event(event: str, **fields) -> None:
    parts = [f"[profile] event={event}"]
    for key, value in fields.items():
        if value is None:
            continue
        parts.append(f"{key}={value}")
    print(" ".join(parts), flush=True)


def _run_profile_command(
    run_command_fn: RunCommandFn,
    command: list[str],
    *,
    cwd: Path,
):
    try:
        return run_command_fn(command, cwd=cwd, stream_output=True)
    except TypeError:
        return run_command_fn(command, cwd=cwd)


@lru_cache(maxsize=8)
def _load_profiles_cached(profile_config_path: str) -> dict:
    config = load_yaml_mapping(resolve_project_path(profile_config_path))
    profiles = config.get("profiles") or {}
    if not isinstance(profiles, dict):
        raise ValueError("'profiles' must be a mapping")
    return profiles


def load_profiles(profile_config_path: str | Path) -> dict:
    return _load_profiles_cached(str(profile_config_path))


def _validate_list_filters(filters: ProfileListFilters) -> None:
    if filters.light_only and filters.heavy_only:
        raise ValueError("Use only one of light_only or heavy_only")
    if filters.official_only and filters.ablation_only:
        raise ValueError("Use only one of official_only or ablation_only")


def list_profiles(profile_config_path: str | Path, filters: ProfileListFilters) -> list[ProfileListItem]:
    _validate_list_filters(filters)
    profiles = load_profiles(profile_config_path)

    filter_args = type(
        "FilterArgs",
        (),
        {
            "list_official_only": filters.official_only,
            "list_ablation_only": filters.ablation_only,
            "list_light_only": filters.light_only,
            "list_heavy_only": filters.heavy_only,
        },
    )()

    items: list[ProfileListItem] = []
    for profile_name in sorted(profiles.keys()):
        if profile_name.startswith("_"):
            continue

        resolved_profile = resolve_profile(profile_name, profiles)
        if not include_profile_by_filters(resolved_profile, filter_args):
            continue

        items.append(
            ProfileListItem(
                name=profile_name,
                description=str(resolved_profile.get("description", "")).strip(),
                mode=profile_mode(resolved_profile),
                resource_class=profile_resource_class(resolved_profile),
            )
        )
    return items


def run_profile(
    options: ProfileExecutionOptions,
    *,
    run_command_fn: RunCommandFn = run_command,
    project_root: Path = PROJECT_ROOT,
) -> ProfileExecutionResult:
    if options.skip_gates and options.gate_only:
        raise ValueError("skip_gates cannot be combined with gate_only")
    if not options.profile_name.strip():
        raise ValueError("profile_name is required")

    profiles = load_profiles(options.profile_config_path)
    profile_name = options.profile_name.strip()
    resolved_profile = resolve_profile(profile_name, profiles)
    resource_class = profile_resource_class(resolved_profile)
    mode = profile_mode(resolved_profile)
    _log_profile_event(
        "resolved",
        profile=profile_name,
        mode=mode,
        resource_class=resource_class,
        dry_run=options.dry_run,
        gate_only=options.gate_only,
        skip_gates=options.skip_gates,
    )

    if (not options.dry_run) and (not options.gate_only) and resource_class == "heavy" and (not options.allow_heavy):
        blocked_reason = (
            f"Blocked heavy profile '{profile_name}' (mode={mode}, resource_class={resource_class}).\n"
            "Use --allow-heavy to execute this profile, or use --dry-run to inspect command."
        )
        _log_profile_event("blocked", profile=profile_name, mode=mode, resource_class=resource_class)
        return ProfileExecutionResult(
            exit_code=2,
            command=[],
            resolved_profile=resolved_profile,
            gate_lines=[],
            blocked_reason=blocked_reason,
            executed=False,
        )

    command = build_command(resolved_profile, options.python_executable)
    _log_profile_event("command_built", profile=profile_name, command=" ".join(command))

    if options.gate_only:
        _log_profile_event("gate_only_start", profile=profile_name)
        exit_code, gate_lines = evaluate_profile_gates(profile_name, resolved_profile)
        _log_profile_event("gate_only_end", profile=profile_name, exit_code=exit_code)
        return ProfileExecutionResult(
            exit_code=exit_code,
            command=command,
            resolved_profile=resolved_profile,
            gate_lines=gate_lines,
            executed=False,
        )

    if options.dry_run:
        _log_profile_event("dry_run", profile=profile_name)
        return ProfileExecutionResult(
            exit_code=0,
            command=command,
            resolved_profile=resolved_profile,
            gate_lines=[],
            executed=False,
        )

    _log_profile_event("execute_start", profile=profile_name)
    run_result = _run_profile_command(run_command_fn, command, cwd=project_root)
    run_exit_code = int(getattr(run_result, "returncode", 1))
    _log_profile_event("execute_end", profile=profile_name, exit_code=run_exit_code)
    if run_exit_code != 0:
        return ProfileExecutionResult(
            exit_code=run_exit_code,
            command=command,
            resolved_profile=resolved_profile,
            gate_lines=[],
            executed=True,
        )

    if options.skip_gates:
        _log_profile_event("gates_skipped", profile=profile_name)
        return ProfileExecutionResult(
            exit_code=0,
            command=command,
            resolved_profile=resolved_profile,
            gate_lines=[],
            executed=True,
        )

    _log_profile_event("gate_start", profile=profile_name)
    exit_code, gate_lines = evaluate_profile_gates(profile_name, resolved_profile)
    _log_profile_event("gate_end", profile=profile_name, exit_code=exit_code)
    return ProfileExecutionResult(
        exit_code=exit_code,
        command=command,
        resolved_profile=resolved_profile,
        gate_lines=gate_lines,
        executed=True,
    )
