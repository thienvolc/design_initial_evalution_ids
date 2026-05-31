from __future__ import annotations

import copy
from pathlib import Path


def deep_merge(parent: dict, child: dict) -> dict:
    merged = copy.deepcopy(parent)
    for key, value in child.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def resolve_profile(name: str, profiles: dict, stack: set[str] | None = None) -> dict:
    active_stack = stack or set()
    if name in active_stack:
        raise ValueError(f"Circular profile inheritance detected: {name}")
    if name not in profiles:
        raise ValueError(f"Profile '{name}' not found")

    active_stack.add(name)
    raw_profile = profiles[name] or {}
    if not isinstance(raw_profile, dict):
        raise ValueError(f"Profile '{name}' must be a mapping")

    parent_name = str(raw_profile.get("extends", "")).strip()
    if parent_name:
        parent_profile = resolve_profile(parent_name, profiles, active_stack)
        resolved_profile = deep_merge(parent_profile, raw_profile)
    else:
        resolved_profile = copy.deepcopy(raw_profile)

    active_stack.remove(name)
    resolved_profile.pop("extends", None)
    return resolved_profile


def to_flag(name: str) -> str:
    return "--" + name.replace("_", "-")


def append_arg(command: list[str], key: str, value) -> None:
    if value is None:
        return
    flag = to_flag(key)
    if isinstance(value, bool):
        if value:
            command.append(flag)
        return
    if isinstance(value, (list, tuple)):
        if not value:
            return
        command.append(flag)
        command.extend(str(item) for item in value)
        return
    command.extend([flag, str(value)])


CONFIG_DRIVEN_SCRIPTS = {
    "run_layer_a_matrix.py",
    "run_layer_b_matrix.py",
    "run_layer_c_matrix.py",
    "run_load_quality_matrix.py",
    "run_watermark_matrix.py",
    "replay_parquet_to_kafka.py",
    "run_structured_streaming.py",
}


def normalize_args_for_script(script: str, arguments: dict) -> dict:
    script_name = Path(script).name.lower()
    if script_name in CONFIG_DRIVEN_SCRIPTS:
        return {}
    return copy.deepcopy(arguments)


def resolve_allowed_script_path(script: str, *, resolve_project_path, project_root, allowed_script_roots: tuple[Path, ...]) -> str:
    resolved_script = resolve_project_path(script).resolve()
    for allowed_root in allowed_script_roots:
        try:
            resolved_script.relative_to(allowed_root)
            return resolved_script.relative_to(project_root).as_posix()
        except ValueError:
            continue

    allowed_roots = ", ".join(str(path) for path in allowed_script_roots)
    raise ValueError(
        f"Profile script must be under one of the allowed roots: {allowed_roots}. "
        f"Received: {resolved_script}"
    )


def build_command(
    profile: dict,
    python_executable: str,
    *,
    resolve_project_path,
    project_root,
    allowed_script_roots: tuple[Path, ...],
) -> list[str]:
    raw_script = str(profile.get("script", "")).strip()
    if not raw_script:
        raise ValueError("Profile requires 'script'")
    script = resolve_allowed_script_path(
        raw_script,
        resolve_project_path=resolve_project_path,
        project_root=project_root,
        allowed_script_roots=allowed_script_roots,
    )

    runtime = str(profile.get("runtime", "host")).strip().lower()
    if runtime == "docker":
        service = str(profile.get("docker_service", "ids-dev")).strip() or "ids-dev"
        command = ["docker", "compose", "exec", "-T", service, "python", script]
    elif runtime == "host":
        command = [python_executable, script]
    else:
        raise ValueError("Profile runtime must be 'host' or 'docker'")

    arguments = profile.get("args") or {}
    if not isinstance(arguments, dict):
        raise ValueError("Profile 'args' must be a mapping")

    arguments = normalize_args_for_script(script, arguments)
    for key, value in arguments.items():
        append_arg(command, str(key), value)
    return command


def profile_mode(profile: dict) -> str:
    mode = str(profile.get("mode", "")).strip().lower()
    if mode:
        return mode
    meta = profile.get("meta") or {}
    if isinstance(meta, dict):
        mode = str(meta.get("mode", "")).strip().lower()
        if mode:
            return mode
    return "official"


def profile_resource_class(profile: dict) -> str:
    resource_class = str(profile.get("resource_class", "")).strip().lower()
    if resource_class:
        return resource_class
    meta = profile.get("meta") or {}
    if isinstance(meta, dict):
        resource_class = str(meta.get("resource_class", "")).strip().lower()
        if resource_class:
            return resource_class
    return "light"


def include_profile_by_filters(profile: dict, args) -> bool:
    mode = profile_mode(profile)
    resource_class = profile_resource_class(profile)
    if args.list_official_only and mode != "official":
        return False
    if args.list_ablation_only and mode != "ablation":
        return False
    if args.list_light_only and resource_class != "light":
        return False
    if args.list_heavy_only and resource_class != "heavy":
        return False
    return True
