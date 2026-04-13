from __future__ import annotations

import copy
import csv
import math
from pathlib import Path

from ids_platform.common.paths import PROJECT_ROOT, resolve_project_path

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


def format_layer_a_profile(value: dict) -> str:
    if not isinstance(value, dict):
        raise ValueError("Layer A profile entries must be mappings")

    profile_name = str(value.get("name", "")).strip()
    max_offsets = value.get("max_offsets_per_trigger")
    shuffle_partitions = value.get("shuffle_partitions")
    trigger_interval = str(value.get("trigger_interval", "")).strip()

    if not profile_name:
        raise ValueError("Layer A profile is missing 'name'")
    if max_offsets is None or shuffle_partitions is None:
        raise ValueError("Layer A profile requires max_offsets_per_trigger and shuffle_partitions")

    base = f"{profile_name}:{int(max_offsets)}:{int(shuffle_partitions)}"
    if trigger_interval:
        return f"{base}:{trigger_interval}"
    return base


def normalize_args_for_script(script: str, arguments: dict) -> dict:
    normalized = copy.deepcopy(arguments)
    script_name = Path(script).name.lower()
    if script_name == "run_layer_a_matrix.py":
        profiles = normalized.get("profiles")
        if isinstance(profiles, list) and profiles and isinstance(profiles[0], dict):
            normalized["profiles"] = [format_layer_a_profile(item) for item in profiles]
    return normalized


def _resolve_allowed_script_path(script: str) -> str:
    resolved_script = resolve_project_path(script).resolve()
    for allowed_root in _ALLOWED_SCRIPT_ROOTS:
        try:
            resolved_script.relative_to(allowed_root)
            return resolved_script.relative_to(PROJECT_ROOT).as_posix()
        except ValueError:
            continue

    allowed_roots = ", ".join(str(path) for path in _ALLOWED_SCRIPT_ROOTS)
    raise ValueError(
        f"Profile script must be under one of the allowed roots: {allowed_roots}. "
        f"Received: {resolved_script}"
    )


def build_command(profile: dict, python_executable: str) -> list[str]:
    raw_script = str(profile.get("script", "")).strip()
    if not raw_script:
        raise ValueError("Profile requires 'script'")
    script = _resolve_allowed_script_path(raw_script)

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


def to_float(value) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = float(text)
    except ValueError:
        return None
    if math.isnan(parsed) or math.isinf(parsed):
        return None
    return parsed


def load_csv_rows(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def extract_series(rows: list[dict], candidates: list[str]) -> tuple[list[float], str]:
    for column_name in candidates:
        values = [to_float(row.get(column_name)) for row in rows]
        numeric_values = [value for value in values if value is not None]
        if numeric_values:
            return numeric_values, column_name
    return [], ""


def sum_rows_total(rows: list[dict]) -> tuple[float, str]:
    for column_name in ["rows_total", "rows", "rows_after_fault"]:
        total = 0.0
        found = False
        for row in rows:
            value = to_float(row.get(column_name))
            if value is None:
                continue
            total += value
            found = True
        if found:
            return total, column_name
    return 0.0, ""


def evaluate_pass_fail(rows: list[dict], pass_fail: dict) -> tuple[bool, list[str]]:
    checks: list[tuple[bool, str]] = []

    statuses = [str(row.get("status", "")).strip().lower() for row in rows if "status" in row]
    invalid_statuses = [status for status in statuses if status and status not in {"ok", "pass"}]
    if invalid_statuses:
        checks.append((False, f"status gate: non-ok rows found {sorted(set(invalid_statuses))}"))
    elif statuses:
        checks.append((True, "status gate: all rows are ok/pass"))

    for key, expected in pass_fail.items():
        if key == "required":
            continue

        expected_num = to_float(expected)
        if expected_num is None:
            checks.append((False, f"{key}: threshold is not numeric ({expected!r})"))
            continue

        if key == "min_rows_total":
            actual, column_name = sum_rows_total(rows)
            if not column_name:
                checks.append((False, f"{key}: missing rows_total/rows/rows_after_fault column"))
            else:
                checks.append((actual >= expected_num, f"{key}: actual={actual:.3f} from {column_name} threshold>={expected_num:.3f}"))
            continue

        if key == "max_fnr":
            values, column_name = extract_series(rows, ["fnr", "fnr_avg"])
            if not values:
                checks.append((False, f"{key}: missing fnr/fnr_avg column"))
            else:
                actual = max(values)
                checks.append((actual <= expected_num, f"{key}: actual={actual:.6f} from {column_name} threshold<={expected_num:.6f}"))
            continue

        if key == "max_fpr":
            values, column_name = extract_series(rows, ["fpr", "fpr_avg"])
            if not values:
                checks.append((False, f"{key}: missing fpr/fpr_avg column"))
            else:
                actual = max(values)
                checks.append((actual <= expected_num, f"{key}: actual={actual:.6f} from {column_name} threshold<={expected_num:.6f}"))
            continue

        if key == "max_e2e_p95_ms":
            values, column_name = extract_series(rows, ["e2e_p95_ms_max", "e2e_p95_ms", "e2e_p95_ms_after_fault"])
            if not values:
                checks.append((False, f"{key}: missing e2e p95 column"))
            else:
                actual = max(values)
                checks.append((actual <= expected_num, f"{key}: actual={actual:.3f} from {column_name} threshold<={expected_num:.3f}"))
            continue

        if key == "min_rows_per_sec_avg":
            values, column_name = extract_series(rows, ["rows_per_sec_avg", "rows_per_sec_actual", "rows_per_sec_after_fault"])
            if not values:
                checks.append((False, f"{key}: missing rows_per_sec column"))
            else:
                actual = sum(values) / len(values)
                checks.append((actual >= expected_num, f"{key}: actual={actual:.3f} from {column_name} threshold>={expected_num:.3f}"))
            continue

        if key == "min_rows_per_sec_actual":
            values, column_name = extract_series(rows, ["rows_per_sec_actual", "rows_per_sec_after_fault", "rows_per_sec_avg"])
            if not values:
                checks.append((False, f"{key}: missing rows_per_sec column"))
            else:
                actual = min(values)
                checks.append((actual >= expected_num, f"{key}: actual={actual:.3f} from {column_name} threshold>={expected_num:.3f}"))
            continue

        if key == "max_lag_records_peak":
            values, column_name = extract_series(rows, ["kafka_lag_records_max", "kafka_lag_records"])
            if not values:
                checks.append((False, f"{key}: missing kafka lag column"))
            else:
                actual = max(values)
                checks.append((actual <= expected_num, f"{key}: actual={actual:.3f} from {column_name} threshold<={expected_num:.3f}"))
            continue

        if key == "min_recovery_events":
            recovery_count = 0
            for row in rows:
                if to_float(row.get("recovery_seconds")) is not None:
                    recovery_count += 1
            actual = float(recovery_count)
            checks.append((actual >= expected_num, f"{key}: actual={actual:.3f} threshold>={expected_num:.3f}"))
            continue

        if key == "max_recovery_seconds":
            values, column_name = extract_series(rows, ["recovery_seconds"])
            if not values:
                checks.append((False, f"{key}: missing recovery_seconds column"))
            else:
                actual = max(values)
                checks.append((actual <= expected_num, f"{key}: actual={actual:.3f} from {column_name} threshold<={expected_num:.3f}"))
            continue

        if key == "max_dead_letter_ratio":
            values, column_name = extract_series(rows, ["dead_letter_ratio", "dead_letter_ratio_avg", "dead_letter_ratio_max"])
            if not values:
                checks.append((False, f"{key}: missing dead-letter ratio column"))
            else:
                actual = max(values)
                checks.append((actual <= expected_num, f"{key}: actual={actual:.6f} from {column_name} threshold<={expected_num:.6f}"))
            continue

        if key.startswith("max_late_event_ratio_delay_"):
            delay_text = key.replace("max_late_event_ratio_delay_", "", 1)
            delay_num = to_float(delay_text)
            if delay_num is None:
                checks.append((False, f"{key}: invalid delay suffix"))
                continue

            scoped_values: list[float] = []
            for row in rows:
                delay_value = to_float(row.get("watermark_delay_sec"))
                metric_value = to_float(row.get("late_event_ratio"))
                if delay_value is None or metric_value is None:
                    continue
                if int(delay_value) == int(delay_num):
                    scoped_values.append(metric_value)

            if not scoped_values:
                checks.append((False, f"{key}: no rows found for watermark_delay_sec={int(delay_num)}"))
            else:
                actual = max(scoped_values)
                checks.append((actual <= expected_num, f"{key}: actual={actual:.6f} from late_event_ratio at delay={int(delay_num)} threshold<={expected_num:.6f}"))
            continue

        checks.append((False, f"{key}: unsupported gate key"))

    passed = all(ok for ok, _ in checks) if checks else True
    details = [f"[{'PASS' if ok else 'FAIL'}] {message}" for ok, message in checks]
    return passed, details


def evaluate_profile_gates(profile_name: str, resolved_profile: dict) -> tuple[int, list[str]]:
    meta = resolved_profile.get("meta") or {}
    pass_fail = meta.get("pass_fail") if isinstance(meta, dict) else None
    if not isinstance(pass_fail, dict):
        _log_gate_event("skip_no_pass_fail", profile=profile_name)
        return 0, []

    required = bool(pass_fail.get("required", False))
    thresholds = {key: value for key, value in pass_fail.items() if key != "required"}
    if not thresholds:
        _log_gate_event("skip_no_thresholds", profile=profile_name)
        return 0, []

    profile_args = resolved_profile.get("args") or {}
    summary_csv = profile_args.get("summary_csv") if isinstance(profile_args, dict) else None
    if not summary_csv:
        message = "Gate evaluation skipped: profile args.summary_csv is missing."
        _log_gate_event("skip_missing_summary_arg", profile=profile_name)
        return (3 if required else 0), [message]

    summary_path = resolve_project_path(str(summary_csv))
    _log_gate_event(
        "start",
        profile=profile_name,
        required=required,
        summary_csv=summary_path,
        thresholds=",".join(sorted(thresholds.keys())),
    )
    if not summary_path.exists():
        message = f"Gate evaluation failed: summary CSV not found at {summary_path}"
        _log_gate_event("summary_missing", profile=profile_name, summary_csv=summary_path)
        return (3 if required else 0), [message]

    rows = load_csv_rows(summary_path)
    if not rows:
        message = f"Gate evaluation failed: summary CSV is empty at {summary_path}"
        _log_gate_event("summary_empty", profile=profile_name, summary_csv=summary_path)
        return (3 if required else 0), [message]

    gate_ok, gate_lines = evaluate_pass_fail(rows, pass_fail)
    verdict = "PASS" if gate_ok else "FAIL"
    enforce_text = "required" if required else "advisory"
    header = f"Gate evaluation for profile '{profile_name}': {verdict} ({enforce_text})"
    exit_code = 3 if required and not gate_ok else 0
    _log_gate_event(
        "end",
        profile=profile_name,
        verdict=verdict,
        required=required,
        rows=len(rows),
        exit_code=exit_code,
    )
    return exit_code, [header, *[f"  {line}" for line in gate_lines]]
