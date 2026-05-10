from __future__ import annotations

import csv
import math
from pathlib import Path


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


def extract_series(rows: list[dict], candidates: list[str], *, to_float_fn) -> tuple[list[float], str]:
    for column_name in candidates:
        values = [to_float_fn(row.get(column_name)) for row in rows]
        numeric_values = [value for value in values if value is not None]
        if numeric_values:
            return numeric_values, column_name
    return [], ""


def sum_rows_total(rows: list[dict], *, to_float_fn) -> tuple[float, str]:
    for column_name in ["rows_total", "rows", "rows_after_fault"]:
        total = 0.0
        found = False
        for row in rows:
            value = to_float_fn(row.get(column_name))
            if value is None:
                continue
            total += value
            found = True
        if found:
            return total, column_name
    return 0.0, ""


def evaluate_pass_fail(rows: list[dict], pass_fail: dict, *, to_float_fn) -> tuple[bool, list[str]]:
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

        expected_num = to_float_fn(expected)
        if expected_num is None:
            checks.append((False, f"{key}: threshold is not numeric ({expected!r})"))
            continue

        if key == "min_rows_total":
            actual, column_name = sum_rows_total(rows, to_float_fn=to_float_fn)
            if not column_name:
                checks.append((False, f"{key}: missing rows_total/rows/rows_after_fault column"))
            else:
                checks.append((actual >= expected_num, f"{key}: actual={actual:.3f} from {column_name} threshold>={expected_num:.3f}"))
            continue

        if key == "max_fnr":
            values, column_name = extract_series(rows, ["fnr", "fnr_avg"], to_float_fn=to_float_fn)
            if not values:
                checks.append((False, f"{key}: missing fnr/fnr_avg column"))
            else:
                actual = max(values)
                checks.append((actual <= expected_num, f"{key}: actual={actual:.6f} from {column_name} threshold<={expected_num:.6f}"))
            continue

        if key == "max_fpr":
            values, column_name = extract_series(rows, ["fpr", "fpr_avg"], to_float_fn=to_float_fn)
            if not values:
                checks.append((False, f"{key}: missing fpr/fpr_avg column"))
            else:
                actual = max(values)
                checks.append((actual <= expected_num, f"{key}: actual={actual:.6f} from {column_name} threshold<={expected_num:.6f}"))
            continue

        if key in {"max_source_to_emit_p95_ms", "max_e2e_p95_ms"}:
            values, column_name = extract_series(
                rows,
                [
                    "source_to_emit_p95_ms_max",
                    "source_to_emit_p95_ms",
                    "source_to_emit_p95_ms_after_fault",
                    "e2e_p95_ms_max",
                    "e2e_p95_ms",
                    "e2e_p95_ms_after_fault",
                ],
                to_float_fn=to_float_fn,
            )
            if not values:
                checks.append((False, f"{key}: missing source-to-emit/e2e p95 column"))
            else:
                actual = max(values)
                checks.append((actual <= expected_num, f"{key}: actual={actual:.3f} from {column_name} threshold<={expected_num:.3f}"))
            continue

        if key in {"max_ingest_to_emit_p95_ms", "max_proc_p95_ms"}:
            values, column_name = extract_series(
                rows,
                [
                    "ingest_to_emit_p95_ms_max",
                    "ingest_to_emit_p95_ms",
                    "ingest_to_emit_p95_ms_after_fault",
                    "proc_p95_ms_max",
                    "proc_p95_ms",
                    "proc_p95_ms_after_fault",
                ],
                to_float_fn=to_float_fn,
            )
            if not values:
                checks.append((False, f"{key}: missing ingest-to-emit/proc p95 column"))
            else:
                actual = max(values)
                checks.append((actual <= expected_num, f"{key}: actual={actual:.3f} from {column_name} threshold<={expected_num:.3f}"))
            continue

        if key == "min_rows_per_sec_avg":
            values, column_name = extract_series(rows, ["rows_per_sec_avg", "rows_per_sec_actual", "rows_per_sec_after_fault"], to_float_fn=to_float_fn)
            if not values:
                checks.append((False, f"{key}: missing rows_per_sec column"))
            else:
                actual = sum(values) / len(values)
                checks.append((actual >= expected_num, f"{key}: actual={actual:.3f} from {column_name} threshold>={expected_num:.3f}"))
            continue

        if key == "min_rows_per_sec_actual":
            values, column_name = extract_series(rows, ["rows_per_sec_actual", "rows_per_sec_after_fault", "rows_per_sec_avg"], to_float_fn=to_float_fn)
            if not values:
                checks.append((False, f"{key}: missing rows_per_sec column"))
            else:
                actual = min(values)
                checks.append((actual >= expected_num, f"{key}: actual={actual:.3f} from {column_name} threshold>={expected_num:.3f}"))
            continue

        if key == "max_lag_records_peak":
            values, column_name = extract_series(rows, ["kafka_lag_records_max", "kafka_lag_records"], to_float_fn=to_float_fn)
            if not values:
                checks.append((False, f"{key}: missing kafka lag column"))
            else:
                actual = max(values)
                checks.append((actual <= expected_num, f"{key}: actual={actual:.3f} from {column_name} threshold<={expected_num:.3f}"))
            continue

        if key == "min_recovery_events":
            recovery_count = 0
            for row in rows:
                if to_float_fn(row.get("recovery_seconds")) is not None:
                    recovery_count += 1
            actual = float(recovery_count)
            checks.append((actual >= expected_num, f"{key}: actual={actual:.3f} threshold>={expected_num:.3f}"))
            continue

        if key == "max_recovery_seconds":
            values, column_name = extract_series(rows, ["recovery_seconds"], to_float_fn=to_float_fn)
            if not values:
                checks.append((False, f"{key}: missing recovery_seconds column"))
            else:
                actual = max(values)
                checks.append((actual <= expected_num, f"{key}: actual={actual:.3f} from {column_name} threshold<={expected_num:.3f}"))
            continue

        if key == "max_dead_letter_ratio":
            values, column_name = extract_series(rows, ["dead_letter_ratio", "dead_letter_ratio_avg", "dead_letter_ratio_max"], to_float_fn=to_float_fn)
            if not values:
                checks.append((False, f"{key}: missing dead-letter ratio column"))
            else:
                actual = max(values)
                checks.append((actual <= expected_num, f"{key}: actual={actual:.6f} from {column_name} threshold<={expected_num:.6f}"))
            continue

        if key.startswith("max_late_event_ratio_delay_"):
            delay_text = key.replace("max_late_event_ratio_delay_", "", 1)
            delay_num = to_float_fn(delay_text)
            if delay_num is None:
                checks.append((False, f"{key}: invalid delay suffix"))
                continue

            scoped_values: list[float] = []
            for row in rows:
                delay_value = to_float_fn(row.get("watermark_delay_sec"))
                metric_value = to_float_fn(row.get("late_event_ratio"))
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


def evaluate_profile_gates(
    profile_name: str,
    resolved_profile: dict,
    *,
    resolve_project_path,
    log_gate_event,
    load_csv_rows_fn,
    evaluate_pass_fail_fn,
) -> tuple[int, list[str]]:
    meta = resolved_profile.get("meta") or {}
    pass_fail = meta.get("pass_fail") if isinstance(meta, dict) else None
    if not isinstance(pass_fail, dict):
        log_gate_event("skip_no_pass_fail", profile=profile_name)
        return 0, []

    required = bool(pass_fail.get("required", False))
    thresholds = {key: value for key, value in pass_fail.items() if key != "required"}
    if not thresholds:
        log_gate_event("skip_no_thresholds", profile=profile_name)
        return 0, []

    profile_args = resolved_profile.get("args") or {}
    summary_csv = profile_args.get("summary_csv") if isinstance(profile_args, dict) else None
    if not summary_csv:
        message = "Gate evaluation skipped: profile args.summary_csv is missing."
        log_gate_event("skip_missing_summary_arg", profile=profile_name)
        return (3 if required else 0), [message]

    summary_path = resolve_project_path(str(summary_csv))
    log_gate_event(
        "start",
        profile=profile_name,
        required=required,
        summary_csv=summary_path,
        thresholds=",".join(sorted(thresholds.keys())),
    )
    if not summary_path.exists():
        message = f"Gate evaluation failed: summary CSV not found at {summary_path}"
        log_gate_event("summary_missing", profile=profile_name, summary_csv=summary_path)
        return (3 if required else 0), [message]

    rows = load_csv_rows_fn(summary_path)
    if not rows:
        message = f"Gate evaluation failed: summary CSV is empty at {summary_path}"
        log_gate_event("summary_empty", profile=profile_name, summary_csv=summary_path)
        return (3 if required else 0), [message]

    gate_ok, gate_lines = evaluate_pass_fail_fn(rows, pass_fail)
    verdict = "PASS" if gate_ok else "FAIL"
    enforce_text = "required" if required else "advisory"
    header = f"Gate evaluation for profile '{profile_name}': {verdict} ({enforce_text})"
    exit_code = 3 if required and not gate_ok else 0
    log_gate_event(
        "end",
        profile=profile_name,
        verdict=verdict,
        required=required,
        rows=len(rows),
        exit_code=exit_code,
    )
    return exit_code, [header, *[f"  {line}" for line in gate_lines]]
