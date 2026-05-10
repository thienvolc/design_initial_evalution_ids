from __future__ import annotations

import time

from ids_platform.streaming.evaluation.matrices.common import annotate_sut_debug_summary


def _log_phase(event: str, **fields) -> None:
    parts = [f"[phase] matrix=layer_c event={event}"]
    for key, value in fields.items():
        if value is None:
            continue
        parts.append(f"{key}={value}")
    print(" ".join(parts), flush=True)


def _next_run_tag(*, index: int, scenario: str) -> str:
    return f"layerC_{index:02d}_{scenario}_{int(time.time() * 1000)}"


def _mark_row_failed(row: dict, notes: str) -> None:
    row["status"] = "failed"
    row["notes"] = str(notes).strip()


def _mark_row_succeeded(row: dict) -> None:
    row["status"] = "ok"
    row["notes"] = ""


def _finalize_scenario_row(row: dict) -> dict:
    if str(row.get("status", "")).strip().lower() == "ok":
        row["notes"] = ""
    return annotate_sut_debug_summary(row)
