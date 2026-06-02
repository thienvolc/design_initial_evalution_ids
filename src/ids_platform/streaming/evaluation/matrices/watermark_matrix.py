from __future__ import annotations

from datetime import datetime, timezone

from ids_platform.streaming.config.common import BenchmarkMatrixConfig
from ids_platform.streaming.evaluation.matrices.benchmark_matrix import run_benchmark_matrix
from ids_platform.streaming.evaluation.matrices.common import (
    apply_quality_summary_fields,
    summarize_runtime_metrics,
)


def _summary_row(run_plan, benchmark, metrics_rows: list[dict], quality_summary: dict) -> dict:
    context = run_plan.summary_context or {}
    delay_sec = int(context.get("watermark_delay_sec", benchmark.runtime.lifecycle.watermark_delay_sec))
    row = {
        "run_tag": benchmark.run_tag,
        "load_profile": benchmark.runtime.run.load_profile,
        "watermark_delay_sec": delay_sec,
        "drop_late_events": bool(context.get("drop_late_events", benchmark.runtime.lifecycle.drop_late_events)),
        "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "rows": "",
        "late_event_ratio": "",
        "late_event_ratio_interpretable": "",
        "freshness_signal_ratio": "",
        "event_lateness_p95_ms": "",
        "source_p95_ms": "",
        "ingest_to_emit_p95_ms": "",
        "source_to_emit_p95_ms": "",
        "proc_p95_ms": "",
        "e2e_p95_ms": "",
        "quality_status": "",
        "precision": "",
        "recall": "",
        "f1": "",
        "fpr": "",
        "fnr": "",
        "status": "ok" if metrics_rows else "metrics_missing",
    }
    apply_quality_summary_fields(row, quality_summary)
    if not metrics_rows:
        return row

    summary = summarize_runtime_metrics(metrics_rows)
    row["rows"] = summary.get("rows_total", "")
    row["late_event_ratio"] = summary.get("late_event_ratio_weighted", "")
    row["late_event_ratio_interpretable"] = summary.get("late_event_ratio_interpretable_weighted", "")
    row["freshness_signal_ratio"] = summary.get("freshness_signal_ratio_weighted", "")
    row["event_lateness_p95_ms"] = summary.get("event_lateness_p95_ms_max", "")
    row["source_p95_ms"] = summary.get("source_p95_ms_max", "")
    row["ingest_to_emit_p95_ms"] = summary.get("ingest_to_emit_p95_ms_max", "")
    row["source_to_emit_p95_ms"] = summary.get("source_to_emit_p95_ms_max", "")
    row["proc_p95_ms"] = summary.get("proc_p95_ms_max", "")
    row["e2e_p95_ms"] = summary.get("e2e_p95_ms_max", "")
    return row


def run_watermark_matrix(config: BenchmarkMatrixConfig) -> int:
    return run_benchmark_matrix(config, label="Watermark", build_summary_row=_summary_row)


run = run_watermark_matrix
