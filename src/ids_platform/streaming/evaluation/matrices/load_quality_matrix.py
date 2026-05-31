from __future__ import annotations

from datetime import datetime, timezone

from ids_platform.streaming.config.common import BenchmarkMatrixConfig
from ids_platform.streaming.evaluation.matrices.benchmark_matrix import run_benchmark_matrix
from ids_platform.streaming.evaluation.matrices.common import summarize_runtime_metrics


def _quality_fields(row: dict, quality_summary: dict) -> None:
    row["quality_status"] = quality_summary.get("quality_status", "")
    row["precision"] = quality_summary.get("precision", "")
    row["recall"] = quality_summary.get("recall", "")
    row["f1"] = quality_summary.get("f1", "")
    row["fpr"] = quality_summary.get("fpr", "")
    row["fnr"] = quality_summary.get("fnr", "")


def _summary_row(run_plan, benchmark, metrics_rows: list[dict], quality_summary: dict) -> dict:
    context = run_plan.summary_context or {}
    row = {
        "run_tag": benchmark.run_tag,
        "load_profile": context.get("load_profile", benchmark.runtime.run.load_profile),
        "rows_per_sec_target": context.get("rows_per_sec_target", ""),
        "max_rows": context.get("max_rows", benchmark.replay.source.table.num_rows),
        "rate_schedule": context.get("rate_schedule", ""),
        "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "rows": "",
        "source_p95_ms": "",
        "ingest_to_emit_p95_ms": "",
        "source_to_emit_p95_ms": "",
        "proc_p95_ms": "",
        "e2e_p95_ms": "",
        "rows_per_sec_actual": "",
        "quality_status": "",
        "precision": "",
        "recall": "",
        "f1": "",
        "fpr": "",
        "fnr": "",
        "kafka_lag_records": "",
        "late_event_ratio": "",
        "late_event_ratio_interpretable": "",
        "freshness_signal_ratio": "",
        "status": "ok" if metrics_rows else "metrics_missing",
    }
    _quality_fields(row, quality_summary)
    if not metrics_rows:
        return row

    summary = summarize_runtime_metrics(metrics_rows)
    row["rows"] = summary.get("rows_total", "")
    row["rows_per_sec_actual"] = summary.get("rows_per_sec_avg", "")
    row["source_p95_ms"] = summary.get("source_p95_ms_max", "")
    row["ingest_to_emit_p95_ms"] = summary.get("ingest_to_emit_p95_ms_max", "")
    row["source_to_emit_p95_ms"] = summary.get("source_to_emit_p95_ms_max", "")
    row["proc_p95_ms"] = summary.get("proc_p95_ms_max", "")
    row["e2e_p95_ms"] = summary.get("e2e_p95_ms_max", "")
    row["kafka_lag_records"] = summary.get("kafka_lag_records_max", "")
    row["late_event_ratio"] = summary.get("late_event_ratio_weighted", "")
    row["late_event_ratio_interpretable"] = summary.get("late_event_ratio_interpretable_weighted", "")
    row["freshness_signal_ratio"] = summary.get("freshness_signal_ratio_weighted", "")
    return row


def run_load_quality_matrix(config: BenchmarkMatrixConfig) -> int:
    return run_benchmark_matrix(config, label="Load quality", build_summary_row=_summary_row)


run = run_load_quality_matrix
