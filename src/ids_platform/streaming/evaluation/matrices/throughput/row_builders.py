from __future__ import annotations

from datetime import datetime, timezone


def _merge_quality_summary(row: dict, quality_summary: dict | None, *, averaged_names: bool) -> None:
    quality = quality_summary or {}
    row["quality_status"] = quality.get("quality_status", "")
    row["avg_prediction_score"] = quality.get("avg_prediction_score_weighted", "")
    row["attack_ratio"] = quality.get("attack_ratio_weighted", "")
    row["scored_rows_total"] = quality.get("scored_rows_total", "")
    if averaged_names:
        row["precision_avg"] = quality.get("precision", "")
        row["recall_avg"] = quality.get("recall", "")
        row["f1_avg"] = quality.get("f1", "")
        row["fpr_avg"] = quality.get("fpr", "")
        row["fnr_avg"] = quality.get("fnr", "")
        return
    row["precision"] = quality.get("precision", "")
    row["recall"] = quality.get("recall", "")
    row["f1"] = quality.get("f1", "")
    row["fpr"] = quality.get("fpr", "")
    row["fnr"] = quality.get("fnr", "")


def build_model_feature_tradeoff_summary_row(
    *,
    run_tag: str,
    repeat_index: int,
    model: str,
    feature_set: str,
    metrics_rows: list[dict],
    summarize_runtime_metrics_fn,
    quality_summary: dict | None = None,
    context: dict | None = None,
) -> dict:
    context = context or {}
    row = {
        "run_tag": run_tag,
        "repeat_index": repeat_index,
        "model": model,
        "feature_set": feature_set,
        "candidate_label": context.get("candidate_label", ""),
        "baseline_label": context.get("baseline_label", ""),
        "baseline_source": context.get("baseline_source", ""),
        "baseline_summary_csv": context.get("baseline_summary_csv", ""),
        "target_rps": context.get("target_rps", ""),
        "load_profile": context.get("candidate_label") or f"{model}:{feature_set}",
        "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "rows": "",
        "source_p95_ms": "",
        "ingest_to_emit_p95_ms": "",
        "source_to_emit_p95_ms": "",
        "proc_p95_ms": "",
        "e2e_p95_ms": "",
        "quality_status": "",
        "avg_prediction_score": "",
        "attack_ratio": "",
        "scored_rows_total": "",
        "precision": "",
        "recall": "",
        "f1": "",
        "fpr": "",
        "fnr": "",
        "late_event_ratio": "",
        "late_event_ratio_interpretable": "",
        "freshness_signal_ratio": "",
        "watermark_delay_sec": "",
        "kafka_lag_records": "",
        "driver_cpu_percent": "",
        "driver_rss_mb": "",
        "executor_mem_util_avg": "",
        "executor_mem_util_p95": "",
        "executor_count": "",
        "status": "ok" if metrics_rows else "metrics_missing",
    }

    if not metrics_rows:
        _merge_quality_summary(row, quality_summary, averaged_names=False)
        return row

    summary = summarize_runtime_metrics_fn(metrics_rows)
    last_payload = summary.get("last_payload") or {}

    row.update(
        {
            "rows": summary.get("rows_total", ""),
            "source_p95_ms": summary.get("source_p95_ms_max", ""),
            "ingest_to_emit_p95_ms": summary.get("ingest_to_emit_p95_ms_max", ""),
            "source_to_emit_p95_ms": summary.get("source_to_emit_p95_ms_max", ""),
            "proc_p95_ms": summary.get("proc_p95_ms_max", ""),
            "e2e_p95_ms": summary.get("e2e_p95_ms_max", ""),
        }
    )
    _merge_quality_summary(row, quality_summary, averaged_names=False)
    row["late_event_ratio"] = summary.get("late_event_ratio_weighted", "")
    row["late_event_ratio_interpretable"] = summary.get("late_event_ratio_interpretable_weighted", "")
    row["freshness_signal_ratio"] = summary.get("freshness_signal_ratio_weighted", "")
    row["watermark_delay_sec"] = summary.get("watermark_delay_sec", "")
    row["kafka_lag_records"] = summary.get("kafka_lag_records_max", "")
    row["driver_cpu_percent"] = summary.get("driver_cpu_percent_avg", "")
    row["driver_rss_mb"] = summary.get("driver_rss_mb_avg", "")
    row["executor_mem_util_avg"] = summary.get("executor_mem_util_avg", "")
    row["executor_mem_util_p95"] = summary.get("executor_mem_util_p95_avg", "")
    row["executor_count"] = summary.get("executor_count_max", "")
    if not row["driver_cpu_percent"]:
        row["driver_cpu_percent"] = ((last_payload.get("system") or {}).get("driver_cpu_percent", ""))
    if not row["driver_rss_mb"]:
        row["driver_rss_mb"] = ((last_payload.get("system") or {}).get("driver_rss_mb", ""))
    if not row["executor_mem_util_avg"]:
        row["executor_mem_util_avg"] = ((last_payload.get("system") or {}).get("executor_mem_util_avg", ""))
    if not row["executor_mem_util_p95"]:
        row["executor_mem_util_p95"] = ((last_payload.get("system") or {}).get("executor_mem_util_p95", ""))
    if not row["executor_count"]:
        row["executor_count"] = ((last_payload.get("system") or {}).get("executor_count", ""))
    return row
