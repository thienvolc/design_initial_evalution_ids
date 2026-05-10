from __future__ import annotations

from datetime import datetime, timezone


def build_layer_a_summary_row(
    *,
    run_tag: str,
    repeat_index: int,
    model: str,
    feature_set: str,
    profile: dict,
    metrics_rows: list[dict],
    summarize_runtime_metrics_fn,
) -> dict:
    row = {
        "run_tag": run_tag,
        "repeat_index": repeat_index,
        "model": model,
        "feature_set": feature_set,
        "profile": profile["name"],
        "max_offsets_per_trigger": profile["max_offsets_per_trigger"],
        "shuffle_partitions": profile["shuffle_partitions"],
        "trigger_interval": profile.get("trigger_interval") or "",
        "load_profile": profile["name"],
        "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "batches": 0,
        "rows_total": 0,
        "rows_per_sec_avg": "",
        "batch_wall_ms_avg": "",
        "source_p95_ms_max": "",
        "ingest_to_emit_p95_ms_max": "",
        "source_to_emit_p95_ms_max": "",
        "proc_p95_ms_max": "",
        "e2e_p95_ms_max": "",
        "avg_prediction_score": "",
        "attack_ratio": "",
        "scored_rows_total": "",
        "precision_avg": "",
        "recall_avg": "",
        "f1_avg": "",
        "fpr_avg": "",
        "fnr_avg": "",
        "late_event_ratio_avg": "",
        "late_event_ratio_interpretable_avg": "",
        "freshness_signal_ratio_avg": "",
        "watermark_delay_sec": "",
        "kafka_lag_records_max": "",
        "driver_cpu_percent_avg": "",
        "driver_rss_mb_avg": "",
        "executor_mem_util_avg": "",
        "executor_mem_util_p95_avg": "",
        "executor_count_max": "",
        "metric_warnings": "",
        "status": "ok" if metrics_rows else "metrics_missing",
    }

    if not metrics_rows:
        return row

    summary = summarize_runtime_metrics_fn(metrics_rows)
    row["batches"] = int(summary.get("batch_count") or 0)
    row["rows_total"] = summary.get("rows_total", "")
    row["rows_per_sec_avg"] = summary.get("rows_per_sec_avg", "")
    row["batch_wall_ms_avg"] = summary.get("batch_wall_ms_avg", "")
    row["source_p95_ms_max"] = summary.get("source_p95_ms_max", "")
    row["ingest_to_emit_p95_ms_max"] = summary.get("ingest_to_emit_p95_ms_max", "")
    row["source_to_emit_p95_ms_max"] = summary.get("source_to_emit_p95_ms_max", "")
    row["proc_p95_ms_max"] = summary.get("proc_p95_ms_max", "")
    row["e2e_p95_ms_max"] = summary.get("e2e_p95_ms_max", "")
    row["avg_prediction_score"] = summary.get("avg_prediction_score_weighted", "")
    row["attack_ratio"] = summary.get("attack_ratio_weighted", "")
    row["scored_rows_total"] = summary.get("scored_rows_total", "")
    row["precision_avg"] = summary.get("precision", "")
    row["recall_avg"] = summary.get("recall", "")
    row["f1_avg"] = summary.get("f1", "")
    row["fpr_avg"] = summary.get("fpr", "")
    row["fnr_avg"] = summary.get("fnr", "")
    row["late_event_ratio_avg"] = summary.get("late_event_ratio_weighted", "")
    row["late_event_ratio_interpretable_avg"] = summary.get("late_event_ratio_interpretable_weighted", "")
    row["freshness_signal_ratio_avg"] = summary.get("freshness_signal_ratio_weighted", "")
    row["watermark_delay_sec"] = summary.get("watermark_delay_sec", "")
    row["kafka_lag_records_max"] = summary.get("kafka_lag_records_max", "")
    row["driver_cpu_percent_avg"] = summary.get("driver_cpu_percent_avg", "")
    row["driver_rss_mb_avg"] = summary.get("driver_rss_mb_avg", "")
    row["executor_mem_util_avg"] = summary.get("executor_mem_util_avg", "")
    row["executor_mem_util_p95_avg"] = summary.get("executor_mem_util_p95_avg", "")
    row["executor_count_max"] = summary.get("executor_count_max", "")
    row["metric_warnings"] = "; ".join(summary.get("metric_warnings") or [])
    return row


def build_layer_b_summary_row(
    *,
    run_tag: str,
    repeat_index: int,
    model: str,
    feature_set: str,
    metrics_rows: list[dict],
    summarize_runtime_metrics_fn,
) -> dict:
    row = {
        "run_tag": run_tag,
        "repeat_index": repeat_index,
        "model": model,
        "feature_set": feature_set,
        "load_profile": f"{model}:{feature_set}",
        "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "rows": "",
        "source_p95_ms": "",
        "ingest_to_emit_p95_ms": "",
        "source_to_emit_p95_ms": "",
        "proc_p95_ms": "",
        "e2e_p95_ms": "",
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
        "metric_warnings": "",
        "status": "ok" if metrics_rows else "metrics_missing",
    }

    if not metrics_rows:
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
            "avg_prediction_score": summary.get("avg_prediction_score_weighted", ""),
            "attack_ratio": summary.get("attack_ratio_weighted", ""),
            "scored_rows_total": summary.get("scored_rows_total", ""),
        }
    )
    row["precision"] = summary.get("precision", "")
    row["recall"] = summary.get("recall", "")
    row["f1"] = summary.get("f1", "")
    row["fpr"] = summary.get("fpr", "")
    row["fnr"] = summary.get("fnr", "")
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
    row["metric_warnings"] = "; ".join(summary.get("metric_warnings") or [])
    return row
