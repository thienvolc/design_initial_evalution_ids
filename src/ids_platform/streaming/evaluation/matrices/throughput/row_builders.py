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
        "kafka_lag_records": "",
        "driver_cpu_percent": "",
        "driver_rss_mb": "",
        "executor_mem_util_avg": "",
        "executor_mem_util_p95": "",
        "executor_count": "",
        "status": "ok" if quality_summary else "quality_missing",
    }

    _merge_quality_summary(row, quality_summary, averaged_names=False)
    return row
