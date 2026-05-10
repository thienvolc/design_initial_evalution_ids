from __future__ import annotations

import csv
import re
import statistics
from pathlib import Path


TIMESERIES_FIELDNAMES = [
    "ts_utc",
    "ts_epoch_ms",
    "batch_id",
    "run_tag",
    "load_profile",
    "model_name",
    "feature_set",
    "metric_warnings",
    "rows",
    "rows_per_sec",
    "batch_wall_ms",
    "source_p95_ms",
    "ingest_to_emit_p95_ms",
    "source_to_emit_p95_ms",
    "proc_p95_ms",
    "e2e_p95_ms",
    "event_lateness_p95_ms",
    "late_event_ratio",
    "late_event_ratio_interpretable",
    "freshness_signal_ratio",
    "watermark_delay_sec",
    "kafka_lag_records_total",
    "kafka_lag_records_max_partition",
    "driver_cpu_percent",
    "driver_rss_mb",
    "executor_mem_util_avg",
    "executor_mem_util_p95",
    "executor_count",
    "labeled_rows",
    "scored_rows",
    "tp",
    "tn",
    "fp",
    "fn",
    "precision",
    "recall",
    "f1",
    "fpr",
    "fnr",
]


def flatten_metrics_payload(payload: dict, *, safe_float) -> dict:
    detection = payload.get("detection") or {}
    latency = payload.get("latency_ms") or {}
    source_latency = latency.get("source_to_ingest") or {}
    ingest_to_emit_latency = latency.get("ingest_to_emit") or {}
    source_to_emit_latency = latency.get("source_to_emit") or {}
    processing_latency = latency.get("processing") or {}
    end_to_end_latency = latency.get("end_to_end") or {}
    event_lateness_latency = latency.get("event_lateness") or {}
    event_time = payload.get("event_time") or {}
    kafka = payload.get("kafka") or {}
    system = payload.get("system") or {}
    system_knobs = payload.get("system_knobs") or {}
    watermark_delay_sec = system_knobs.get("watermark_delay_sec", "")
    late_event_ratio = event_time.get("late_event_ratio", "")
    interpretable_late_ratio = late_event_ratio
    freshness_signal_ratio = ""
    watermark_delay_num = safe_float(watermark_delay_sec)
    if watermark_delay_num == 0:
        interpretable_late_ratio = ""
        freshness_signal_ratio = late_event_ratio

    return {
        "ts_utc": payload.get("ts_utc", ""),
        "ts_epoch_ms": payload.get("ts_epoch_ms", ""),
        "batch_id": payload.get("batch_id", ""),
        "run_tag": payload.get("run_tag", ""),
        "load_profile": payload.get("load_profile", ""),
        "model_name": payload.get("model_name", ""),
        "feature_set": payload.get("feature_set", ""),
        "metric_warnings": "; ".join(str(item).strip() for item in (payload.get("metric_warnings") or []) if str(item).strip()),
        "rows": payload.get("rows", ""),
        "rows_per_sec": payload.get("rows_per_sec", ""),
        "batch_wall_ms": payload.get("batch_wall_ms", ""),
        "source_p95_ms": source_latency.get("p95", ""),
        "ingest_to_emit_p95_ms": ingest_to_emit_latency.get("p95", processing_latency.get("p95", "")),
        "source_to_emit_p95_ms": source_to_emit_latency.get("p95", end_to_end_latency.get("p95", "")),
        "proc_p95_ms": processing_latency.get("p95", ""),
        "e2e_p95_ms": end_to_end_latency.get("p95", ""),
        "event_lateness_p95_ms": event_lateness_latency.get("p95", ""),
        "late_event_ratio": late_event_ratio,
        "late_event_ratio_interpretable": interpretable_late_ratio,
        "freshness_signal_ratio": freshness_signal_ratio,
        "watermark_delay_sec": watermark_delay_sec,
        "kafka_lag_records_total": kafka.get("lag_records_total", ""),
        "kafka_lag_records_max_partition": kafka.get("lag_records_max_partition", ""),
        "driver_cpu_percent": system.get("driver_cpu_percent", ""),
        "driver_rss_mb": system.get("driver_rss_mb", ""),
        "executor_mem_util_avg": system.get("executor_mem_util_avg", ""),
        "executor_mem_util_p95": system.get("executor_mem_util_p95", ""),
        "executor_count": system.get("executor_count", ""),
        "labeled_rows": detection.get("labeled_rows", ""),
        "scored_rows": detection.get("scored_rows", ""),
        "tp": detection.get("tp", ""),
        "tn": detection.get("tn", ""),
        "fp": detection.get("fp", ""),
        "fn": detection.get("fn", ""),
        "precision": detection.get("precision", ""),
        "recall": detection.get("recall", ""),
        "f1": detection.get("f1", ""),
        "fpr": detection.get("fpr", ""),
        "fnr": detection.get("fnr", ""),
    }


def sanitize_run_tag(run_tag: str) -> str:
    text = str(run_tag).strip()
    if not text:
        return "unknown_run"
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", text)


def runtime_log_output_path(
    *,
    run_tag: str,
    output_dir: str,
    resolve_project_path,
    project_root,
) -> Path:
    resolved_dir = resolve_project_path(output_dir, project_root)
    return resolved_dir / f"{sanitize_run_tag(run_tag)}.log"


def timeseries_output_path(
    *,
    run_tag: str,
    output_dir: str,
    resolve_project_path,
    project_root,
) -> Path:
    resolved_dir = resolve_project_path(output_dir, project_root)
    return resolved_dir / f"{sanitize_run_tag(run_tag)}.csv"


def write_metrics_timeseries(
    metrics_rows: list[dict],
    *,
    run_tag: str,
    output_dir: str,
    is_terminal_metric_payload,
    payload_batch_id,
    flatten_metrics_payload,
    timeseries_output_path,
) -> Path | None:
    materialized_rows = [payload for payload in metrics_rows if not is_terminal_metric_payload(payload)]
    if not materialized_rows:
        return None

    path = timeseries_output_path(run_tag=run_tag, output_dir=output_dir)
    path.parent.mkdir(parents=True, exist_ok=True)

    ordered_rows = sorted(materialized_rows, key=payload_batch_id)
    flattened_rows = [flatten_metrics_payload(payload) for payload in ordered_rows]

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=TIMESERIES_FIELDNAMES)
        writer.writeheader()
        writer.writerows(flattened_rows)

    return path


def summarize_runtime_metrics(
    metrics_rows: list[dict],
    *,
    is_terminal_metric_payload,
    safe_float,
    nested_float,
    compute_f1_score,
) -> dict:
    materialized_rows = [payload for payload in metrics_rows if not is_terminal_metric_payload(payload)]
    if not materialized_rows:
        return {}

    rows_total = 0.0
    labeled_rows_total = 0.0
    scored_rows_total = 0.0
    tp_total = 0.0
    tn_total = 0.0
    fp_total = 0.0
    fn_total = 0.0
    score_weighted_total = 0.0
    attack_ratio_weighted_total = 0.0
    late_ratio_weighted_total = 0.0
    late_ratio_interpretable_weighted_total = 0.0
    freshness_signal_weighted_total = 0.0
    late_ratio_interpretable_rows_total = 0.0
    freshness_signal_rows_total = 0.0

    rows_per_sec_values: list[float] = []
    batch_wall_values: list[float] = []
    source_p95_values: list[float] = []
    ingest_to_emit_p95_values: list[float] = []
    source_to_emit_p95_values: list[float] = []
    proc_p95_values: list[float] = []
    e2e_p95_values: list[float] = []
    event_lateness_p95_values: list[float] = []
    lag_totals: list[float] = []
    driver_cpu_values: list[float] = []
    driver_rss_values: list[float] = []
    executor_mem_values: list[float] = []
    executor_mem_p95_values: list[float] = []
    executor_count_values: list[float] = []
    metric_warnings: set[str] = set()

    last_payload = materialized_rows[-1]

    for payload in materialized_rows:
        for warning in payload.get("metric_warnings") or []:
            warning_text = str(warning).strip()
            if warning_text:
                metric_warnings.add(warning_text)
        row_count = safe_float(payload.get("rows")) or 0.0
        rows_total += row_count

        rows_per_sec = safe_float(payload.get("rows_per_sec"))
        if rows_per_sec is not None:
            rows_per_sec_values.append(rows_per_sec)

        batch_wall_ms = safe_float(payload.get("batch_wall_ms"))
        if batch_wall_ms is not None:
            batch_wall_values.append(batch_wall_ms)

        source_p95 = nested_float(payload, "latency_ms", "source_to_ingest", "p95")
        if source_p95 is not None:
            source_p95_values.append(source_p95)

        ingest_to_emit_p95 = nested_float(payload, "latency_ms", "ingest_to_emit", "p95")
        if ingest_to_emit_p95 is not None:
            ingest_to_emit_p95_values.append(ingest_to_emit_p95)

        source_to_emit_p95 = nested_float(payload, "latency_ms", "source_to_emit", "p95")
        if source_to_emit_p95 is not None:
            source_to_emit_p95_values.append(source_to_emit_p95)

        proc_p95 = nested_float(payload, "latency_ms", "processing", "p95")
        if proc_p95 is not None:
            proc_p95_values.append(proc_p95)

        e2e_p95 = nested_float(payload, "latency_ms", "end_to_end", "p95")
        if e2e_p95 is not None:
            e2e_p95_values.append(e2e_p95)

        event_lateness_p95 = nested_float(payload, "latency_ms", "event_lateness", "p95")
        if event_lateness_p95 is not None:
            event_lateness_p95_values.append(event_lateness_p95)

        avg_score = safe_float(payload.get("avg_prediction_score"))
        if avg_score is not None and row_count > 0:
            score_weighted_total += avg_score * row_count

        attack_ratio = safe_float(payload.get("attack_ratio"))
        if attack_ratio is not None and row_count > 0:
            attack_ratio_weighted_total += attack_ratio * row_count

        late_ratio = nested_float(payload, "event_time", "late_event_ratio")
        watermark_delay_sec = nested_float(payload, "system_knobs", "watermark_delay_sec")
        if late_ratio is not None and row_count > 0:
            late_ratio_weighted_total += late_ratio * row_count
            if watermark_delay_sec == 0:
                freshness_signal_weighted_total += late_ratio * row_count
                freshness_signal_rows_total += row_count
            else:
                late_ratio_interpretable_weighted_total += late_ratio * row_count
                late_ratio_interpretable_rows_total += row_count

        detection = payload.get("detection") or {}
        labeled_rows_total += safe_float(detection.get("labeled_rows")) or 0.0
        scored_rows_total += safe_float(detection.get("scored_rows")) or 0.0
        tp_total += safe_float(detection.get("tp")) or 0.0
        tn_total += safe_float(detection.get("tn")) or 0.0
        fp_total += safe_float(detection.get("fp")) or 0.0
        fn_total += safe_float(detection.get("fn")) or 0.0

        lag_total = nested_float(payload, "kafka", "lag_records_total")
        if lag_total is not None:
            lag_totals.append(lag_total)

        driver_cpu = nested_float(payload, "system", "driver_cpu_percent")
        if driver_cpu is not None:
            driver_cpu_values.append(driver_cpu)

        driver_rss = nested_float(payload, "system", "driver_rss_mb")
        if driver_rss is not None:
            driver_rss_values.append(driver_rss)

        executor_mem = nested_float(payload, "system", "executor_mem_util_avg")
        if executor_mem is not None:
            executor_mem_values.append(executor_mem)

        executor_mem_p95 = nested_float(payload, "system", "executor_mem_util_p95")
        if executor_mem_p95 is not None:
            executor_mem_p95_values.append(executor_mem_p95)

        executor_count = nested_float(payload, "system", "executor_count")
        if executor_count is not None:
            executor_count_values.append(executor_count)

    precision = (tp_total / (tp_total + fp_total)) if scored_rows_total > 0 and (tp_total + fp_total) > 0 else None
    recall = (tp_total / (tp_total + fn_total)) if scored_rows_total > 0 and (tp_total + fn_total) > 0 else None
    f1 = compute_f1_score(precision, recall) if scored_rows_total > 0 else None
    fpr = (fp_total / (fp_total + tn_total)) if scored_rows_total > 0 and (fp_total + tn_total) > 0 else None
    fnr = (fn_total / (fn_total + tp_total)) if scored_rows_total > 0 and (fn_total + tp_total) > 0 else None

    return {
        "rows_total": int(rows_total),
        "rows_per_sec_avg": statistics.fmean(rows_per_sec_values) if rows_per_sec_values else None,
        "batch_wall_ms_avg": statistics.fmean(batch_wall_values) if batch_wall_values else None,
        "source_p95_ms_max": max(source_p95_values) if source_p95_values else None,
        "ingest_to_emit_p95_ms_max": max(ingest_to_emit_p95_values) if ingest_to_emit_p95_values else None,
        "source_to_emit_p95_ms_max": max(source_to_emit_p95_values) if source_to_emit_p95_values else None,
        "proc_p95_ms_max": max(proc_p95_values) if proc_p95_values else None,
        "e2e_p95_ms_max": max(e2e_p95_values) if e2e_p95_values else None,
        "event_lateness_p95_ms_max": max(event_lateness_p95_values) if event_lateness_p95_values else None,
        "avg_prediction_score_weighted": (score_weighted_total / rows_total) if rows_total > 0 else None,
        "attack_ratio_weighted": (attack_ratio_weighted_total / rows_total) if rows_total > 0 else None,
        "late_event_ratio_weighted": (late_ratio_weighted_total / rows_total) if rows_total > 0 else None,
        "late_event_ratio_interpretable_weighted": (
            late_ratio_interpretable_weighted_total / late_ratio_interpretable_rows_total
        ) if late_ratio_interpretable_rows_total > 0 else None,
        "freshness_signal_ratio_weighted": (
            freshness_signal_weighted_total / freshness_signal_rows_total
        ) if freshness_signal_rows_total > 0 else None,
        "labeled_rows_total": int(labeled_rows_total),
        "scored_rows_total": int(scored_rows_total),
        "tp_total": int(tp_total),
        "tn_total": int(tn_total),
        "fp_total": int(fp_total),
        "fn_total": int(fn_total),
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "fpr": fpr,
        "fnr": fnr,
        "kafka_lag_records_max": max(lag_totals) if lag_totals else None,
        "driver_cpu_percent_avg": statistics.fmean(driver_cpu_values) if driver_cpu_values else None,
        "driver_rss_mb_avg": statistics.fmean(driver_rss_values) if driver_rss_values else None,
        "executor_mem_util_avg": statistics.fmean(executor_mem_values) if executor_mem_values else None,
        "executor_mem_util_p95_avg": statistics.fmean(executor_mem_p95_values) if executor_mem_p95_values else None,
        "executor_count_max": max(executor_count_values) if executor_count_values else None,
        "metric_warnings": sorted(metric_warnings),
        "watermark_delay_sec": nested_float(last_payload, "system_knobs", "watermark_delay_sec"),
        "last_payload": last_payload,
        "batch_count": len(materialized_rows),
    }


def annotate_sut_debug_summary(row: dict) -> dict:
    annotated = dict(row)
    annotated.setdefault("evaluation_source", "sut_debug_metrics")
    annotated.setdefault("boundary_mode", "sut_metrics_only")
    annotated.setdefault("official_source_of_truth", "matrix_summaries_derived_from_ids_metrics")
    annotated.setdefault("legacy_metrics_status", str(annotated.get("status", "")).strip().lower() or "missing")
    annotated.setdefault("metrics_comparison_status", "not_applicable")
    annotated.setdefault("comparison_notes", "official summary currently derived from SUT-emitted debug metrics")
    return annotated


def write_summary_rows(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return

    fieldnames: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row.keys():
            if key in seen:
                continue
            seen.add(key)
            fieldnames.append(key)

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
