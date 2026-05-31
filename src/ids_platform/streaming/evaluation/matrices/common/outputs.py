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
]


def _payload_section(payload: dict, key: str) -> dict:
    section = payload.get(key) or {}
    return section if isinstance(section, dict) else {}


def _latency_bucket(payload: dict, bucket_name: str) -> dict:
    bucket = _payload_section(payload, "latency_ms").get(bucket_name) or {}
    return bucket if isinstance(bucket, dict) else {}


def _latency_p95(payload: dict, primary_bucket: str, *, fallback_bucket: str = "", default=None):
    value = _latency_bucket(payload, primary_bucket).get("p95")
    if value is not None:
        return value
    if fallback_bucket:
        return _latency_bucket(payload, fallback_bucket).get("p95", default)
    return default


def _append_number(values: list[float], value, *, safe_float) -> None:
    number = safe_float(value)
    if number is not None:
        values.append(number)


def _mean_or_none(values: list[float]) -> float | None:
    return statistics.fmean(values) if values else None


def _max_or_none(values: list[float]) -> float | None:
    return max(values) if values else None


def flatten_metrics_payload(payload: dict, *, safe_float) -> dict:
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
        "rows": payload.get("rows", ""),
        "rows_per_sec": payload.get("rows_per_sec", ""),
        "batch_wall_ms": payload.get("batch_wall_ms", ""),
        "source_p95_ms": _latency_p95(payload, "source_to_ingest", default=""),
        "ingest_to_emit_p95_ms": _latency_p95(
            payload,
            "ingest_to_emit",
            fallback_bucket="processing",
            default="",
        ),
        "source_to_emit_p95_ms": _latency_p95(
            payload,
            "source_to_emit",
            fallback_bucket="end_to_end",
            default="",
        ),
        "proc_p95_ms": _latency_p95(payload, "processing", default=""),
        "e2e_p95_ms": _latency_p95(payload, "end_to_end", default=""),
        "event_lateness_p95_ms": _latency_p95(payload, "event_lateness", default=""),
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
) -> dict:
    materialized_rows = [payload for payload in metrics_rows if not is_terminal_metric_payload(payload)]
    if not materialized_rows:
        return {}

    rows_total = 0.0
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

    last_payload = materialized_rows[-1]

    for payload in materialized_rows:
        row_count = safe_float(payload.get("rows")) or 0.0
        rows_total += row_count

        _append_number(rows_per_sec_values, payload.get("rows_per_sec"), safe_float=safe_float)
        _append_number(batch_wall_values, payload.get("batch_wall_ms"), safe_float=safe_float)
        _append_number(
            source_p95_values,
            _latency_p95(payload, "source_to_ingest"),
            safe_float=safe_float,
        )
        _append_number(
            ingest_to_emit_p95_values,
            _latency_p95(payload, "ingest_to_emit", fallback_bucket="processing"),
            safe_float=safe_float,
        )
        _append_number(
            source_to_emit_p95_values,
            _latency_p95(payload, "source_to_emit", fallback_bucket="end_to_end"),
            safe_float=safe_float,
        )
        _append_number(
            proc_p95_values,
            _latency_p95(payload, "processing"),
            safe_float=safe_float,
        )
        _append_number(
            e2e_p95_values,
            _latency_p95(payload, "end_to_end"),
            safe_float=safe_float,
        )
        _append_number(
            event_lateness_p95_values,
            _latency_p95(payload, "event_lateness"),
            safe_float=safe_float,
        )

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

    return {
        "rows_total": int(rows_total),
        "rows_per_sec_avg": _mean_or_none(rows_per_sec_values),
        "batch_wall_ms_avg": _mean_or_none(batch_wall_values),
        "source_p95_ms_max": _max_or_none(source_p95_values),
        "ingest_to_emit_p95_ms_max": _max_or_none(ingest_to_emit_p95_values),
        "source_to_emit_p95_ms_max": _max_or_none(source_to_emit_p95_values),
        "proc_p95_ms_max": _max_or_none(proc_p95_values),
        "e2e_p95_ms_max": _max_or_none(e2e_p95_values),
        "event_lateness_p95_ms_max": _max_or_none(event_lateness_p95_values),
        "late_event_ratio_weighted": (late_ratio_weighted_total / rows_total) if rows_total > 0 else None,
        "late_event_ratio_interpretable_weighted": (
            late_ratio_interpretable_weighted_total / late_ratio_interpretable_rows_total
        ) if late_ratio_interpretable_rows_total > 0 else None,
        "freshness_signal_ratio_weighted": (
            freshness_signal_weighted_total / freshness_signal_rows_total
        ) if freshness_signal_rows_total > 0 else None,
        "kafka_lag_records_max": _max_or_none(lag_totals),
        "driver_cpu_percent_avg": _mean_or_none(driver_cpu_values),
        "driver_rss_mb_avg": _mean_or_none(driver_rss_values),
        "executor_mem_util_avg": _mean_or_none(executor_mem_values),
        "executor_mem_util_p95_avg": _mean_or_none(executor_mem_p95_values),
        "executor_count_max": _max_or_none(executor_count_values),
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
