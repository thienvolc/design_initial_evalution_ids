from __future__ import annotations

import math
from datetime import datetime, timezone

from ids_platform.streaming.config.calibration import (
    CapacityCalibrationConfig,
    CapacityCalibrationRun,
    CapacitySloConfig,
)
from ids_platform.streaming.evaluation.matrices.common import (
    filter_metrics_by_phase,
    summarize_runtime_metrics,
    write_metrics_timeseries,
    write_summary_rows,
)
from ids_platform.streaming.evaluation.matrices.throughput.benchmark_run import (
    run_benchmark_run,
)


def _safe_float(value) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def trigger_interval_ms(trigger_interval: str) -> float | None:
    parts = str(trigger_interval or "").strip().lower().split()
    if not parts:
        return None
    value = _safe_float(parts[0])
    if value is None:
        return None
    unit = parts[1] if len(parts) > 1 else "seconds"
    if unit.startswith("millisecond") or unit == "ms":
        return value
    if unit.startswith("second") or unit == "s":
        return value * 1_000.0
    if unit.startswith("minute") or unit == "m":
        return value * 60_000.0
    return None


def _latency_percentile(payload: dict, bucket_name: str, percentile_name: str) -> float | None:
    latency = payload.get("latency_ms") or {}
    bucket = latency.get(bucket_name) or {}
    if not isinstance(bucket, dict):
        return None
    return _safe_float(bucket.get(percentile_name))


def _max_latency_percentile(metrics_rows: list[dict], bucket_name: str, percentile_name: str) -> float | None:
    values = [
        value
        for value in (
            _latency_percentile(payload, bucket_name, percentile_name)
            for payload in metrics_rows
        )
        if value is not None
    ]
    return max(values) if values else None


def _percentile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(min(math.ceil(float(quantile) * len(ordered)) - 1, len(ordered) - 1), 0)
    return ordered[index]


def _batch_wall_p95(metrics_rows: list[dict]) -> float | None:
    values = [
        value
        for value in (_safe_float(payload.get("batch_wall_ms")) for payload in metrics_rows)
        if value is not None
    ]
    return _percentile(values, 0.95)


def _lag_is_increasing(metrics_rows: list[dict]) -> bool:
    values: list[float] = []
    for payload in metrics_rows:
        kafka = payload.get("kafka") or {}
        value = _safe_float(kafka.get("lag_records_total"))
        if value is not None:
            values.append(value)
    if len(values) < 3:
        return False
    return values[-1] > values[0] and all(right >= left for left, right in zip(values, values[1:]))


def evaluate_capacity_slo(
    *,
    summary: dict,
    metrics_rows: list[dict],
    target_rps: float,
    expected_rows: int,
    trigger_interval: str,
    slo: CapacitySloConfig,
) -> dict:
    rows_total = int(_safe_float(summary.get("rows_total")) or 0)
    actual_rps = _safe_float(summary.get("rows_per_sec_avg"))
    throughput_ratio = (actual_rps / float(target_rps)) if actual_rps is not None and target_rps > 0 else None
    p50_e2e_ms = _max_latency_percentile(metrics_rows, "end_to_end", "p50")
    p95_e2e_ms = _safe_float(summary.get("e2e_p95_ms_max"))
    batch_wall_p95_ms = _batch_wall_p95(metrics_rows)
    trigger_ms = trigger_interval_ms(trigger_interval)
    kafka_lag_max = _safe_float(summary.get("kafka_lag_records_max"))

    failures: list[str] = []
    if not metrics_rows:
        failures.append("metrics_missing")
    if expected_rows > 0 and rows_total < expected_rows * float(slo.min_rows_ratio):
        failures.append("rows_below_expected")
    if throughput_ratio is None or throughput_ratio < float(slo.min_throughput_ratio):
        failures.append("throughput_below_target")
    if p50_e2e_ms is None or p95_e2e_ms is None:
        failures.append("latency_missing")
    else:
        if p50_e2e_ms > float(slo.max_p50_e2e_ms):
            failures.append("p50_e2e_exceeded")
        if p95_e2e_ms > float(slo.max_p95_e2e_ms):
            failures.append("p95_e2e_exceeded")
    if batch_wall_p95_ms is None or trigger_ms is None:
        failures.append("batch_wall_or_trigger_missing")
    elif batch_wall_p95_ms >= trigger_ms * float(slo.max_batch_wall_trigger_ratio):
        failures.append("batch_wall_exceeds_trigger_budget")
    if _lag_is_increasing(metrics_rows):
        failures.append("lag_increasing")

    return {
        "rows_total": rows_total,
        "actual_rps": actual_rps,
        "throughput_ratio": throughput_ratio,
        "p50_e2e_ms": p50_e2e_ms,
        "p95_e2e_ms": p95_e2e_ms,
        "batch_wall_p95_ms": batch_wall_p95_ms,
        "trigger_interval_ms": trigger_ms,
        "kafka_lag_max": kafka_lag_max,
        "slo_status": "fail" if failures else "pass",
        "failure_reason": ";".join(failures),
    }


def _summary_row(calibration_run: CapacityCalibrationRun, metrics_rows: list[dict], summary: dict, slo_result: dict) -> dict:
    benchmark = calibration_run.benchmark
    profile = benchmark.profile
    row = {
        "run_tag": benchmark.run_tag,
        "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "profile": profile.name,
        "mode": calibration_run.mode,
        "model": benchmark.model_label,
        "feature_set": benchmark.feature_set,
        "target_rps": calibration_run.target_rps,
        "trigger_interval": profile.trigger_interval,
        "spark_master": profile.spark_master,
        "max_offsets_per_trigger": profile.max_offsets_per_trigger,
        "shuffle_partitions": profile.shuffle_partitions,
        "rows_expected": calibration_run.expected_rows,
        "batch_count": summary.get("batch_count", 0),
        "status": "ok" if metrics_rows else "metrics_missing",
    }
    row.update(slo_result)
    return row


def run_capacity_calibration_matrix(config: CapacityCalibrationConfig) -> int:
    rows: list[dict] = []
    for calibration_run in config.runs:
        result = run_benchmark_run(calibration_run.benchmark)
        write_metrics_timeseries(result.metrics_rows, run_tag=calibration_run.benchmark.run_tag)
        metrics_rows = filter_metrics_by_phase(result.metrics_rows, "measure")
        summary = summarize_runtime_metrics(metrics_rows)
        slo_result = evaluate_capacity_slo(
            summary=summary,
            metrics_rows=metrics_rows,
            target_rps=calibration_run.target_rps,
            expected_rows=calibration_run.expected_rows,
            trigger_interval=calibration_run.benchmark.profile.trigger_interval,
            slo=config.slo,
        )
        rows.append(_summary_row(calibration_run, metrics_rows, summary, slo_result))

    write_summary_rows(config.summary_csv, rows)
    return 0


run = run_capacity_calibration_matrix
