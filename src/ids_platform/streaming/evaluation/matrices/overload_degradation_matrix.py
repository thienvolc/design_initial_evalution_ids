from __future__ import annotations

from datetime import datetime, timezone

from ids_platform.streaming.config.common import BenchmarkMatrixConfig
from ids_platform.streaming.evaluation.matrices.benchmark_matrix import run_benchmark_matrix
from ids_platform.streaming.evaluation.matrices.common import (
    apply_quality_summary_fields,
    summarize_prediction_latency,
)


def _safe_float(value) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def classify_overload_degradation(
    *,
    drain_throughput_ratio: float | None,
    p95_e2e_ms: float | None,
    kafka_lag_growth: float | None,
    kafka_lag_peak_ratio: float | None = None,
    max_p95_e2e_ms: float = 2_000.0,
    min_throughput_ratio: float = 0.90,
) -> str:
    if drain_throughput_ratio is None or p95_e2e_ms is None:
        return "artifact_missing"
    if drain_throughput_ratio < min_throughput_ratio or p95_e2e_ms > max_p95_e2e_ms:
        return "overloaded"
    if kafka_lag_peak_ratio is not None and kafka_lag_peak_ratio >= 1.0:
        return "backpressure"
    if kafka_lag_growth is not None and kafka_lag_growth > 0:
        return "backpressure"
    return "stable"


def _summary_row(run_plan, benchmark, quality_summary: dict, run_result) -> dict:
    context = run_plan.summary_context or {}
    summary: dict = {}
    summary.update(
        summarize_prediction_latency(
            benchmark.artifact_output,
            phase="measure",
        )
    )
    summary.update(run_result.kafka_lag_summary)
    summary.update(run_result.resource_summary)
    summary.update(run_result.replay_summary)
    expected_rows = int(context.get("expected_rows") or benchmark.replay.source.expected_rows)
    target_rps = _safe_float(context.get("target_rps"))
    drain_rps = _safe_float(summary.get("drain_rps"))
    drain_throughput_ratio = (drain_rps / target_rps) if drain_rps is not None and target_rps else None
    target_to_drain_ratio = (target_rps / drain_rps) if drain_rps and target_rps else None
    p50_e2e_ms = _safe_float(summary.get("artifact_e2e_p50_ms"))
    p95_e2e_ms = _safe_float(summary.get("artifact_e2e_p95_ms"))
    p99_e2e_ms = _safe_float(summary.get("artifact_e2e_p99_ms"))
    processing_p95_ms = _safe_float(summary.get("artifact_processing_p95_ms"))
    source_to_kafka_p95_ms = _safe_float(summary.get("artifact_source_p95_ms"))
    kafka_lag_peak = _safe_float(summary.get("kafka_lag_peak_records"))
    kafka_lag_peak_ratio = (kafka_lag_peak / target_rps) if kafka_lag_peak is not None and target_rps else None

    row = {
        "run_tag": benchmark.run_tag,
        "repeat_index": benchmark.repeat_index,
        "overload_profile": context.get("overload_profile", benchmark.runtime.run.load_profile),
        "model": benchmark.model_label,
        "feature_set": benchmark.feature_set,
        "traffic_mode": context.get("traffic_mode", ""),
        "target_rps": target_rps if target_rps is not None else "",
        "peak_rps": context.get("peak_rps", ""),
        "duration_sec": context.get("duration_sec", ""),
        "expected_rows": expected_rows,
        "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "rows": summary.get("rows_total", ""),
        "latency_status": summary.get("latency_status", ""),
        "artifact_rows_total": summary.get("artifact_rows_total", ""),
        "measure_wall_clock_sec": summary.get("measure_wall_clock_sec", ""),
        "drain_rps": drain_rps if drain_rps is not None else "",
        "drain_throughput_ratio": drain_throughput_ratio if drain_throughput_ratio is not None else "",
        "target_to_drain_ratio": target_to_drain_ratio if target_to_drain_ratio is not None else "",
        "p50_e2e_ms": p50_e2e_ms if p50_e2e_ms is not None else "",
        "p95_e2e_ms": p95_e2e_ms if p95_e2e_ms is not None else "",
        "p99_e2e_ms": p99_e2e_ms if p99_e2e_ms is not None else "",
        "processing_p95_ms": processing_p95_ms if processing_p95_ms is not None else "",
        "processing_p99_ms": summary.get("artifact_processing_p99_ms", ""),
        "source_to_kafka_p95_ms": source_to_kafka_p95_ms if source_to_kafka_p95_ms is not None else "",
        "source_to_kafka_p99_ms": summary.get("artifact_source_p99_ms", ""),
        "kafka_lag_status": summary.get("kafka_lag_status", ""),
        "kafka_lag_records_end": summary.get("kafka_lag_records_end", ""),
        "kafka_lag_peak_records": summary.get("kafka_lag_peak_records", ""),
        "kafka_lag_at_replay_end_records": summary.get("kafka_lag_at_replay_end_records", ""),
        "kafka_lag_clear_sec": summary.get("kafka_lag_clear_sec", ""),
        "kafka_lag_area_records_sec": summary.get("kafka_lag_area_records_sec", ""),
        "kafka_lag_end_after_drain_records": summary.get("kafka_lag_end_after_drain_records", ""),
        "kafka_lag_records_raw_end": summary.get("kafka_lag_records_raw_end", ""),
        "kafka_control_tail_records": summary.get("kafka_control_tail_records", ""),
        "kafka_lag_partitions": summary.get("kafka_lag_partitions", ""),
        "kafka_lag_samples": summary.get("kafka_lag_samples", ""),
        "kafka_lag_timeseries_path": summary.get("kafka_lag_timeseries_path", ""),
        "kafka_lag_peak_ratio": kafka_lag_peak_ratio if kafka_lag_peak_ratio is not None else "",
        "kafka_lag_growth": "",
        "replay_status": summary.get("replay_status", ""),
        "replay_failure_reason": summary.get("replay_failure_reason", ""),
        "replay_target_rps": summary.get("replay_target_rps", ""),
        "replay_actual_rps": summary.get("replay_actual_rps", ""),
        "replay_actual_elapsed_sec": summary.get("replay_actual_elapsed_sec", ""),
        "replay_emit_gap_p95_ms": summary.get("replay_emit_gap_p95_ms", ""),
        "replay_tick_lag_p95_ms": summary.get("replay_tick_lag_p95_ms", ""),
        "quality_status": "",
        "precision": "",
        "recall": "",
        "f1": "",
        "fpr": "",
        "fnr": "",
        "resource_status": summary.get("resource_status", ""),
        "resource_samples": summary.get("resource_samples", ""),
        "cpu_avg_pct": summary.get("cpu_avg_pct", ""),
        "cpu_max_pct": summary.get("cpu_max_pct", ""),
        "mem_avg_mb": summary.get("mem_avg_mb", ""),
        "mem_max_mb": summary.get("mem_max_mb", ""),
        "status": "ok" if summary.get("latency_status") == "ok" else "artifact_missing",
        "degradation_status": classify_overload_degradation(
            drain_throughput_ratio=drain_throughput_ratio,
            p95_e2e_ms=p95_e2e_ms,
            kafka_lag_growth=None,
            kafka_lag_peak_ratio=kafka_lag_peak_ratio,
        ),
    }
    apply_quality_summary_fields(row, quality_summary)
    return row


def run_overload_degradation_matrix(config: BenchmarkMatrixConfig) -> int:
    return run_benchmark_matrix(config, build_summary_row=_summary_row)


run = run_overload_degradation_matrix
