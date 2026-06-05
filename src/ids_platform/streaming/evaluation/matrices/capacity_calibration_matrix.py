from __future__ import annotations

from datetime import datetime, timezone

from ids_platform.streaming.config.calibration import (
    CapacityCalibrationConfig,
    CapacityCalibrationRun,
    CapacitySloConfig,
)
from ids_platform.streaming.evaluation.matrices.common import (
    summarize_prediction_latency,
    write_summary_rows,
)
from ids_platform.streaming.evaluation.matrices.throughput.benchmark_run import (
    cleanup_benchmark_run_topics,
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


def evaluate_capacity_slo(
    *,
    summary: dict,
    target_rps: float,
    expected_rows: int,
    trigger_interval: str,
    slo: CapacitySloConfig,
) -> dict:
    artifact_rows_total = int(_safe_float(summary.get("artifact_rows_total")) or 0)
    rows_total = int(_safe_float(summary.get("rows_total")) or artifact_rows_total or 0)
    actual_rps = _safe_float(summary.get("drain_rps"))
    throughput_ratio = (actual_rps / float(target_rps)) if actual_rps is not None and target_rps > 0 else None
    p50_e2e_ms = _safe_float(summary.get("artifact_e2e_p50_ms"))
    p95_e2e_ms = _safe_float(summary.get("artifact_e2e_p95_ms"))
    p99_e2e_ms = _safe_float(summary.get("artifact_e2e_p99_ms"))
    trigger_ms = trigger_interval_ms(trigger_interval)
    has_artifact_latency = str(summary.get("latency_status") or "") == "ok" and artifact_rows_total > 0

    failures: list[str] = []
    if not has_artifact_latency:
        failures.append("artifact_latency_missing")
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
    if str(summary.get("replay_status") or "") == "unstable":
        failures.append("load_generator_unstable")

    return {
        "rows_total": rows_total,
        "actual_rps": actual_rps,
        "drain_rps": actual_rps,
        "throughput_ratio": throughput_ratio,
        "p50_e2e_ms": p50_e2e_ms,
        "p95_e2e_ms": p95_e2e_ms,
        "p99_e2e_ms": p99_e2e_ms,
        "trigger_interval_ms": trigger_ms,
        "kafka_lag_peak_records": _safe_float(summary.get("kafka_lag_peak_records")),
        "slo_status": "fail" if failures else "pass",
        "failure_reason": ";".join(failures),
    }


def _summary_row(calibration_run: CapacityCalibrationRun, summary: dict, slo_result: dict) -> dict:
    benchmark = calibration_run.benchmark
    profile = benchmark.profile
    row = {
        "run_tag": benchmark.run_tag,
        "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "repeat_index": benchmark.repeat_index,
        "profile": profile.name,
        "mode": calibration_run.mode,
        "model": benchmark.model_label,
        "feature_set": benchmark.feature_set,
        "target_rps": calibration_run.target_rps,
        "traffic_mode": benchmark.replay.rate.traffic_mode,
        "trigger_interval": profile.trigger_interval,
        "spark_master": profile.spark_master,
        "max_offsets_per_trigger": profile.max_offsets_per_trigger,
        "shuffle_partitions": profile.shuffle_partitions,
        "topic_partitions": benchmark.topic_partitions,
        "rows_expected": calibration_run.expected_rows,
        "status": "ok" if summary.get("latency_status") == "ok" else "artifact_missing",
        "latency_status": summary.get("latency_status", ""),
        "artifact_rows_total": summary.get("artifact_rows_total", ""),
        "artifact_source_p95_ms": summary.get("artifact_source_p95_ms", ""),
        "artifact_source_p99_ms": summary.get("artifact_source_p99_ms", ""),
        "artifact_processing_p95_ms": summary.get("artifact_processing_p95_ms", ""),
        "artifact_processing_p99_ms": summary.get("artifact_processing_p99_ms", ""),
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
        "warmup_artifact_barrier_status": summary.get("warmup_artifact_barrier_status", ""),
        "warmup_artifact_collect_status": summary.get("warmup_artifact_collect_status", ""),
        "warmup_artifact_collect_rows": summary.get("warmup_artifact_collect_rows", ""),
        "warmup_artifact_collect_labeled_rows": summary.get("warmup_artifact_collect_labeled_rows", ""),
        "warmup_artifact_rows_expected": summary.get("warmup_artifact_rows_expected", ""),
        "warmup_artifact_rows_actual": summary.get("warmup_artifact_rows_actual", ""),
        "warmup_kafka_drain_barrier_status": summary.get("warmup_kafka_drain_barrier_status", ""),
        "warmup_kafka_lag_records_end": summary.get("warmup_kafka_lag_records_end", ""),
        "measure_artifact_barrier_status": summary.get("measure_artifact_barrier_status", ""),
        "measure_artifact_collect_status": summary.get("measure_artifact_collect_status", ""),
        "measure_artifact_collect_rows": summary.get("measure_artifact_collect_rows", ""),
        "measure_artifact_collect_labeled_rows": summary.get("measure_artifact_collect_labeled_rows", ""),
        "measure_artifact_rows_expected": summary.get("measure_artifact_rows_expected", ""),
        "measure_artifact_rows_actual": summary.get("measure_artifact_rows_actual", ""),
        "measure_kafka_drain_barrier_status": summary.get("measure_kafka_drain_barrier_status", ""),
        "measure_kafka_lag_records_end": summary.get("measure_kafka_lag_records_end", ""),
        "measure_kafka_lag_end_after_drain_records": summary.get(
            "measure_kafka_lag_end_after_drain_records",
            "",
        ),
        "resource_status": summary.get("resource_status", ""),
        "resource_samples": summary.get("resource_samples", ""),
        "cpu_avg_pct": summary.get("cpu_avg_pct", ""),
        "cpu_max_pct": summary.get("cpu_max_pct", ""),
        "mem_avg_mb": summary.get("mem_avg_mb", ""),
        "mem_max_mb": summary.get("mem_max_mb", ""),
        "replay_status": summary.get("replay_status", ""),
        "replay_failure_reason": summary.get("replay_failure_reason", ""),
        "replay_target_rps": summary.get("replay_target_rps", ""),
        "replay_actual_rps": summary.get("replay_actual_rps", ""),
        "replay_actual_elapsed_sec": summary.get("replay_actual_elapsed_sec", ""),
        "replay_emit_gap_p95_ms": summary.get("replay_emit_gap_p95_ms", ""),
        "replay_emit_gap_p99_ms": summary.get("replay_emit_gap_p99_ms", ""),
        "replay_emit_gap_max_ms": summary.get("replay_emit_gap_max_ms", ""),
        "replay_tick_lag_p95_ms": summary.get("replay_tick_lag_p95_ms", ""),
        "replay_tick_lag_max_ms": summary.get("replay_tick_lag_max_ms", ""),
        "replay_producer_buffer_stall_count": summary.get("replay_producer_buffer_stall_count", ""),
        "replay_producer_buffer_stall_sec": summary.get("replay_producer_buffer_stall_sec", ""),
        "replay_producer_flush_sec": summary.get("replay_producer_flush_sec", ""),
    }
    row.update(slo_result)
    return row


def run_capacity_calibration_matrix(config: CapacityCalibrationConfig) -> int:
    rows: list[dict] = []
    try:
        for calibration_run in config.runs:
            result = run_benchmark_run(calibration_run.benchmark)
            summary: dict = {}
            summary.update(
                summarize_prediction_latency(
                    calibration_run.benchmark.artifact_output,
                    phase="measure",
                )
            )
            summary.update(result.kafka_lag_summary)
            summary.update(result.resource_summary)
            summary.update(result.replay_summary)
            summary.update(result.barrier_summary)
            slo_result = evaluate_capacity_slo(
                summary=summary,
                target_rps=calibration_run.target_rps,
                expected_rows=calibration_run.expected_rows,
                trigger_interval=calibration_run.benchmark.profile.trigger_interval,
                slo=config.slo,
            )
            rows.append(_summary_row(calibration_run, summary, slo_result))
            write_summary_rows(config.summary_csv, rows)
    finally:
        cleanup_benchmark_run_topics(
            (calibration_run.benchmark for calibration_run in config.runs),
            ignore_errors=True,
        )

    return 0


run = run_capacity_calibration_matrix
