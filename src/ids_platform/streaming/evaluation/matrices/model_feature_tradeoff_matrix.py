from __future__ import annotations

from ids_platform.streaming.config.common import BenchmarkMatrixConfig
from ids_platform.streaming.evaluation.matrices.benchmark_matrix import run_benchmark_matrix
from ids_platform.streaming.evaluation.matrices.common import summarize_prediction_latency
from ids_platform.streaming.evaluation.matrices.throughput.row_builders import (
    build_model_feature_tradeoff_summary_row,
)


def _summary_row(run_plan, benchmark, quality_summary: dict, run_result) -> dict:
    row = build_model_feature_tradeoff_summary_row(
        run_tag=benchmark.run_tag,
        repeat_index=benchmark.repeat_index,
        model=benchmark.model_label,
        feature_set=benchmark.feature_set,
        quality_summary=quality_summary,
        context=run_plan.summary_context,
    )
    artifact_summary = summarize_prediction_latency(
        benchmark.artifact_output,
        phase="measure",
    )
    row.update(
        {
            "rows": artifact_summary.get("rows_total", row.get("rows", "")),
            "artifact_rows_total": artifact_summary.get("artifact_rows_total", ""),
            "latency_status": artifact_summary.get("latency_status", ""),
            "source_p95_ms": artifact_summary.get("artifact_source_p95_ms", row.get("source_p95_ms", "")),
            "source_p99_ms": artifact_summary.get("artifact_source_p99_ms", ""),
            "proc_p95_ms": artifact_summary.get("artifact_processing_p95_ms", row.get("proc_p95_ms", "")),
            "proc_p99_ms": artifact_summary.get("artifact_processing_p99_ms", ""),
            "e2e_p95_ms": artifact_summary.get("artifact_e2e_p95_ms", row.get("e2e_p95_ms", "")),
            "e2e_p99_ms": artifact_summary.get("artifact_e2e_p99_ms", ""),
            "drain_rps": artifact_summary.get("drain_rps", ""),
        }
    )
    row.update(run_result.kafka_lag_summary)
    row.update(run_result.resource_summary)
    if artifact_summary.get("latency_status") == "ok" or quality_summary.get("quality_status") == "ok":
        row["status"] = "ok"
    return row


def run_model_feature_tradeoff_matrix(config: BenchmarkMatrixConfig) -> int:
    return run_benchmark_matrix(config, build_summary_row=_summary_row)


run = run_model_feature_tradeoff_matrix
