from __future__ import annotations

from ids_platform.streaming.config.capacity import CapacityMatrixConfig
from ids_platform.streaming.evaluation.matrices.benchmark_matrix import run_benchmark_matrix
from ids_platform.streaming.evaluation.matrices.common import summarize_runtime_metrics
from ids_platform.streaming.evaluation.matrices.throughput.row_builders import (
    build_capacity_summary_row,
)


def _summary_row(run_plan, benchmark, metrics_rows: list[dict], quality_summary: dict) -> dict:
    return build_capacity_summary_row(
        run_tag=benchmark.run_tag,
        repeat_index=benchmark.repeat_index,
        model=benchmark.model_label,
        feature_set=benchmark.feature_set,
        profile=benchmark.profile.to_legacy_dict(),
        metrics_rows=metrics_rows,
        summarize_runtime_metrics_fn=summarize_runtime_metrics,
        quality_summary=quality_summary,
    )


def run_capacity_matrix(config: CapacityMatrixConfig) -> int:
    return run_benchmark_matrix(config, label="Capacity", build_summary_row=_summary_row)
