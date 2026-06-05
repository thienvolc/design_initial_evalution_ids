from __future__ import annotations

from ids_platform.streaming.config.common import BenchmarkMatrixConfig, BenchmarkRunPlan
from ids_platform.streaming.evaluation.matrices.common import (
    summarize_prediction_quality,
    write_summary_rows,
)
from ids_platform.streaming.evaluation.matrices.throughput.benchmark_run import (
    BenchmarkRunConfig,
    cleanup_benchmark_run_topics,
    run_benchmark_run,
)


def run_benchmark_matrix(
    config: BenchmarkMatrixConfig,
    *,
    build_summary_row,
) -> int:
    summary_rows: list[dict] = []

    try:
        for run_plan in config.runs:
            benchmark = run_plan.benchmark
            result = run_benchmark_run(benchmark)
            summary = build_summary_row(
                run_plan,
                benchmark,
                summarize_prediction_quality(benchmark.artifact_output, phase="measure"),
                result,
            )
            summary_rows.append(summary)

        write_summary_rows(config.summary_csv, summary_rows)
    finally:
        cleanup_benchmark_run_topics(
            (run_plan.benchmark for run_plan in config.runs),
            ignore_errors=True,
        )
    return 0


__all__ = [
    "BenchmarkMatrixConfig",
    "BenchmarkRunConfig",
    "BenchmarkRunPlan",
    "run_benchmark_matrix",
]
