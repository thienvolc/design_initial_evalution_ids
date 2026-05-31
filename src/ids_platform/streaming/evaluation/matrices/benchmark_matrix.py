from __future__ import annotations

from ids_platform.streaming.config.common import BenchmarkMatrixConfig, BenchmarkRunPlan
from ids_platform.streaming.evaluation.matrices.common import (
    annotate_sut_debug_summary,
    summarize_prediction_quality,
    write_metrics_timeseries,
    write_summary_rows,
)
from ids_platform.streaming.evaluation.matrices.throughput.benchmark_run import (
    BenchmarkRunConfig,
    run_benchmark_run,
)
from ids_platform.streaming.evaluation.matrices.throughput.result_helpers import (
    materialize_sut_summary_row,
    persist_summary_rows,
)


def run_benchmark_matrix(
    config: BenchmarkMatrixConfig,
    *,
    label: str,
    build_summary_row,
) -> int:
    summary_rows: list[dict] = []

    for run_plan in config.runs:
        if run_plan.warmup is not None:
            run_benchmark_run(run_plan.warmup)

        benchmark = run_plan.benchmark
        result = run_benchmark_run(benchmark)
        summary, _ = materialize_sut_summary_row(
            run_tag=benchmark.run_tag,
            metrics_rows=result.metrics_rows,
            build_legacy_row_fn=lambda materialized_metrics_rows: build_summary_row(
                run_plan,
                benchmark,
                materialized_metrics_rows,
                summarize_prediction_quality(benchmark.runtime.output.artifact_output),
            ),
            write_metrics_timeseries_fn=write_metrics_timeseries,
            annotate_sut_debug_summary_fn=annotate_sut_debug_summary,
        )
        summary_rows.append(summary)

    persist_summary_rows(
        summary_csv=str(config.summary_csv),
        rows=summary_rows,
        write_summary_rows_fn=write_summary_rows,
        label=label,
    )
    return 0


__all__ = [
    "BenchmarkMatrixConfig",
    "BenchmarkRunConfig",
    "BenchmarkRunPlan",
    "run_benchmark_matrix",
]
