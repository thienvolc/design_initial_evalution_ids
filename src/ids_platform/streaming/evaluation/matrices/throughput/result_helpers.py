from __future__ import annotations

from pathlib import Path


def materialize_sut_summary_row(
    *,
    run_tag: str,
    metrics_rows: list[dict],
    build_legacy_row_fn,
    write_metrics_timeseries_fn,
    annotate_sut_debug_summary_fn,
) -> tuple[dict, Path | None]:
    timeseries_path = write_metrics_timeseries_fn(metrics_rows, run_tag=run_tag)
    legacy_row = build_legacy_row_fn(metrics_rows)
    return annotate_sut_debug_summary_fn(legacy_row), timeseries_path


def persist_summary_rows(
    *,
    summary_csv: str,
    rows: list[dict],
    write_summary_rows_fn,
    label: str,
    resolve_project_path_fn=None,
) -> Path:
    summary_path = resolve_project_path_fn(summary_csv) if resolve_project_path_fn is not None else Path(summary_csv)
    write_summary_rows_fn(summary_path, rows)
    return summary_path
