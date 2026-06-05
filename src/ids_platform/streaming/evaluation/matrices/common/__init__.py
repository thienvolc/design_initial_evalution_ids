from __future__ import annotations

from pathlib import Path

from ids_platform.streaming.evaluation.matrices.common import outputs, quality


def summarize_prediction_quality(artifact_output: str | Path, *, phase: str = "measure") -> dict:
    return quality.summarize_prediction_quality(Path(artifact_output), phase=phase)


def summarize_prediction_latency(artifact_output: str | Path, *, phase: str = "measure") -> dict:
    return quality.summarize_prediction_latency(Path(artifact_output), phase=phase)


def apply_quality_summary_fields(row: dict, quality_summary: dict | None) -> None:
    quality.apply_quality_summary_fields(row, quality_summary)


def write_summary_rows(path: Path, rows: list[dict]) -> None:
    outputs.write_summary_rows(path, rows)
