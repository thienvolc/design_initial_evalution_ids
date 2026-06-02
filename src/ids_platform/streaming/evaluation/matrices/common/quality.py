from __future__ import annotations

from pathlib import Path

QUALITY_SUMMARY_FIELDS = (
    "quality_status",
    "precision",
    "recall",
    "f1",
    "fpr",
    "fnr",
)


def apply_quality_summary_fields(row: dict, quality_summary: dict | None) -> None:
    quality = quality_summary or {}
    for field in QUALITY_SUMMARY_FIELDS:
        row[field] = quality.get(field, "")


def _empty_quality(status: str) -> dict:
    return {
        "quality_status": status,
        "labeled_rows_total": 0,
        "scored_rows_total": 0,
        "tp_total": 0,
        "tn_total": 0,
        "fp_total": 0,
        "fn_total": 0,
        "precision": None,
        "recall": None,
        "f1": None,
        "fpr": None,
        "fnr": None,
        "avg_prediction_score_weighted": None,
        "attack_ratio_weighted": None,
    }


def _ratio(numerator: float, denominator: float) -> float | None:
    if denominator <= 0:
        return None
    return float(numerator / denominator)


def _f1(precision: float | None, recall: float | None) -> float | None:
    if precision is None or recall is None:
        return None
    denominator = precision + recall
    if denominator <= 0:
        return 0.0
    return float((2.0 * precision * recall) / denominator)


def summarize_prediction_quality(artifact_output: Path, *, phase: str = "measure") -> dict:
    path = Path(artifact_output)
    if not path.exists():
        return _empty_quality("quality_missing")
    if path.is_dir() and not any(path.glob("*.parquet")):
        return _empty_quality("quality_missing")

    import pandas as pd

    frame = pd.read_parquet(path)
    if phase and "benchmark_phase" in frame.columns:
        expected_phase = str(phase).strip().lower()
        if expected_phase:
            phases = frame["benchmark_phase"].fillna("measure").astype(str).str.lower()
            frame = frame[phases == expected_phase]
    if frame.empty:
        return _empty_quality("no_predictions")
    required_columns = {"label_binary", "prediction_label"}
    if not required_columns.issubset(frame.columns):
        return _empty_quality("unlabeled")

    labels = pd.to_numeric(frame["label_binary"], errors="coerce")
    predictions = pd.to_numeric(frame["prediction_label"], errors="coerce")
    scored_mask = predictions.notna()
    labeled_mask = labels.notna() & scored_mask
    if not bool(labeled_mask.any()):
        quality = _empty_quality("unlabeled")
        quality["scored_rows_total"] = int(scored_mask.sum())
        return quality

    labels = labels[labeled_mask].astype(int)
    predictions = predictions[labeled_mask].astype(int)
    tp = int(((labels == 1) & (predictions == 1)).sum())
    tn = int(((labels == 0) & (predictions == 0)).sum())
    fp = int(((labels == 0) & (predictions == 1)).sum())
    fn = int(((labels == 1) & (predictions == 0)).sum())
    precision = _ratio(tp, tp + fp)
    recall = _ratio(tp, tp + fn)

    avg_prediction_score = None
    if "prediction_score" in frame.columns:
        scores = pd.to_numeric(frame["prediction_score"], errors="coerce")
        avg_prediction_score = float(scores[scored_mask].mean()) if bool(scored_mask.any()) else None

    return {
        "quality_status": "ok",
        "labeled_rows_total": int(labeled_mask.sum()),
        "scored_rows_total": int(scored_mask.sum()),
        "tp_total": tp,
        "tn_total": tn,
        "fp_total": fp,
        "fn_total": fn,
        "precision": precision,
        "recall": recall,
        "f1": _f1(precision, recall),
        "fpr": _ratio(fp, fp + tn),
        "fnr": _ratio(fn, fn + tp),
        "avg_prediction_score_weighted": avg_prediction_score,
        "attack_ratio_weighted": float(predictions.mean()),
    }
