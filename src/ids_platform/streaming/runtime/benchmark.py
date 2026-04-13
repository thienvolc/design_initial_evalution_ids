from __future__ import annotations

import time
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from ids_platform.streaming.config import StreamingModelConfig

SOURCE_LATENCY_MODES = {"auto", "source_timestamp", "replay_relative", "disabled"}

_MODEL_CACHE: dict[str, object] = {}


def metrics_from_counts(tn: int, fp: int, fn: int, tp: int) -> dict[str, float]:
    total = tn + fp + fn + tp
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    accuracy = (tp + tn) / total if total else 0.0
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
    fpr = fp / (fp + tn) if (fp + tn) else 0.0
    fnr = fn / (fn + tp) if (fn + tp) else 0.0
    return {
        "accuracy": round(float(accuracy), 6),
        "precision": round(float(precision), 6),
        "recall": round(float(recall), 6),
        "f1": round(float(f1), 6),
        "fpr": round(float(fpr), 6),
        "fnr": round(float(fnr), 6),
    }


def unique_preserve(items: list[str]) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        output.append(item)
    return output


def first_existing(candidates: list[str], available: set[str]) -> str | None:
    for name in candidates:
        if name in available:
            return name
    return None


def make_struct_score_udf(model_path: str, feature_columns: list[str], fill_values: dict[str, float]):
    from pyspark.sql.functions import pandas_udf
    from pyspark.sql.types import DoubleType, StructField, StructType

    output_schema = StructType(
        [
            StructField("score", DoubleType(), nullable=False),
            StructField("processing_ms", DoubleType(), nullable=False),
        ]
    )

    @pandas_udf(output_schema)
    def _score_udf(feature_struct: pd.DataFrame) -> pd.DataFrame:
        import joblib

        model = _MODEL_CACHE.get(model_path)
        if model is None:
            model = joblib.load(model_path)
            _MODEL_CACHE[model_path] = model

        if isinstance(feature_struct, pd.DataFrame):
            frame = feature_struct.copy()
        else:
            frame = pd.DataFrame(feature_struct)

        frame = frame.reindex(columns=feature_columns)
        frame = frame.apply(pd.to_numeric, errors="coerce")

        for column_name in feature_columns:
            if column_name in fill_values:
                frame[column_name] = frame[column_name].fillna(float(fill_values[column_name]))

        frame = frame.fillna(0.0)
        started = time.perf_counter()

        if hasattr(model, "predict_proba"):
            scores = model.predict_proba(frame)[:, 1]
        else:
            scores = model.predict(frame)

        elapsed_ms = (time.perf_counter() - started) * 1000.0
        per_row_ms = elapsed_ms / max(len(frame), 1)

        return pd.DataFrame(
            {
                "score": np.asarray(scores, dtype=np.float64),
                "processing_ms": np.full(len(frame), per_row_ms, dtype=np.float64),
            }
        )

    return _score_udf


def build_active_model_specs(
    model_configs: list["StreamingModelConfig"],
    *,
    selected_names: set[str] | None,
    thresholds: dict[str, float],
    feature_set: str,
    resolve_model_path,
) -> list[dict[str, object]]:
    active_models: list[dict[str, object]] = []
    for model_config in model_configs:
        model_name = model_config.name
        if not model_name or not model_config.enabled:
            continue
        if selected_names is not None and model_name not in selected_names:
            continue
        if not model_config.joblib_path and not model_config.joblib_path_by_feature_set:
            continue

        threshold = model_config.threshold
        if threshold is None:
            threshold = thresholds.get(model_name, 0.5)

        active_models.append(
            {
                "name": model_name,
                "joblib_path": str(resolve_model_path(model_config.raw, feature_set)),
                "threshold": float(threshold),
            }
        )

    return active_models
