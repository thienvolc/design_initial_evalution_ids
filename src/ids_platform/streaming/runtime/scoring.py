from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pandas as pd

if TYPE_CHECKING:
    from pyspark.sql.column import Column

_MODEL_CACHE: dict[str, object] = {}


def _load_model(model_path: str):
    import joblib

    model = _MODEL_CACHE.get(model_path)
    if model is None:
        model = joblib.load(model_path)
        _MODEL_CACHE[model_path] = model
    return model


def _score_frame(*, model_path: str, feature_columns: list[str], fill_values: dict[str, float], frame: pd.DataFrame):
    import numpy as np

    model = _load_model(model_path)
    frame.columns = feature_columns
    frame = frame.apply(pd.to_numeric, errors="coerce")

    for col in feature_columns:
        if col in fill_values:
            frame[col] = frame[col].fillna(float(fill_values[col]))

    frame = frame.fillna(0.0)
    if hasattr(model, "predict_proba"):
        scores = model.predict_proba(frame)[:, 1]
    else:
        scores = model.predict(frame)
    return np.asarray(scores, dtype=float)


def make_score_udf(model_path: str, feature_columns: list[str], fill_values: dict[str, float]):
    from pyspark.sql.types import DoubleType
    from pyspark.sql.functions import pandas_udf

    @pandas_udf(DoubleType())
    def _score_udf(*cols: pd.Series) -> pd.Series:
        frame = pd.concat(cols, axis=1)
        return pd.Series(
            _score_frame(
                model_path=model_path,
                feature_columns=feature_columns,
                fill_values=fill_values,
                frame=frame,
            )
        )

    return _score_udf


def add_prediction_columns(
    prepared_df,
    *,
    score_udf,
    feature_columns: list[str],
    threshold: float,
    model_name: str,
    feature_set: str,
    run_tag: str,
):
    from pyspark.sql import functions as F

    prediction_score_column = cast(
        "Column",
        score_udf(*[F.col(feature_name) for feature_name in feature_columns]),
    )
    prediction_label_condition = cast(
        "Column",
        prediction_score_column >= F.lit(threshold),
    )
    projected_columns = [F.col(column_name) for column_name in prepared_df.columns]
    projected_columns.extend(
        [
            prediction_score_column.alias("prediction_score"),
            F.when(prediction_label_condition, F.lit(1)).otherwise(F.lit(0)).alias("prediction_label"),
            F.current_timestamp().alias("emit_time"),
            F.lit(model_name).alias("model_name"),
            F.lit(feature_set).alias("feature_set"),
            F.lit(run_tag).alias("run_tag"),
            F.lit(float(threshold)).alias("threshold_used"),
        ]
    )
    return prepared_df.select(*projected_columns)


def add_passthrough_prediction_columns(
    prepared_df,
    *,
    model_name: str,
    feature_set: str,
    run_tag: str,
):
    from pyspark.sql import functions as F

    projected_columns = [F.col(column_name) for column_name in prepared_df.columns]
    projected_columns.extend(
        [
            F.lit(None).cast("double").alias("prediction_score"),
            F.lit(None).cast("int").alias("prediction_label"),
            F.current_timestamp().alias("emit_time"),
            F.lit(model_name).alias("model_name"),
            F.lit(feature_set).alias("feature_set"),
            F.lit(run_tag).alias("run_tag"),
            F.lit(None).cast("double").alias("threshold_used"),
        ]
    )
    return prepared_df.select(*projected_columns)
