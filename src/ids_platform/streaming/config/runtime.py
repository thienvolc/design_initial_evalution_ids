from __future__ import annotations

from pathlib import Path

import pandas as pd

from ids_platform.common.paths import resolve_project_path
from ids_platform.offline.config import load_json
from ids_platform.streaming.runtime.config import (
    RuntimeConfig,
    RuntimeFeatureConfig,
    RuntimeKafkaConfig,
    RuntimeLifecycleConfig,
    RuntimeMetricsConfig,
    RuntimeModelConfig,
    RuntimeOutputConfig,
    RuntimeRunConfig,
    RuntimeSparkConfig,
)
from ids_platform.streaming.runtime.query import safe_tag


FEATURE_MANIFEST_BY_SET = {
    "full": resolve_project_path("artifacts/offline/preprocessing/feature_manifest.json"),
    "reduced": resolve_project_path("artifacts/offline/preprocessing/feature_manifest_reduced.json"),
}

VALID_METRICS_BY_SET = {
    "full": resolve_project_path("artifacts/offline/models/valid_metrics.csv"),
    "reduced": resolve_project_path("artifacts/offline/models/valid_metrics_reduced.csv"),
}

MODEL_ARTIFACT_BY_NAME_AND_FEATURE_SET = {
    ("random_forest", "full"): resolve_project_path("artifacts/offline/models/random_forest.joblib"),
    ("random_forest", "reduced"): resolve_project_path("artifacts/offline/models/random_forest_reduced.joblib"),
    ("logistic_regression", "full"): resolve_project_path("artifacts/offline/models/logistic_regression.joblib"),
    ("logistic_regression", "reduced"): resolve_project_path("artifacts/offline/models/logistic_regression_reduced.joblib"),
    ("gradient_boosting", "full"): resolve_project_path("artifacts/offline/models/gradient_boosting.joblib"),
    ("gradient_boosting", "reduced"): resolve_project_path("artifacts/offline/models/gradient_boosting_reduced.joblib"),
}

STREAMING_CHECKPOINT_ROOT = resolve_project_path("artifacts/streaming/checkpoints")
STREAMING_PREDICTION_ROOT = resolve_project_path("artifacts/streaming/predictions")


def load_thresholds(valid_metrics_csv: Path) -> dict[str, float]:
    if not valid_metrics_csv.exists():
        return {}
    df = pd.read_csv(valid_metrics_csv)
    if "model" not in df.columns or "opt_threshold" not in df.columns:
        return {}

    thresholds: dict[str, float] = {}
    for _, row in df.iterrows():
        try:
            thresholds[str(row["model"]).strip()] = float(row["opt_threshold"])
        except (TypeError, ValueError):
            continue
    return thresholds


def _load_runtime_feature_config(feature_set: str) -> RuntimeFeatureConfig:
    manifest = load_json(FEATURE_MANIFEST_BY_SET[feature_set])
    return RuntimeFeatureConfig(
        feature_set=feature_set,
        columns=[str(column_name) for column_name in manifest.get("feature_columns", [])],
        fill_values={str(key): float(value) for key, value in (manifest.get("imputer_fill_values") or {}).items()},
    )


def _runtime_output_config(*, run_tag: str, model_name: str, feature_set: str) -> RuntimeOutputConfig:
    run_suffix = safe_tag(run_tag) if run_tag.strip() else safe_tag(f"{model_name}_{feature_set}")
    return RuntimeOutputConfig(
        parquet_checkpoint=STREAMING_CHECKPOINT_ROOT / run_suffix / "parquet",
        metrics_checkpoint=STREAMING_CHECKPOINT_ROOT / run_suffix / "metrics",
        sentinel_checkpoint=STREAMING_CHECKPOINT_ROOT / run_suffix / "sentinel",
        artifact_output=STREAMING_PREDICTION_ROOT / run_suffix,
    )


def _runtime_model_config(*, model_name: str, feature_set: str, mode: str) -> RuntimeModelConfig:
    if mode == "pass_through":
        return RuntimeModelConfig(name=model_name, mode="pass_through", artifact_path=None, threshold=None)

    thresholds = load_thresholds(Path(VALID_METRICS_BY_SET[feature_set]))
    return RuntimeModelConfig(
        name=model_name,
        mode="model",
        artifact_path=MODEL_ARTIFACT_BY_NAME_AND_FEATURE_SET[(model_name, feature_set)],
        threshold=float(thresholds.get(model_name, 0.5)),
    )


def build_runtime_config(
    *,
    model_name: str = "logistic_regression",
    feature_set: str = "full",
    run_tag: str = "",
    input_run_tag: str = "",
    load_profile: str = "",
    mode: str = "model",
    starting_offsets: str = "earliest",
    spark_master: str = "local[1]",
    max_offsets_per_trigger: int = 20_000,
    shuffle_partitions: int = 8,
    trigger_interval: str = "10 seconds",
    watermark_delay_sec: int = 0,
    drop_late_events: bool = False,
    reset_outputs: bool = False,
) -> RuntimeConfig:
    model = _runtime_model_config(model_name=model_name, feature_set=feature_set, mode=mode)
    return RuntimeConfig(
        run=RuntimeRunConfig(run_tag=run_tag, input_run_tag=input_run_tag, load_profile=load_profile),
        spark=RuntimeSparkConfig(
            master=spark_master,
            shuffle_partitions=shuffle_partitions,
            trigger_interval=trigger_interval,
        ),
        kafka=RuntimeKafkaConfig(
            starting_offsets=starting_offsets,
            max_offsets_per_trigger=max_offsets_per_trigger,
        ),
        model=model,
        features=_load_runtime_feature_config(feature_set),
        output=_runtime_output_config(
            run_tag=run_tag,
            model_name=model.reported_name,
            feature_set=feature_set,
        ),
        lifecycle=RuntimeLifecycleConfig(
            drop_late_events=drop_late_events,
            watermark_delay_sec=watermark_delay_sec,
            reset_outputs=reset_outputs,
        ),
        metrics=RuntimeMetricsConfig(),
    )
