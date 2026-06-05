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
    RuntimeModelConfig,
    RuntimeOutputConfig,
    RuntimeRunConfig,
    RuntimeSparkConfig,
)
from ids_platform.streaming.runtime.query import safe_tag


PRIMARY_STREAMING_MODEL_NAME = "random_forest"
PRIMARY_STREAMING_FEATURE_SET = "full"
PRIMARY_STREAMING_MODEL_LABEL = "RF-Full"

FEATURE_MANIFEST_BY_SET = {
    "full": resolve_project_path("artifacts/offline/spark/full/preprocessing/feature_manifest.json"),
    "reduced": resolve_project_path("artifacts/offline/spark/reduced/preprocessing/feature_manifest.json"),
}

VALID_METRICS_BY_SET = {
    "full": resolve_project_path("artifacts/offline/spark/full/models/valid_metrics.csv"),
    "reduced": resolve_project_path("artifacts/offline/spark/reduced/models/valid_metrics.csv"),
}

MODEL_ARTIFACT_BY_NAME_AND_FEATURE_SET = {
    ("random_forest", "full"): resolve_project_path("artifacts/offline/spark/full/models/random_forest"),
    ("random_forest", "reduced"): resolve_project_path("artifacts/offline/spark/reduced/models/random_forest"),
    ("logistic_regression", "full"): resolve_project_path("artifacts/offline/spark/full/models/logistic_regression"),
    ("gradient_boosting", "full"): resolve_project_path("artifacts/offline/spark/full/models/gradient_boosting"),
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


def load_operating_point_threshold(*, model_name: str, feature_set: str) -> float | None:
    path = resolve_project_path(
        f"artifacts/offline/spark/{feature_set}/models/operating_points_{model_name}.json"
    )
    if not path.exists():
        return None
    payload = load_json(path)
    try:
        return float(payload["opt_threshold"])
    except (KeyError, TypeError, ValueError):
        return None


def _load_runtime_feature_config(feature_set: str) -> RuntimeFeatureConfig:
    manifest = load_json(FEATURE_MANIFEST_BY_SET[feature_set])
    return RuntimeFeatureConfig(
        feature_set=feature_set,
        columns=[str(column_name) for column_name in manifest.get("feature_columns", [])],
    )


def _runtime_output_config(*, run_tag: str, model_name: str, feature_set: str) -> RuntimeOutputConfig:
    run_suffix = safe_tag(run_tag) if run_tag.strip() else safe_tag(f"{model_name}_{feature_set}")
    return RuntimeOutputConfig(
        prediction_checkpoint=STREAMING_CHECKPOINT_ROOT / run_suffix / "prediction",
    )


def _runtime_model_config(*, model_name: str, feature_set: str, mode: str) -> RuntimeModelConfig:
    if mode == "pass_through":
        return RuntimeModelConfig(name=model_name, mode="pass_through", artifact_path=None, threshold=None)

    thresholds = load_thresholds(Path(VALID_METRICS_BY_SET[feature_set]))
    threshold = thresholds.get(model_name)
    if threshold is None:
        threshold = load_operating_point_threshold(model_name=model_name, feature_set=feature_set)
    return RuntimeModelConfig(
        name=model_name,
        mode="model",
        artifact_path=MODEL_ARTIFACT_BY_NAME_AND_FEATURE_SET[(model_name, feature_set)],
        threshold=float(threshold if threshold is not None else 0.5),
    )


def build_runtime_config(
    *,
    model_name: str = PRIMARY_STREAMING_MODEL_NAME,
    feature_set: str = PRIMARY_STREAMING_FEATURE_SET,
    run_tag: str = "",
    input_run_tag: str = "",
    load_profile: str = "",
    mode: str = "model",
    input_topic: str = "ids.raw.flows",
    prediction_topic: str = "ids.predictions",
    starting_offsets: str = "earliest",
    spark_master: str = "local[1]",
    max_offsets_per_trigger: int = 20_000,
    shuffle_partitions: int = 8,
    trigger_interval: str = "10 seconds",
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
            input_topic=input_topic,
            prediction_topic=prediction_topic,
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
        lifecycle=RuntimeLifecycleConfig(reset_outputs=reset_outputs),
    )
