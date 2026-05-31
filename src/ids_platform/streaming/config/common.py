from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from ids_platform.common.paths import resolve_project_path
from ids_platform.offline.config import load_json
from ids_platform.streaming.core.artifacts import load_thresholds
from ids_platform.streaming.replay.config import (
    RateStep,
    ReplayConfig,
    ReplayRatePlan,
    ReplayRuntimeConfig,
    ReplaySource,
    ReplayTimingConfig,
)
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

if TYPE_CHECKING:
    from ids_platform.streaming.evaluation.matrices.throughput.benchmark_run import (
        BenchmarkRunConfig,
    )


@dataclass(frozen=True)
class RuntimeProfile:
    name: str
    max_offsets_per_trigger: int
    shuffle_partitions: int
    trigger_interval: str = "10 seconds"

    def to_legacy_dict(self) -> dict:
        return {
            "name": self.name,
            "max_offsets_per_trigger": int(self.max_offsets_per_trigger),
            "shuffle_partitions": int(self.shuffle_partitions),
            "trigger_interval": self.trigger_interval,
        }


@dataclass(frozen=True)
class BenchmarkRunPlan:
    benchmark: "BenchmarkRunConfig"
    warmup: "BenchmarkRunConfig | None" = None
    summary_context: dict | None = None


@dataclass(frozen=True)
class BenchmarkMatrixConfig:
    name: str
    runs: tuple[BenchmarkRunPlan, ...]
    summary_csv: Path


DEFAULT_REPLAY_RUNTIME_CONFIG = ReplayRuntimeConfig(
    run_tag="",
    bootstrap_servers="kafka:29092",
    topic="ids.raw.flows",
)

DEFAULT_TIMING_CONFIG = ReplayTimingConfig(
    random_seed=42,
    trace_order_column="timestamp",
    reorder_window_size=0,
    late_event_ratio=0.0,
    late_event_max_sec=0.0,
)

NO_THROTTLE_RATE = ReplayRatePlan(rows_per_sec=0.0, schedule=())

LAYER_MAIN_RATE = ReplayRatePlan(
    rows_per_sec=0.0,
    schedule=(
        RateStep(rows_per_sec=5_000, duration_sec=180),
        RateStep(rows_per_sec=10_000, duration_sec=120),
    ),
)

LAYER_WARMUP_RATE = ReplayRatePlan(
    rows_per_sec=0.0,
    schedule=(RateStep(rows_per_sec=1_000, duration_sec=50),),
)

LAYER_C_WARMUP_RATE = ReplayRatePlan(
    rows_per_sec=0.0,
    schedule=(RateStep(rows_per_sec=5_000, duration_sec=24),),
)


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

DEFAULT_RUNTIME_SPARK_CONFIG = RuntimeSparkConfig()
DEFAULT_RUNTIME_KAFKA_CONFIG = RuntimeKafkaConfig()
DEFAULT_RUNTIME_METRICS_CONFIG = RuntimeMetricsConfig()


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
        metrics=DEFAULT_RUNTIME_METRICS_CONFIG,
    )


def benchmark_run_tag(
    *,
    prefix: str,
    repeat_index: int,
    run_index: int,
    label: str,
    created_ms: int | None = None,
) -> str:
    suffix_ms = int(time.time() * 1000) if created_ms is None else int(created_ms)
    clean_label = safe_tag(label)
    return f"{prefix}_r{repeat_index:02d}_{run_index:02d}_{clean_label}_{suffix_ms}"


def stream_wait_timeout(*, row_count: int, rate: ReplayRatePlan, override_seconds: int = 0) -> int:
    if override_seconds > 0:
        return max(int(override_seconds), 30)
    expected_replay_seconds = rate.expected_elapsed_for_rows(max(int(row_count), 0))
    return max(int(expected_replay_seconds) + 120, 120)


def runtime_mode(mode: str) -> str:
    return "pass_through" if str(mode).strip().lower() == "pass_through" else "model"


def model_label(*, model_name: str, mode: str) -> str:
    return "pass_through" if runtime_mode(mode) == "pass_through" else model_name


def make_replay_config(
    *,
    run_tag: str,
    runtime: RuntimeConfig,
    source: ReplaySource,
    rate: ReplayRatePlan,
    timing: ReplayTimingConfig,
) -> ReplayConfig:
    return ReplayConfig(
        source=source,
        runtime=ReplayRuntimeConfig(
            run_tag=run_tag,
            bootstrap_servers=runtime.kafka.bootstrap_servers,
            topic=runtime.kafka.input_topic,
        ),
        rate=rate,
        timing=timing,
    )


def build_benchmark_run_config(
    *,
    run_tag: str,
    repeat_index: int,
    profile: RuntimeProfile,
    source: ReplaySource,
    rate: ReplayRatePlan,
    timing: ReplayTimingConfig,
    model_name: str,
    feature_set: str,
    mode: str = "model",
    metrics_timeout_sec: int,
    metrics_idle_sec: int,
    stream_startup_wait_sec: int,
    stream_wait_timeout_sec: int,
    load_profile: str = "",
    watermark_delay_sec: int = 0,
    drop_late_events: bool = False,
) -> "BenchmarkRunConfig":
    from ids_platform.streaming.evaluation.matrices.throughput.benchmark_run import (
        BenchmarkRunConfig,
    )

    runtime = build_runtime_config(
        model_name=model_name,
        feature_set=feature_set,
        run_tag=run_tag,
        input_run_tag=run_tag,
        load_profile=load_profile or profile.name,
        mode=runtime_mode(mode),
        max_offsets_per_trigger=profile.max_offsets_per_trigger,
        shuffle_partitions=profile.shuffle_partitions,
        trigger_interval=profile.trigger_interval,
        watermark_delay_sec=watermark_delay_sec,
        drop_late_events=drop_late_events,
        reset_outputs=True,
    )
    return BenchmarkRunConfig(
        run_tag=run_tag,
        runtime=runtime,
        replay=make_replay_config(
            run_tag=run_tag,
            runtime=runtime,
            source=source,
            rate=rate,
            timing=timing,
        ),
        profile=profile,
        repeat_index=repeat_index,
        model_label=model_label(model_name=model_name, mode=mode),
        feature_set=feature_set,
        metrics_timeout_sec=metrics_timeout_sec,
        metrics_idle_sec=metrics_idle_sec,
        stream_startup_wait_sec=stream_startup_wait_sec,
        stream_wait_timeout_sec=stream_wait_timeout_sec,
    )
