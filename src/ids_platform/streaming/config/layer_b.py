from __future__ import annotations

import time

from ids_platform.common.paths import resolve_project_path
from ids_platform.streaming.config.common import (
    BenchmarkMatrixConfig,
    BenchmarkRunPlan,
    DEFAULT_TIMING_CONFIG,
    RuntimeProfile,
    SMOKE_BATCH_SIZE,
    SMOKE_METRICS_IDLE_SEC,
    SMOKE_METRICS_TIMEOUT_SEC,
    SMOKE_REPLAY_RATE,
    SMOKE_ROW_LIMIT,
    SMOKE_STREAM_STARTUP_WAIT_SEC,
    SMOKE_STREAM_WAIT_TIMEOUT_SEC,
    benchmark_run_tag,
    build_benchmark_run_config,
    stream_wait_timeout,
)
from ids_platform.streaming.replay.config import RateStep, ReplayRatePlan, ReplaySourceFactory


LAYER_B_SMOKE_RATE = SMOKE_REPLAY_RATE

LAYER_B_LIGHT_RATE = ReplayRatePlan(
    rows_per_sec=0.0,
    schedule=(RateStep(rows_per_sec=5_000, duration_sec=120),),
)

LAYER_B_MAIN_RATE = ReplayRatePlan(
    rows_per_sec=0.0,
    schedule=(
        RateStep(rows_per_sec=5_000, duration_sec=180),
        RateStep(rows_per_sec=10_000, duration_sec=120),
    ),
)

LAYER_B_WARMUP_RATE = ReplayRatePlan(
    rows_per_sec=0.0,
    schedule=(RateStep(rows_per_sec=1_000, duration_sec=50),),
)

LAYER_B_SMOKE_PAIRS = (("logistic_regression", "full"),)
LAYER_B_LIGHT_PAIRS = (
    ("logistic_regression", "reduced"),
    ("random_forest", "reduced"),
)
LAYER_B_MAIN_PAIRS = (
    ("logistic_regression", "reduced"),
    ("random_forest", "reduced"),
    ("random_forest", "full"),
    ("gradient_boosting", "reduced"),
)


def _profile(model_name: str, feature_set: str) -> RuntimeProfile:
    return RuntimeProfile(f"{model_name}:{feature_set}", 20_000, 8, "10 seconds")


def build_layer_b_matrix_config(
    *,
    name: str,
    model_feature_pairs: tuple[tuple[str, str], ...],
    source_factory: ReplaySourceFactory,
    rate: ReplayRatePlan,
    summary_csv: str,
    repeats: int = 1,
    run_prefix: str = "layerB",
    warmup_source_factory: ReplaySourceFactory | None = None,
    warmup_rate: ReplayRatePlan | None = None,
    metrics_timeout_sec: int = 120,
    metrics_idle_sec: int = 5,
    stream_startup_wait_sec: int = 10,
    stream_wait_timeout_sec: int = 0,
    warmup_stream_wait_timeout_sec: int = 0,
) -> BenchmarkMatrixConfig:
    source = source_factory.create()
    warmup_source = warmup_source_factory.create() if warmup_source_factory is not None else None
    created_ms = int(time.time() * 1000)
    runs: list[BenchmarkRunPlan] = []
    run_index = 0

    for repeat_index in range(1, max(int(repeats), 1) + 1):
        for model_name, feature_set in model_feature_pairs:
            run_index += 1
            profile = _profile(model_name, feature_set)
            run_tag = benchmark_run_tag(
                prefix=run_prefix,
                repeat_index=repeat_index,
                run_index=run_index,
                label=f"{model_name}_{feature_set}",
                created_ms=created_ms,
            )
            warmup = None
            if warmup_source is not None and warmup_rate is not None:
                warmup = build_benchmark_run_config(
                    run_tag=f"{run_tag}_warmup",
                    repeat_index=repeat_index,
                    profile=profile,
                    source=warmup_source,
                    rate=warmup_rate,
                    timing=DEFAULT_TIMING_CONFIG,
                    model_name=model_name,
                    feature_set=feature_set,
                    metrics_timeout_sec=0,
                    metrics_idle_sec=0,
                    stream_startup_wait_sec=stream_startup_wait_sec,
                    stream_wait_timeout_sec=stream_wait_timeout(
                        row_count=warmup_source.table.num_rows,
                        rate=warmup_rate,
                        override_seconds=warmup_stream_wait_timeout_sec,
                    ),
                    load_profile=f"{model_name}:{feature_set}:warmup",
                )

            benchmark = build_benchmark_run_config(
                run_tag=run_tag,
                repeat_index=repeat_index,
                profile=profile,
                source=source,
                rate=rate,
                timing=DEFAULT_TIMING_CONFIG,
                model_name=model_name,
                feature_set=feature_set,
                metrics_timeout_sec=metrics_timeout_sec,
                metrics_idle_sec=metrics_idle_sec,
                stream_startup_wait_sec=stream_startup_wait_sec,
                stream_wait_timeout_sec=stream_wait_timeout(
                    row_count=source.table.num_rows,
                    rate=rate,
                    override_seconds=stream_wait_timeout_sec,
                ),
                load_profile=f"{model_name}:{feature_set}",
            )
            runs.append(BenchmarkRunPlan(benchmark=benchmark, warmup=warmup))

    return BenchmarkMatrixConfig(
        name=name,
        runs=tuple(runs),
        summary_csv=resolve_project_path(summary_csv),
    )


LAYER_B_SMOKE_CONFIG = build_layer_b_matrix_config(
    name="layer_b_smoke",
    model_feature_pairs=LAYER_B_SMOKE_PAIRS,
    source_factory=ReplaySourceFactory(batch_size=SMOKE_BATCH_SIZE, row_limit=SMOKE_ROW_LIMIT),
    rate=LAYER_B_SMOKE_RATE,
    summary_csv="artifacts/streaming/evaluation/layer_b_smoke.csv",
    run_prefix="layerBSmoke",
    metrics_timeout_sec=SMOKE_METRICS_TIMEOUT_SEC,
    metrics_idle_sec=SMOKE_METRICS_IDLE_SEC,
    stream_startup_wait_sec=SMOKE_STREAM_STARTUP_WAIT_SEC,
    stream_wait_timeout_sec=SMOKE_STREAM_WAIT_TIMEOUT_SEC,
)

def build_layer_b_light_config() -> BenchmarkMatrixConfig:
    return build_layer_b_matrix_config(
        name="layer_b_light",
        model_feature_pairs=LAYER_B_LIGHT_PAIRS,
        source_factory=ReplaySourceFactory(batch_size=5_000, row_limit=200_000),
        rate=LAYER_B_LIGHT_RATE,
        warmup_source_factory=ReplaySourceFactory(batch_size=5_000, row_limit=20_000),
        warmup_rate=ReplayRatePlan(rows_per_sec=0.0, schedule=(RateStep(rows_per_sec=1_000, duration_sec=20),)),
        summary_csv="artifacts/streaming/evaluation/layer_b_summary_light.csv",
        run_prefix="layerB_light",
        metrics_timeout_sec=900,
    )


def build_layer_b_main_config() -> BenchmarkMatrixConfig:
    return build_layer_b_matrix_config(
        name="layer_b_500k",
        model_feature_pairs=LAYER_B_MAIN_PAIRS,
        source_factory=ReplaySourceFactory(batch_size=5_000, row_limit=125_000),
        rate=LAYER_B_MAIN_RATE,
        warmup_source_factory=ReplaySourceFactory(batch_size=5_000, row_limit=50_000),
        warmup_rate=LAYER_B_WARMUP_RATE,
        summary_csv="artifacts/streaming/evaluation/layer_b_summary_500k.csv",
        run_prefix="layerB_scaleup",
        metrics_timeout_sec=1_500,
    )

LAYER_B_CONFIG = LAYER_B_SMOKE_CONFIG
