from __future__ import annotations

import time

from ids_platform.common.paths import resolve_project_path
from ids_platform.streaming.config.common import (
    BenchmarkMatrixConfig,
    BenchmarkRunPlan,
    DEFAULT_TIMING_CONFIG,
    RuntimeProfile,
    benchmark_run_tag,
    build_benchmark_run_config,
    stream_wait_timeout,
)
from ids_platform.streaming.replay.config import RateStep, ReplayRatePlan, ReplaySourceFactory

CapacityRunConfig = BenchmarkRunPlan
CapacityMatrixConfig = BenchmarkMatrixConfig


CAPACITY_SMOKE_PROFILES = (
    RuntimeProfile("capacity_smoke", 500, 4, "5 seconds"),
)

CAPACITY_MAIN_PROFILES = (
    RuntimeProfile("capacity_low", 500, 4, "10 seconds"),
    RuntimeProfile("capacity_mid", 2_000, 8, "10 seconds"),
    RuntimeProfile("capacity_high", 8_000, 16, "10 seconds"),
)

CAPACITY_SMOKE_RATE = ReplayRatePlan(
    rows_per_sec=0.0,
    schedule=(RateStep(rows_per_sec=500, duration_sec=2),),
)

CAPACITY_MAIN_RATE = ReplayRatePlan(
    rows_per_sec=0.0,
    schedule=(
        RateStep(rows_per_sec=5_000, duration_sec=180),
        RateStep(rows_per_sec=10_000, duration_sec=120),
    ),
)


def build_capacity_matrix_config(
    *,
    name: str,
    profiles: tuple[RuntimeProfile, ...],
    source_factory: ReplaySourceFactory,
    rate: ReplayRatePlan,
    summary_csv: str,
    repeats: int = 1,
    model_name: str = "logistic_regression",
    feature_set: str = "full",
    mode: str = "model",
    run_prefix: str = "capacity",
    warmup_source_factory: ReplaySourceFactory | None = None,
    warmup_rate: ReplayRatePlan | None = None,
    metrics_timeout_sec: int = 120,
    metrics_idle_sec: int = 5,
    stream_startup_wait_sec: int = 10,
    stream_wait_timeout_sec: int = 0,
    warmup_stream_wait_timeout_sec: int = 0,
) -> CapacityMatrixConfig:
    source = source_factory.create()
    warmup_source = warmup_source_factory.create() if warmup_source_factory is not None else None
    created_ms = int(time.time() * 1000)
    runs: list[BenchmarkRunPlan] = []

    for repeat_index in range(1, max(int(repeats), 1) + 1):
        for run_index, profile in enumerate(profiles, start=1):
            run_tag = benchmark_run_tag(
                prefix=run_prefix,
                repeat_index=repeat_index,
                run_index=run_index,
                label=profile.name,
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
                    mode=mode,
                    metrics_timeout_sec=0,
                    metrics_idle_sec=0,
                    stream_startup_wait_sec=stream_startup_wait_sec,
                    stream_wait_timeout_sec=stream_wait_timeout(
                        row_count=warmup_source.table.num_rows,
                        rate=warmup_rate,
                        override_seconds=warmup_stream_wait_timeout_sec,
                    ),
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
                mode=mode,
                metrics_timeout_sec=metrics_timeout_sec,
                metrics_idle_sec=metrics_idle_sec,
                stream_startup_wait_sec=stream_startup_wait_sec,
                stream_wait_timeout_sec=stream_wait_timeout(
                    row_count=source.table.num_rows,
                    rate=rate,
                    override_seconds=stream_wait_timeout_sec,
                ),
            )
            runs.append(BenchmarkRunPlan(benchmark=benchmark, warmup=warmup))

    return BenchmarkMatrixConfig(
        name=name,
        runs=tuple(runs),
        summary_csv=resolve_project_path(summary_csv),
    )


CAPACITY_SMOKE_CONFIG = build_capacity_matrix_config(
    name="capacity_smoke",
    profiles=CAPACITY_SMOKE_PROFILES,
    source_factory=ReplaySourceFactory(batch_size=500, row_limit=1_000),
    rate=CAPACITY_SMOKE_RATE,
    summary_csv="artifacts/streaming/evaluation/capacity_smoke.csv",
    run_prefix="capacitySmoke",
    metrics_timeout_sec=120,
    metrics_idle_sec=5,
    stream_startup_wait_sec=10,
    stream_wait_timeout_sec=60,
)

def build_capacity_main_config() -> CapacityMatrixConfig:
    return build_capacity_matrix_config(
        name="capacity_main",
        profiles=CAPACITY_MAIN_PROFILES,
        source_factory=ReplaySourceFactory(batch_size=5_000),
        rate=CAPACITY_MAIN_RATE,
        summary_csv="artifacts/streaming/evaluation/capacity_summary.csv",
        run_prefix="capacity",
        metrics_timeout_sec=120,
        metrics_idle_sec=5,
        stream_startup_wait_sec=10,
    )

CAPACITY_CONFIG = CAPACITY_SMOKE_CONFIG
