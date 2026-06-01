from __future__ import annotations

import time
from dataclasses import dataclass

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
    SMOKE_RPS,
    SMOKE_STREAM_STARTUP_WAIT_SEC,
    SMOKE_STREAM_WAIT_TIMEOUT_SEC,
    benchmark_run_tag,
    build_benchmark_run_config,
    stream_wait_timeout,
)
from ids_platform.streaming.replay.config import RateStep, ReplayRatePlan, ReplaySourceFactory


@dataclass(frozen=True)
class LoadProfile:
    name: str
    rows_per_sec: float
    max_rows: int
    rate_schedule: tuple[RateStep, ...] = ()

    @property
    def rate(self) -> ReplayRatePlan:
        if self.rate_schedule:
            return ReplayRatePlan(rows_per_sec=0.0, schedule=self.rate_schedule)
        return ReplayRatePlan(rows_per_sec=float(self.rows_per_sec), schedule=())

    @property
    def reported_rows_per_sec(self) -> float:
        if not self.rate_schedule:
            return float(self.rows_per_sec)
        total_seconds = sum(step.duration_sec for step in self.rate_schedule)
        if total_seconds <= 0:
            return 0.0
        return sum(step.rows_per_sec * step.duration_sec for step in self.rate_schedule) / total_seconds

    @property
    def reported_schedule(self) -> str:
        return ",".join(f"{step.rows_per_sec:g}:{step.duration_sec:g}" for step in self.rate_schedule)


LOAD_QUALITY_SMOKE_PROFILES = (
    LoadProfile("smoke", SMOKE_RPS, SMOKE_ROW_LIMIT, SMOKE_REPLAY_RATE.schedule),
)

LOAD_QUALITY_LOCAL_PROFILES = (
    LoadProfile("low", 2_000, 20_000),
    LoadProfile("medium", 4_000, 30_000),
    LoadProfile("high", 8_000, 40_000),
)

LOAD_QUALITY_MAIN_PROFILES = (
    LoadProfile(
        "trace_step_rate_main",
        0.0,
        150_000,
        (
            RateStep(rows_per_sec=5_000, duration_sec=180),
            RateStep(rows_per_sec=10_000, duration_sec=180),
            RateStep(rows_per_sec=20_000, duration_sec=139.8286),
        ),
    ),
)

LOAD_QUALITY_BURSTY_PROFILES = (
    LoadProfile(
        "trace_bursty_secondary",
        0.0,
        5_496_572,
        (
            RateStep(rows_per_sec=10_000, duration_sec=120),
            RateStep(rows_per_sec=30_000, duration_sec=20),
            RateStep(rows_per_sec=10_000, duration_sec=120),
            RateStep(rows_per_sec=40_000, duration_sec=20),
            RateStep(rows_per_sec=10_000, duration_sec=120),
        ),
    ),
)

LOAD_QUALITY_WARMUP_RATE = ReplayRatePlan(
    rows_per_sec=0.0,
    schedule=(RateStep(rows_per_sec=5_000, duration_sec=24),),
)


def _runtime_profile(profile: LoadProfile) -> RuntimeProfile:
    return RuntimeProfile(profile.name, 20_000, 8, "10 seconds")


def build_load_quality_matrix_config(
    *,
    name: str,
    profiles: tuple[LoadProfile, ...],
    summary_csv: str,
    model_name: str = "random_forest",
    feature_set: str = "full",
    batch_size: int = 10_000,
    run_prefix: str = "load",
    warmup_rows: int = 0,
    warmup_rate: ReplayRatePlan | None = None,
    metrics_timeout_sec: int = 120,
    metrics_idle_sec: int = 5,
    stream_startup_wait_sec: int = 10,
    stream_wait_timeout_sec: int = 0,
    warmup_stream_wait_timeout_sec: int = 0,
) -> BenchmarkMatrixConfig:
    created_ms = int(time.time() * 1000)
    runs: list[BenchmarkRunPlan] = []

    for run_index, load_profile in enumerate(profiles, start=1):
        source = ReplaySourceFactory(batch_size=batch_size, row_limit=load_profile.max_rows).create()
        profile = _runtime_profile(load_profile)
        run_tag = benchmark_run_tag(
            prefix=run_prefix,
            repeat_index=1,
            run_index=run_index,
            label=load_profile.name,
            created_ms=created_ms,
        )
        warmup = None
        if warmup_rows > 0 and warmup_rate is not None:
            warmup_source = ReplaySourceFactory(batch_size=batch_size, row_limit=warmup_rows).create()
            warmup = build_benchmark_run_config(
                run_tag=f"{run_tag}_warmup",
                repeat_index=1,
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
                load_profile=f"{load_profile.name}_warmup",
            )

        benchmark = build_benchmark_run_config(
            run_tag=run_tag,
            repeat_index=1,
            profile=profile,
            source=source,
            rate=load_profile.rate,
            timing=DEFAULT_TIMING_CONFIG,
            model_name=model_name,
            feature_set=feature_set,
            metrics_timeout_sec=metrics_timeout_sec,
            metrics_idle_sec=metrics_idle_sec,
            stream_startup_wait_sec=stream_startup_wait_sec,
            stream_wait_timeout_sec=stream_wait_timeout(
                row_count=source.table.num_rows,
                rate=load_profile.rate,
                override_seconds=stream_wait_timeout_sec,
            ),
            load_profile=load_profile.name,
        )
        runs.append(
            BenchmarkRunPlan(
                benchmark=benchmark,
                warmup=warmup,
                summary_context={
                    "load_profile": load_profile.name,
                    "rows_per_sec_target": load_profile.reported_rows_per_sec,
                    "max_rows": load_profile.max_rows,
                    "rate_schedule": load_profile.reported_schedule,
                },
            )
        )

    return BenchmarkMatrixConfig(
        name=name,
        runs=tuple(runs),
        summary_csv=resolve_project_path(summary_csv),
    )


LOAD_QUALITY_SMOKE_CONFIG = build_load_quality_matrix_config(
    name="load_quality_smoke",
    profiles=LOAD_QUALITY_SMOKE_PROFILES,
    summary_csv="artifacts/streaming/evaluation/load_quality_smoke.csv",
    model_name="random_forest",
    batch_size=SMOKE_BATCH_SIZE,
    run_prefix="loadSmoke",
    metrics_timeout_sec=SMOKE_METRICS_TIMEOUT_SEC,
    metrics_idle_sec=SMOKE_METRICS_IDLE_SEC,
    stream_startup_wait_sec=SMOKE_STREAM_STARTUP_WAIT_SEC,
    stream_wait_timeout_sec=SMOKE_STREAM_WAIT_TIMEOUT_SEC,
)

def build_load_quality_local_config() -> BenchmarkMatrixConfig:
    return build_load_quality_matrix_config(
        name="load_quality_local_medium",
        profiles=LOAD_QUALITY_LOCAL_PROFILES,
        summary_csv="artifacts/streaming/evaluation/load_quality_summary_local_medium.csv",
        batch_size=3_000,
        run_prefix="loadLocal",
        warmup_rows=20_000,
        warmup_rate=ReplayRatePlan(rows_per_sec=0.0, schedule=(RateStep(rows_per_sec=800, duration_sec=25),)),
        metrics_timeout_sec=1_200,
        metrics_idle_sec=20,
    )


def build_load_quality_main_config() -> BenchmarkMatrixConfig:
    return build_load_quality_matrix_config(
        name="load_quality_stress_main",
        profiles=LOAD_QUALITY_MAIN_PROFILES,
        summary_csv="artifacts/streaming/evaluation/load_quality_summary_stress_full_testx4.csv",
        batch_size=10_000,
        run_prefix="loadStress",
        warmup_rows=120_000,
        warmup_rate=LOAD_QUALITY_WARMUP_RATE,
        metrics_timeout_sec=3_600,
    )


def build_load_quality_bursty_config() -> BenchmarkMatrixConfig:
    return build_load_quality_matrix_config(
        name="load_quality_bursty",
        profiles=LOAD_QUALITY_BURSTY_PROFILES,
        summary_csv="artifacts/streaming/evaluation/load_quality_summary_bursty_trace.csv",
        batch_size=10_000,
        run_prefix="loadBursty",
        warmup_rows=120_000,
        warmup_rate=LOAD_QUALITY_WARMUP_RATE,
        metrics_timeout_sec=3_600,
    )

LOAD_QUALITY_CONFIG = LOAD_QUALITY_SMOKE_CONFIG
