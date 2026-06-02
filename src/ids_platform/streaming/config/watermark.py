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
    build_benchmark_run_plan,
)
from ids_platform.streaming.replay.config import (
    RateStep,
    ReplayRatePlan,
    ReplaySourceFactory,
    ReplayTimingConfig,
)


WATERMARK_MAIN_RATE = ReplayRatePlan(
    rows_per_sec=0.0,
    schedule=(
        RateStep(rows_per_sec=5_000, duration_sec=180),
        RateStep(rows_per_sec=10_000, duration_sec=120),
    ),
)

WATERMARK_SMOKE_RATE = SMOKE_REPLAY_RATE

WATERMARK_WARMUP_RATE = ReplayRatePlan(
    rows_per_sec=0.0,
    schedule=(RateStep(rows_per_sec=1_000, duration_sec=50),),
)

WATERMARK_LATE_INJECTION_TIMING_CONFIG = ReplayTimingConfig(
    random_seed=42,
    trace_order_column="event_time",
    reorder_window_size=500,
    late_event_ratio=0.15,
    late_event_max_sec=8,
)


def _profile(delay_sec: int) -> RuntimeProfile:
    return RuntimeProfile(f"watermark_delay_{delay_sec}s", 20_000, 8, "10 seconds")


def build_watermark_matrix_config(
    *,
    name: str,
    delays_sec: tuple[int, ...],
    source_factory: ReplaySourceFactory,
    rate: ReplayRatePlan,
    summary_csv: str,
    model_name: str = "random_forest",
    feature_set: str = "full",
    run_prefix: str = "watermark",
    drop_late_events: bool = True,
    timing: ReplayTimingConfig = DEFAULT_TIMING_CONFIG,
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

    for run_index, delay_sec in enumerate(delays_sec, start=1):
        profile = _profile(delay_sec)
        run_tag = benchmark_run_tag(
            prefix=run_prefix,
            repeat_index=1,
            run_index=run_index,
            label=profile.name,
            created_ms=created_ms,
        )
        runs.append(
            build_benchmark_run_plan(
                run_tag=run_tag,
                repeat_index=1,
                profile=profile,
                source=source,
                rate=rate,
                timing=timing,
                model_name=model_name,
                feature_set=feature_set,
                metrics_timeout_sec=metrics_timeout_sec,
                metrics_idle_sec=metrics_idle_sec,
                stream_startup_wait_sec=stream_startup_wait_sec,
                stream_wait_timeout_sec=stream_wait_timeout_sec,
                watermark_delay_sec=delay_sec,
                drop_late_events=drop_late_events,
                warmup_source=warmup_source,
                warmup_rate=warmup_rate,
                warmup_load_profile=f"{profile.name}_warmup",
                warmup_stream_wait_timeout_sec=warmup_stream_wait_timeout_sec,
                summary_context={
                    "watermark_delay_sec": delay_sec,
                    "drop_late_events": drop_late_events,
                },
            )
        )

    return BenchmarkMatrixConfig(
        name=name,
        runs=tuple(runs),
        summary_csv=resolve_project_path(summary_csv),
    )


WATERMARK_SMOKE_CONFIG = build_watermark_matrix_config(
    name="watermark_smoke",
    delays_sec=(0,),
    source_factory=ReplaySourceFactory(batch_size=SMOKE_BATCH_SIZE, row_limit=SMOKE_ROW_LIMIT),
    rate=WATERMARK_SMOKE_RATE,
    summary_csv="artifacts/streaming/evaluation/watermark_smoke.csv",
    run_prefix="watermarkSmoke",
    metrics_timeout_sec=SMOKE_METRICS_TIMEOUT_SEC,
    metrics_idle_sec=SMOKE_METRICS_IDLE_SEC,
    stream_startup_wait_sec=SMOKE_STREAM_STARTUP_WAIT_SEC,
    stream_wait_timeout_sec=SMOKE_STREAM_WAIT_TIMEOUT_SEC,
)

def build_watermark_main_config() -> BenchmarkMatrixConfig:
    return build_watermark_matrix_config(
        name="watermark_500k",
        delays_sec=(0, 10, 30),
        source_factory=ReplaySourceFactory(batch_size=5_000, row_limit=125_000),
        rate=WATERMARK_MAIN_RATE,
        warmup_source_factory=ReplaySourceFactory(batch_size=5_000, row_limit=50_000),
        warmup_rate=WATERMARK_WARMUP_RATE,
        summary_csv="artifacts/streaming/evaluation/watermark_summary_500k.csv",
        run_prefix="watermark",
        metrics_timeout_sec=1_800,
    )


def build_watermark_late_injection_config() -> BenchmarkMatrixConfig:
    return build_watermark_matrix_config(
        name="watermark_late_injection",
        delays_sec=(0, 10, 30),
        source_factory=ReplaySourceFactory(batch_size=5_000, row_limit=125_000),
        rate=WATERMARK_MAIN_RATE,
        warmup_source_factory=ReplaySourceFactory(batch_size=5_000, row_limit=50_000),
        warmup_rate=WATERMARK_WARMUP_RATE,
        timing=WATERMARK_LATE_INJECTION_TIMING_CONFIG,
        summary_csv="artifacts/streaming/evaluation/watermark_summary_500k_reordered_late.csv",
        run_prefix="watermarkLate",
        metrics_timeout_sec=1_800,
    )

WATERMARK_CONFIG = WATERMARK_SMOKE_CONFIG
