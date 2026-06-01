from __future__ import annotations

import time
from dataclasses import dataclass

from ids_platform.common.paths import resolve_project_path
from ids_platform.streaming.config.common import (
    DEFAULT_TIMING_CONFIG,
    RuntimeProfile,
    SMOKE_BATCH_SIZE,
    SMOKE_METRICS_IDLE_SEC,
    SMOKE_METRICS_TIMEOUT_SEC,
    SMOKE_REPLAY_RATE,
    SMOKE_ROW_LIMIT,
    SMOKE_STREAM_STARTUP_WAIT_SEC,
    SMOKE_STREAM_WAIT_TIMEOUT_SEC,
    build_benchmark_run_config,
    stream_wait_timeout,
)
from ids_platform.streaming.replay.config import RateStep, ReplayConfig, ReplayRatePlan, ReplaySourceFactory
from ids_platform.streaming.runtime.config import RuntimeConfig


@dataclass(frozen=True)
class LayerCScenarioConfig:
    scenario: str
    run_tag: str
    runtime: RuntimeConfig
    warmup_replay: ReplayConfig
    post_fault_replay: ReplayConfig
    metrics_timeout_sec: int
    metrics_idle_sec: int
    stream_startup_wait_sec: int
    stream_wait_timeout_sec: int
    producer_restart_pause_sec: int = 0


@dataclass(frozen=True)
class LayerCMatrixConfig:
    name: str
    scenarios: tuple[LayerCScenarioConfig, ...]
    summary_csv: object


LAYER_C_SMOKE_RATE = SMOKE_REPLAY_RATE

LAYER_C_MAIN_WARMUP_RATE = ReplayRatePlan(
    rows_per_sec=0.0,
    schedule=(RateStep(rows_per_sec=5_000, duration_sec=24),),
)

LAYER_C_MAIN_POST_FAULT_RATE = ReplayRatePlan(rows_per_sec=10_000.0, schedule=())


def _scenario_config(
    *,
    scenario: str,
    run_index: int,
    run_prefix: str,
    model_name: str,
    feature_set: str,
    warmup_rows: int,
    warmup_rate: ReplayRatePlan,
    post_fault_rows: int,
    post_fault_rate: ReplayRatePlan,
    batch_size: int,
    metrics_timeout_sec: int,
    metrics_idle_sec: int,
    stream_startup_wait_sec: int,
    stream_wait_timeout_sec: int,
    producer_restart_pause_sec: int,
    created_ms: int,
) -> LayerCScenarioConfig:
    run_tag = f"{run_prefix}_{run_index:02d}_{scenario}_{created_ms}"
    profile = RuntimeProfile(scenario, 20_000, 8, "10 seconds")
    warmup_source = ReplaySourceFactory(batch_size=batch_size, row_limit=warmup_rows).create()
    post_fault_source = ReplaySourceFactory(batch_size=batch_size, row_limit=post_fault_rows).create()
    warmup = build_benchmark_run_config(
        run_tag=run_tag,
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
            override_seconds=stream_wait_timeout_sec,
        ),
        load_profile=f"{scenario}_warmup",
    )
    post_fault = build_benchmark_run_config(
        run_tag=run_tag,
        repeat_index=1,
        profile=profile,
        source=post_fault_source,
        rate=post_fault_rate,
        timing=DEFAULT_TIMING_CONFIG,
        model_name=model_name,
        feature_set=feature_set,
        metrics_timeout_sec=metrics_timeout_sec,
        metrics_idle_sec=metrics_idle_sec,
        stream_startup_wait_sec=stream_startup_wait_sec,
        stream_wait_timeout_sec=stream_wait_timeout(
            row_count=post_fault_source.table.num_rows,
            rate=post_fault_rate,
            override_seconds=stream_wait_timeout_sec,
        ),
        load_profile=scenario,
    )
    return LayerCScenarioConfig(
        scenario=scenario,
        run_tag=run_tag,
        runtime=warmup.runtime,
        warmup_replay=warmup.replay,
        post_fault_replay=post_fault.replay,
        metrics_timeout_sec=metrics_timeout_sec,
        metrics_idle_sec=metrics_idle_sec,
        stream_startup_wait_sec=stream_startup_wait_sec,
        stream_wait_timeout_sec=post_fault.stream_wait_timeout_sec,
        producer_restart_pause_sec=producer_restart_pause_sec,
    )


def build_layer_c_matrix_config(
    *,
    name: str,
    scenarios: tuple[str, ...],
    summary_csv: str,
    model_name: str = "random_forest",
    feature_set: str = "full",
    warmup_rows: int,
    warmup_rate: ReplayRatePlan,
    post_fault_rows: int,
    post_fault_rate: ReplayRatePlan,
    batch_size: int,
    metrics_timeout_sec: int,
    metrics_idle_sec: int = 5,
    stream_startup_wait_sec: int = 10,
    stream_wait_timeout_sec: int = 0,
    producer_restart_pause_sec: int = 0,
    run_prefix: str = "layerC",
) -> LayerCMatrixConfig:
    created_ms = int(time.time() * 1000)
    return LayerCMatrixConfig(
        name=name,
        scenarios=tuple(
            _scenario_config(
                scenario=scenario,
                run_index=index,
                run_prefix=run_prefix,
                model_name=model_name,
                feature_set=feature_set,
                warmup_rows=warmup_rows,
                warmup_rate=warmup_rate,
                post_fault_rows=post_fault_rows,
                post_fault_rate=post_fault_rate,
                batch_size=batch_size,
                metrics_timeout_sec=metrics_timeout_sec,
                metrics_idle_sec=metrics_idle_sec,
                stream_startup_wait_sec=stream_startup_wait_sec,
                stream_wait_timeout_sec=stream_wait_timeout_sec,
                producer_restart_pause_sec=producer_restart_pause_sec,
                created_ms=created_ms,
            )
            for index, scenario in enumerate(scenarios, start=1)
        ),
        summary_csv=resolve_project_path(summary_csv),
    )


LAYER_C_SMOKE_CONFIG = build_layer_c_matrix_config(
    name="layer_c_smoke",
    scenarios=("producer_restart",),
    warmup_rows=SMOKE_ROW_LIMIT,
    warmup_rate=LAYER_C_SMOKE_RATE,
    post_fault_rows=SMOKE_ROW_LIMIT,
    post_fault_rate=LAYER_C_SMOKE_RATE,
    batch_size=SMOKE_BATCH_SIZE,
    producer_restart_pause_sec=1,
    metrics_timeout_sec=SMOKE_METRICS_TIMEOUT_SEC,
    metrics_idle_sec=SMOKE_METRICS_IDLE_SEC,
    stream_startup_wait_sec=SMOKE_STREAM_STARTUP_WAIT_SEC,
    stream_wait_timeout_sec=SMOKE_STREAM_WAIT_TIMEOUT_SEC,
    summary_csv="artifacts/streaming/evaluation/layer_c_smoke.csv",
)


def build_layer_c_main_config() -> LayerCMatrixConfig:
    return build_layer_c_matrix_config(
        name="layer_c_700k_fault",
        scenarios=("spark_process_restart", "producer_restart", "network_slowdown"),
        warmup_rows=120_000,
        warmup_rate=LAYER_C_MAIN_WARMUP_RATE,
        post_fault_rows=180_000,
        post_fault_rate=LAYER_C_MAIN_POST_FAULT_RATE,
        batch_size=5_000,
        producer_restart_pause_sec=5,
        metrics_timeout_sec=1_200,
        stream_startup_wait_sec=90,
        stream_wait_timeout_sec=1_200,
        summary_csv="artifacts/streaming/evaluation/layer_c_summary_700k_fault.csv",
    )


LAYER_C_CONFIG = LAYER_C_SMOKE_CONFIG
