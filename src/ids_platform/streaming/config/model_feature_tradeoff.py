from __future__ import annotations

from ids_platform.common.paths import resolve_project_path
from ids_platform.streaming.config.calibration import CAPACITY_CALIBRATION_PROFILE
from ids_platform.streaming.config.common import (
    BenchmarkMatrixConfig,
    BenchmarkRunPlan,
    DEFAULT_TIMING_CONFIG,
    PRIMARY_STREAMING_FEATURE_SET,
    PRIMARY_STREAMING_MODEL_LABEL,
    PRIMARY_STREAMING_MODEL_NAME,
    SMOKE_BATCH_SIZE,
    SMOKE_COLLECTOR_IDLE_SEC,
    SMOKE_COLLECTOR_TIMEOUT_SEC,
    SMOKE_STREAM_STARTUP_WAIT_SEC,
    SMOKE_STREAM_WAIT_TIMEOUT_SEC,
    benchmark_run_tag,
    build_benchmark_run_plan,
)
from ids_platform.streaming.replay.config import RateStep, ReplayRatePlan, ReplaySourceFactory


RF17_MODEL_NAME = "random_forest"
RF17_FEATURE_SET = "reduced"
RF17_LABEL = "RF-17"
RF_FULL_LABEL = PRIMARY_STREAMING_MODEL_LABEL

TRADEOFF_TARGET_RPS = (500,)
TRADEOFF_DURATION_SEC = 60
TRADEOFF_WARMUP_DURATION_SEC = 20
TRADEOFF_REPEATS = 3
TRADEOFF_WORKLOADS = (
    (PRIMARY_STREAMING_MODEL_NAME, PRIMARY_STREAMING_FEATURE_SET, RF_FULL_LABEL),
    (RF17_MODEL_NAME, RF17_FEATURE_SET, RF17_LABEL),
)


def _rate_plan(target_rps: int, duration_sec: int) -> ReplayRatePlan:
    return ReplayRatePlan(
        rows_per_sec=0.0,
        schedule=(RateStep(rows_per_sec=float(target_rps), duration_sec=float(duration_sec)),),
    )


def _summary_context(*, target_rps: int, label: str) -> dict:
    return {
        "target_rps": float(target_rps),
        "candidate_label": label,
        "baseline_label": RF_FULL_LABEL,
        "baseline_summary_csv": "",
        "baseline_source": "same_matrix",
    }


def build_model_feature_tradeoff_config(
    *,
    name: str,
    target_rps_values: tuple[int, ...],
    duration_sec: int,
    summary_csv: str,
    repeats: int = 1,
    source_batch_size: int = 5_000,
    warmup_duration_sec: int = 0,
    run_prefix: str = "modelFeatureTradeoff",
    collector_timeout_sec: int = 180,
    collector_idle_sec: int = 5,
    stream_startup_wait_sec: int = 10,
    stream_wait_timeout_sec: int = 0,
    workloads: tuple[tuple[str, str, str], ...] = TRADEOFF_WORKLOADS,
) -> BenchmarkMatrixConfig:
    runs: list[BenchmarkRunPlan] = []

    for repeat_index in range(1, max(int(repeats), 1) + 1):
        run_index = 0
        for target_rps in target_rps_values:
            expected_rows = max(int(float(target_rps) * float(duration_sec)), 1)
            source = ReplaySourceFactory(
                batch_size=source_batch_size,
                row_limit=expected_rows,
            ).plan()
            warmup_source = None
            warmup_rate = None
            if int(warmup_duration_sec) > 0:
                warmup_rows = max(int(float(target_rps) * float(warmup_duration_sec)), 1)
                warmup_source = ReplaySourceFactory(
                    batch_size=source_batch_size,
                    row_limit=warmup_rows,
                ).plan()
                warmup_rate = _rate_plan(target_rps, warmup_duration_sec)
            rate = _rate_plan(target_rps, duration_sec)
            for model_name, feature_set, workload_label in workloads:
                run_index += 1
                label = f"{workload_label}_{target_rps}rps"
                run_tag = benchmark_run_tag(
                    prefix=run_prefix,
                    repeat_index=repeat_index,
                    run_index=run_index,
                    label=label,
                )
                runs.append(
                    build_benchmark_run_plan(
                        run_tag=run_tag,
                        repeat_index=repeat_index,
                        profile=CAPACITY_CALIBRATION_PROFILE,
                        source=source,
                        rate=rate,
                        timing=DEFAULT_TIMING_CONFIG,
                        model_name=model_name,
                        feature_set=feature_set,
                        collector_timeout_sec=collector_timeout_sec,
                        collector_idle_sec=collector_idle_sec,
                        stream_startup_wait_sec=stream_startup_wait_sec,
                        stream_wait_timeout_sec=stream_wait_timeout_sec,
                        load_profile=label,
                        warmup_source=warmup_source,
                        warmup_rate=warmup_rate,
                        warmup_load_profile=f"{label}_warmup",
                        summary_context=_summary_context(
                            target_rps=target_rps,
                            label=workload_label,
                        ),
                    )
                )

    return BenchmarkMatrixConfig(
        name=name,
        runs=tuple(runs),
        summary_csv=resolve_project_path(summary_csv),
    )


MODEL_FEATURE_TRADEOFF_SMOKE_CONFIG = build_model_feature_tradeoff_config(
    name="model_feature_tradeoff_smoke",
    target_rps_values=(500,),
    duration_sec=2,
    summary_csv="artifacts/streaming/evaluation/model_feature_tradeoff_smoke.csv",
    source_batch_size=SMOKE_BATCH_SIZE,
    warmup_duration_sec=2,
    collector_timeout_sec=SMOKE_COLLECTOR_TIMEOUT_SEC,
    collector_idle_sec=SMOKE_COLLECTOR_IDLE_SEC,
    stream_startup_wait_sec=SMOKE_STREAM_STARTUP_WAIT_SEC,
    stream_wait_timeout_sec=SMOKE_STREAM_WAIT_TIMEOUT_SEC,
)


def build_model_feature_tradeoff_main_config() -> BenchmarkMatrixConfig:
    return build_model_feature_tradeoff_config(
        name="model_feature_tradeoff_main",
        target_rps_values=TRADEOFF_TARGET_RPS,
        duration_sec=TRADEOFF_DURATION_SEC,
        summary_csv="artifacts/streaming/evaluation/model_feature_tradeoff.csv",
        repeats=TRADEOFF_REPEATS,
        source_batch_size=500,
        warmup_duration_sec=TRADEOFF_WARMUP_DURATION_SEC,
        collector_timeout_sec=420,
        collector_idle_sec=5,
        stream_startup_wait_sec=10,
        stream_wait_timeout_sec=240,
    )


MODEL_FEATURE_TRADEOFF_CONFIG = MODEL_FEATURE_TRADEOFF_SMOKE_CONFIG
