from __future__ import annotations

from ids_platform.common.paths import resolve_project_path
from ids_platform.streaming.config.calibration import CAPACITY_CALIBRATION_PROFILE
from ids_platform.streaming.config.common import (
    BenchmarkMatrixConfig,
    BenchmarkRunPlan,
    DEFAULT_TIMING_CONFIG,
    SMOKE_BATCH_SIZE,
    SMOKE_METRICS_IDLE_SEC,
    SMOKE_METRICS_TIMEOUT_SEC,
    SMOKE_STREAM_STARTUP_WAIT_SEC,
    SMOKE_STREAM_WAIT_TIMEOUT_SEC,
    benchmark_run_tag,
    build_benchmark_run_plan,
)
from ids_platform.streaming.replay.config import RateStep, ReplayRatePlan, ReplaySourceFactory


RF17_MODEL_NAME = "random_forest"
RF17_FEATURE_SET = "reduced"
RF17_LABEL = "RF-17"
RF_FULL_BASELINE_SUMMARY_CSV = "artifacts/streaming/evaluation/capacity_calibration.csv"

TRADEOFF_TARGET_RPS = (500, 750)
TRADEOFF_DURATION_SEC = 20
TRADEOFF_WARMUP_DURATION_SEC = 10
TRADEOFF_REPEATS = 3


def _rate_plan(target_rps: int, duration_sec: int) -> ReplayRatePlan:
    return ReplayRatePlan(
        rows_per_sec=0.0,
        schedule=(RateStep(rows_per_sec=float(target_rps), duration_sec=float(duration_sec)),),
    )


def _summary_context(target_rps: int) -> dict:
    return {
        "target_rps": float(target_rps),
        "candidate_label": RF17_LABEL,
        "baseline_label": "RF-Full",
        "baseline_summary_csv": RF_FULL_BASELINE_SUMMARY_CSV,
        "baseline_source": "capacity_calibration",
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
    metrics_timeout_sec: int = 180,
    metrics_idle_sec: int = 5,
    stream_startup_wait_sec: int = 10,
    stream_wait_timeout_sec: int = 0,
) -> BenchmarkMatrixConfig:
    runs: list[BenchmarkRunPlan] = []

    for repeat_index in range(1, max(int(repeats), 1) + 1):
        for run_index, target_rps in enumerate(target_rps_values, start=1):
            expected_rows = max(int(float(target_rps) * float(duration_sec)), 1)
            source = ReplaySourceFactory(
                batch_size=source_batch_size,
                row_limit=expected_rows,
            ).create()
            warmup_source = None
            warmup_rate = None
            if int(warmup_duration_sec) > 0:
                warmup_rows = max(int(float(target_rps) * float(warmup_duration_sec)), 1)
                warmup_source = ReplaySourceFactory(
                    batch_size=source_batch_size,
                    row_limit=warmup_rows,
                ).create()
                warmup_rate = _rate_plan(target_rps, warmup_duration_sec)
            rate = _rate_plan(target_rps, duration_sec)
            label = f"{RF17_LABEL}_{target_rps}rps"
            run_tag = benchmark_run_tag(
                prefix=run_prefix,
                repeat_index=repeat_index,
                run_index=run_index,
                label=label,
                created_ms=0,
            )
            runs.append(
                build_benchmark_run_plan(
                    run_tag=run_tag,
                    repeat_index=repeat_index,
                    profile=CAPACITY_CALIBRATION_PROFILE,
                    source=source,
                    rate=rate,
                    timing=DEFAULT_TIMING_CONFIG,
                    model_name=RF17_MODEL_NAME,
                    feature_set=RF17_FEATURE_SET,
                    metrics_timeout_sec=metrics_timeout_sec,
                    metrics_idle_sec=metrics_idle_sec,
                    stream_startup_wait_sec=stream_startup_wait_sec,
                    stream_wait_timeout_sec=stream_wait_timeout_sec,
                    load_profile=label,
                    warmup_source=warmup_source,
                    warmup_rate=warmup_rate,
                    warmup_load_profile=f"{label}_warmup",
                    summary_context=_summary_context(target_rps),
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
    metrics_timeout_sec=SMOKE_METRICS_TIMEOUT_SEC,
    metrics_idle_sec=SMOKE_METRICS_IDLE_SEC,
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
        metrics_timeout_sec=420,
        metrics_idle_sec=5,
        stream_startup_wait_sec=10,
        stream_wait_timeout_sec=240,
    )


MODEL_FEATURE_TRADEOFF_CONFIG = MODEL_FEATURE_TRADEOFF_SMOKE_CONFIG
