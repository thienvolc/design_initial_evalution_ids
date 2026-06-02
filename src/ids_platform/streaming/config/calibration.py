from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from ids_platform.common.paths import resolve_project_path
from ids_platform.streaming.config.common import (
    DEFAULT_TIMING_CONFIG,
    RuntimeProfile,
    SMOKE_BATCH_SIZE,
    SMOKE_METRICS_IDLE_SEC,
    SMOKE_METRICS_TIMEOUT_SEC,
    SMOKE_STREAM_STARTUP_WAIT_SEC,
    SMOKE_STREAM_WAIT_TIMEOUT_SEC,
    benchmark_run_tag,
    build_benchmark_run_plan,
)
from ids_platform.streaming.replay.config import RateStep, ReplayRatePlan, ReplaySourceFactory

if TYPE_CHECKING:
    from ids_platform.streaming.evaluation.matrices.throughput.benchmark_run import (
        BenchmarkRunConfig,
    )


@dataclass(frozen=True)
class CapacitySloConfig:
    min_throughput_ratio: float = 0.90
    min_rows_ratio: float = 0.95
    max_p50_e2e_ms: float = 500.0
    max_p95_e2e_ms: float = 1_500.0
    max_batch_wall_trigger_ratio: float = 0.80


@dataclass(frozen=True)
class CapacityCalibrationRun:
    benchmark: "BenchmarkRunConfig"
    target_rps: float
    expected_rows: int
    mode: str
    model_name: str
    feature_set: str


@dataclass(frozen=True)
class CapacityCalibrationProfilePlan:
    profile: RuntimeProfile
    target_rps_values: tuple[int, ...]


@dataclass(frozen=True)
class CapacityCalibrationConfig:
    name: str
    runs: tuple[CapacityCalibrationRun, ...]
    summary_csv: Path
    slo: CapacitySloConfig


CAPACITY_CALIBRATION_PROFILE = RuntimeProfile(
    "capacity_calibration_local4_1s",
    1_000,
    4,
    "1 second",
    "local[4]",
)
CAPACITY_CALIBRATION_TARGET_RPS = (500, 750, 1_000)
CAPACITY_CALIBRATION_DURATION_SEC = 20
CAPACITY_CALIBRATION_WARMUP_DURATION_SEC = 10
CAPACITY_CALIBRATION_BATCH_SIZE = 500
CAPACITY_CALIBRATION_PROFILE_PLANS = (
    CapacityCalibrationProfilePlan(CAPACITY_CALIBRATION_PROFILE, CAPACITY_CALIBRATION_TARGET_RPS),
)
CALIBRATION_MODES = (
    ("pass_through", "random_forest", "full"),
    ("model", "random_forest", "full"),
)
CAPACITY_CALIBRATION_PRACTICAL_SLO = CapacitySloConfig(
    max_p50_e2e_ms=2_000.0,
    max_p95_e2e_ms=2_000.0,
    max_batch_wall_trigger_ratio=1.0,
)


def _rate_plan(target_rps: float, duration_sec: float) -> ReplayRatePlan:
    return ReplayRatePlan(
        rows_per_sec=0.0,
        schedule=(RateStep(rows_per_sec=float(target_rps), duration_sec=float(duration_sec)),),
    )


def _mode_label(*, mode: str, model_name: str, feature_set: str) -> str:
    if mode == "pass_through":
        return "pass_through"
    return f"{model_name}_{feature_set}"


def build_capacity_calibration_config(
    *,
    name: str,
    profile_plans: tuple[CapacityCalibrationProfilePlan, ...],
    duration_sec: float,
    summary_csv: str,
    modes: tuple[tuple[str, str, str], ...] = CALIBRATION_MODES,
    source_batch_size: int = CAPACITY_CALIBRATION_BATCH_SIZE,
    warmup_duration_sec: float = 0.0,
    run_prefix: str = "capacityCalibration",
    repeats: int = 1,
    metrics_timeout_sec: int = 180,
    metrics_idle_sec: int = 5,
    stream_startup_wait_sec: int = 10,
    stream_wait_timeout_sec: int = 0,
    slo: CapacitySloConfig = CapacitySloConfig(),
) -> CapacityCalibrationConfig:
    created_ms = 0
    runs: list[CapacityCalibrationRun] = []

    for repeat_index in range(1, max(int(repeats), 1) + 1):
        run_index = 0
        for profile_plan in profile_plans:
            profile = profile_plan.profile
            for target_rps in profile_plan.target_rps_values:
                expected_rows = max(int(float(target_rps) * float(duration_sec)), 1)
                source = ReplaySourceFactory(
                    batch_size=source_batch_size,
                    row_limit=expected_rows,
                ).create()
                rate = _rate_plan(float(target_rps), float(duration_sec))
                warmup_source = None
                warmup_rate = None
                if float(warmup_duration_sec) > 0:
                    warmup_rows = max(int(float(target_rps) * float(warmup_duration_sec)), 1)
                    warmup_source = ReplaySourceFactory(
                        batch_size=source_batch_size,
                        row_limit=warmup_rows,
                    ).create()
                    warmup_rate = _rate_plan(float(target_rps), float(warmup_duration_sec))

                for mode, model_name, feature_set in modes:
                    run_index += 1
                    label = (
                        f"{profile.name}_"
                        f"{_mode_label(mode=mode, model_name=model_name, feature_set=feature_set)}_"
                        f"{target_rps}rps"
                    )
                    run_tag = benchmark_run_tag(
                        prefix=run_prefix,
                        repeat_index=repeat_index,
                        run_index=run_index,
                        label=label,
                        created_ms=created_ms,
                    )
                    run_plan = build_benchmark_run_plan(
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
                        stream_wait_timeout_sec=stream_wait_timeout_sec,
                        load_profile=label,
                        warmup_source=warmup_source,
                        warmup_rate=warmup_rate,
                        warmup_load_profile=f"{label}_warmup",
                    )
                    runs.append(
                        CapacityCalibrationRun(
                            benchmark=run_plan.benchmark,
                            target_rps=float(target_rps),
                            expected_rows=source.table.num_rows,
                            mode=_mode_label(mode=mode, model_name=model_name, feature_set=feature_set),
                            model_name=model_name,
                            feature_set=feature_set,
                        )
                    )

    return CapacityCalibrationConfig(
        name=name,
        runs=tuple(runs),
        summary_csv=resolve_project_path(summary_csv),
        slo=slo,
    )


CAPACITY_CALIBRATION_SMOKE_CONFIG = build_capacity_calibration_config(
    name="capacity_calibration_smoke",
    profile_plans=(
        CapacityCalibrationProfilePlan(CAPACITY_CALIBRATION_PROFILE, (500,)),
    ),
    duration_sec=2,
    summary_csv="artifacts/streaming/evaluation/capacity_calibration_smoke.csv",
    modes=(("pass_through", "random_forest", "full"),),
    source_batch_size=SMOKE_BATCH_SIZE,
    warmup_duration_sec=2,
    metrics_timeout_sec=SMOKE_METRICS_TIMEOUT_SEC,
    metrics_idle_sec=SMOKE_METRICS_IDLE_SEC,
    stream_startup_wait_sec=SMOKE_STREAM_STARTUP_WAIT_SEC,
    stream_wait_timeout_sec=SMOKE_STREAM_WAIT_TIMEOUT_SEC,
    slo=CAPACITY_CALIBRATION_PRACTICAL_SLO,
)


def build_capacity_calibration_main_config() -> CapacityCalibrationConfig:
    return build_capacity_calibration_config(
        name="capacity_calibration_main",
        profile_plans=CAPACITY_CALIBRATION_PROFILE_PLANS,
        duration_sec=CAPACITY_CALIBRATION_DURATION_SEC,
        summary_csv="artifacts/streaming/evaluation/capacity_calibration.csv",
        modes=CALIBRATION_MODES,
        source_batch_size=CAPACITY_CALIBRATION_BATCH_SIZE,
        warmup_duration_sec=CAPACITY_CALIBRATION_WARMUP_DURATION_SEC,
        repeats=3,
        metrics_timeout_sec=420,
        metrics_idle_sec=5,
        stream_startup_wait_sec=10,
        stream_wait_timeout_sec=240,
        slo=CAPACITY_CALIBRATION_PRACTICAL_SLO,
    )


CAPACITY_CALIBRATION_CONFIG = CAPACITY_CALIBRATION_SMOKE_CONFIG
