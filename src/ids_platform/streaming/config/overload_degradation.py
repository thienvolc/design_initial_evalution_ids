from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ids_platform.common.paths import resolve_project_path
from ids_platform.streaming.config.calibration import CAPACITY_CALIBRATION_PROFILE
from ids_platform.streaming.config.common import (
    BenchmarkMatrixConfig,
    BenchmarkRunPlan,
    DEFAULT_TIMING_CONFIG,
    RuntimeProfile,
    PRIMARY_STREAMING_FEATURE_SET,
    PRIMARY_STREAMING_MODEL_NAME,
    SMOKE_BATCH_SIZE,
    SMOKE_COLLECTOR_IDLE_SEC,
    SMOKE_COLLECTOR_TIMEOUT_SEC,
    SMOKE_REPLAY_RATE,
    SMOKE_ROW_LIMIT,
    SMOKE_RPS,
    SMOKE_STREAM_STARTUP_WAIT_SEC,
    SMOKE_STREAM_WAIT_TIMEOUT_SEC,
    benchmark_run_tag,
    build_benchmark_run_plan,
)
from ids_platform.streaming.replay.config import RateStep, ReplayRatePlan, ReplaySourceFactory


@dataclass(frozen=True)
class OverloadProfile:
    label: str
    rate_steps: tuple[RateStep, ...]
    traffic_mode: str = "burst"

    @classmethod
    def constant(cls, label: str, target_rps: int, duration_sec: int, *, traffic_mode: str = "burst"):
        return cls(
            label=label,
            rate_steps=(RateStep(rows_per_sec=float(target_rps), duration_sec=float(duration_sec)),),
            traffic_mode=traffic_mode,
        )

    @property
    def expected_rows(self) -> int:
        rows = sum(float(step.rows_per_sec) * float(step.duration_sec) for step in self.rate_steps)
        return max(int(rows), 1)

    @property
    def duration_sec(self) -> int:
        return max(int(sum(float(step.duration_sec) for step in self.rate_steps)), 1)

    @property
    def target_rps(self) -> float:
        return float(self.expected_rows) / float(self.duration_sec)

    @property
    def peak_rps(self) -> float:
        return max((float(step.rows_per_sec) for step in self.rate_steps), default=0.0)

    @property
    def rate(self) -> ReplayRatePlan:
        return ReplayRatePlan(
            rows_per_sec=0.0,
            schedule=self.rate_steps,
            traffic_mode=self.traffic_mode,
        )


@dataclass(frozen=True)
class OverloadWorkloadPlan:
    model_name: str
    feature_set: str
    profiles: tuple[OverloadProfile, ...]


OVERLOAD_DEGRADATION_SMOKE_PROFILES = (
    OverloadProfile.constant("smoke_500rps_burst", SMOKE_RPS, 2),
)
OVERLOAD_DEGRADATION_SMOKE_RUNTIME_PROFILE = RuntimeProfile(
    "overload_degradation_smoke_local4_1s",
    500,
    CAPACITY_CALIBRATION_PROFILE.shuffle_partitions,
    CAPACITY_CALIBRATION_PROFILE.trigger_interval,
    CAPACITY_CALIBRATION_PROFILE.spark_master,
)

OVERLOAD_DEGRADATION_MAIN_PROFILES = (
    OverloadProfile.constant("operating_point_500rps_burst", 500, 20, traffic_mode="burst"),
    OverloadProfile.constant("pressure_point_600rps_burst", 600, 20, traffic_mode="burst"),
    OverloadProfile.constant("overload_750rps_burst", 750, 20, traffic_mode="burst"),
    OverloadProfile.constant("clear_overload_900rps_burst", 900, 20, traffic_mode="burst"),
)
OVERLOAD_DEGRADATION_MAIN_WORKLOAD_PLANS = (
    OverloadWorkloadPlan(
        PRIMARY_STREAMING_MODEL_NAME,
        PRIMARY_STREAMING_FEATURE_SET,
        OVERLOAD_DEGRADATION_MAIN_PROFILES,
    ),
)

OVERLOAD_WARMUP_RATE = ReplayRatePlan(
    rows_per_sec=0.0,
    schedule=(RateStep(rows_per_sec=500.0, duration_sec=10.0),),
)
OVERLOAD_WARMUP_ROWS = 5_000


def _summary_context(profile: OverloadProfile) -> dict:
    return {
        "overload_profile": profile.label,
        "target_rps": float(profile.target_rps),
        "peak_rps": float(profile.peak_rps),
        "traffic_mode": profile.traffic_mode,
        "duration_sec": int(profile.duration_sec),
        "expected_rows": int(profile.expected_rows),
    }


def build_overload_degradation_config(
    *,
    name: str,
    profiles: tuple[OverloadProfile, ...],
    summary_csv: str | Path,
    workload_plans: tuple[OverloadWorkloadPlan, ...] | None = None,
    repeats: int = 1,
    source_batch_size: int = 500,
    run_prefix: str = "overloadDegradation",
    model_name: str = PRIMARY_STREAMING_MODEL_NAME,
    feature_set: str = PRIMARY_STREAMING_FEATURE_SET,
    warmup_rows: int = OVERLOAD_WARMUP_ROWS,
    warmup_rate: ReplayRatePlan | None = OVERLOAD_WARMUP_RATE,
    collector_timeout_sec: int = 420,
    collector_idle_sec: int = 5,
    stream_startup_wait_sec: int = 10,
    stream_wait_timeout_sec: int = 240,
    runtime_profile: RuntimeProfile = CAPACITY_CALIBRATION_PROFILE,
) -> BenchmarkMatrixConfig:
    runs: list[BenchmarkRunPlan] = []
    resolved_workload_plans = workload_plans or (
        OverloadWorkloadPlan(model_name, feature_set, profiles),
    )
    for repeat_index in range(1, max(int(repeats), 1) + 1):
        run_index = 0
        for workload in resolved_workload_plans:
            for profile in workload.profiles:
                run_index += 1
                source = ReplaySourceFactory(
                    batch_size=source_batch_size,
                    row_limit=profile.expected_rows,
                ).plan()
                warmup_source = None
                if warmup_rows > 0 and warmup_rate is not None:
                    warmup_source = ReplaySourceFactory(
                        batch_size=source_batch_size,
                        row_limit=int(warmup_rows),
                    ).plan()
                label = f"{profile.label}_{workload.model_name}_{workload.feature_set}"
                run_tag = benchmark_run_tag(
                    prefix=run_prefix,
                    repeat_index=repeat_index,
                    run_index=run_index,
                    label=label,
                )
                summary_context = _summary_context(profile)
                summary_context.update(
                    {
                        "model_name": workload.model_name,
                        "feature_set": workload.feature_set,
                    }
                )
                runs.append(
                    build_benchmark_run_plan(
                        run_tag=run_tag,
                        repeat_index=repeat_index,
                        profile=runtime_profile,
                        source=source,
                        rate=profile.rate,
                        timing=DEFAULT_TIMING_CONFIG,
                        model_name=workload.model_name,
                        feature_set=workload.feature_set,
                        collector_timeout_sec=collector_timeout_sec,
                        collector_idle_sec=collector_idle_sec,
                        stream_startup_wait_sec=stream_startup_wait_sec,
                        stream_wait_timeout_sec=stream_wait_timeout_sec,
                        load_profile=label,
                        warmup_source=warmup_source,
                        warmup_rate=warmup_rate,
                        warmup_load_profile=f"{label}_warmup",
                        summary_context=summary_context,
                    )
                )
    return BenchmarkMatrixConfig(
        name=name,
        runs=tuple(runs),
        summary_csv=resolve_project_path(str(summary_csv)),
    )


def build_overload_degradation_smoke_config() -> BenchmarkMatrixConfig:
    return build_overload_degradation_config(
        name="overload_degradation_smoke",
        profiles=OVERLOAD_DEGRADATION_SMOKE_PROFILES,
        summary_csv="artifacts/streaming/evaluation/overload_degradation_smoke.csv",
        source_batch_size=SMOKE_BATCH_SIZE,
        warmup_rows=SMOKE_ROW_LIMIT,
        warmup_rate=SMOKE_REPLAY_RATE,
        collector_timeout_sec=SMOKE_COLLECTOR_TIMEOUT_SEC,
        collector_idle_sec=SMOKE_COLLECTOR_IDLE_SEC,
        stream_startup_wait_sec=SMOKE_STREAM_STARTUP_WAIT_SEC,
        stream_wait_timeout_sec=SMOKE_STREAM_WAIT_TIMEOUT_SEC,
        runtime_profile=OVERLOAD_DEGRADATION_SMOKE_RUNTIME_PROFILE,
    )


def build_overload_degradation_main_config() -> BenchmarkMatrixConfig:
    return build_overload_degradation_config(
        name="overload_degradation_main",
        profiles=OVERLOAD_DEGRADATION_MAIN_PROFILES,
        workload_plans=OVERLOAD_DEGRADATION_MAIN_WORKLOAD_PLANS,
        summary_csv="artifacts/streaming/evaluation/overload_degradation.csv",
        repeats=3,
        source_batch_size=500,
        warmup_rows=OVERLOAD_WARMUP_ROWS,
        warmup_rate=OVERLOAD_WARMUP_RATE,
        collector_timeout_sec=900,
        collector_idle_sec=20,
        stream_startup_wait_sec=10,
        stream_wait_timeout_sec=900,
    )

OVERLOAD_DEGRADATION_CONFIG = build_overload_degradation_smoke_config
