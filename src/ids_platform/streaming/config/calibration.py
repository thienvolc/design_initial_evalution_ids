from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from ids_platform.common.paths import resolve_project_path
from ids_platform.streaming.config.common import (
    DEFAULT_TIMING_CONFIG,
    PRIMARY_STREAMING_FEATURE_SET,
    PRIMARY_STREAMING_MODEL_NAME,
    RuntimeProfile,
    SMOKE_BATCH_SIZE,
    SMOKE_COLLECTOR_IDLE_SEC,
    SMOKE_COLLECTOR_TIMEOUT_SEC,
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
    topic_partitions: int = 1


@dataclass(frozen=True)
class CapacityCalibrationWorkloadPlan:
    mode: str
    model_name: str
    feature_set: str
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
CAPACITY_THRESHOLD_PROFILE = RuntimeProfile(
    "capacity_threshold_balanced_1s_1000offsets",
    1_000,
    4,
    "1 second",
    "local[4]",
)
CAPACITY_CALIBRATION_TARGET_RPS = (100, 300, 500, 600)
CAPACITY_CALIBRATION_DURATION_SEC = 60
CAPACITY_CALIBRATION_WARMUP_DURATION_SEC = 20
CAPACITY_CALIBRATION_BATCH_SIZE = 500
CAPACITY_LOAD_THRESHOLD_PROFILE_PLANS = (
    CapacityCalibrationProfilePlan(CAPACITY_THRESHOLD_PROFILE, CAPACITY_CALIBRATION_TARGET_RPS),
)
RUNTIME_SENSITIVITY_TARGET_RPS = (500,)
RUNTIME_SENSITIVITY_PROFILE_PLANS = (
    CapacityCalibrationProfilePlan(
        RuntimeProfile(
            "runtime_sensitivity_low_latency_500ms_500offsets",
            500,
            4,
            "500 milliseconds",
            "local[4]",
        ),
        RUNTIME_SENSITIVITY_TARGET_RPS,
    ),
    CapacityCalibrationProfilePlan(
        RuntimeProfile(
            "runtime_sensitivity_balanced_1s_1000offsets",
            1_000,
            4,
            "1 second",
            "local[4]",
        ),
        RUNTIME_SENSITIVITY_TARGET_RPS,
    ),
    CapacityCalibrationProfilePlan(
        RuntimeProfile(
            "runtime_sensitivity_batch_2s_2000offsets",
            2_000,
            4,
            "2 seconds",
            "local[4]",
        ),
        RUNTIME_SENSITIVITY_TARGET_RPS,
    ),
)
RUNTIME_SENSITIVITY_SCREENING_TARGET_RPS = (300, 500)
RUNTIME_SENSITIVITY_SCREENING_PROFILE_PLANS = (
    CapacityCalibrationProfilePlan(
        RuntimeProfile(
            "runtime_sensitivity_latency_500ms_500offsets",
            500,
            4,
            "500 milliseconds",
            "local[4]",
        ),
        RUNTIME_SENSITIVITY_SCREENING_TARGET_RPS,
    ),
    CapacityCalibrationProfilePlan(
        RuntimeProfile(
            "runtime_sensitivity_latency_high_500ms_1000offsets",
            1_000,
            4,
            "500 milliseconds",
            "local[4]",
        ),
        RUNTIME_SENSITIVITY_SCREENING_TARGET_RPS,
    ),
    CapacityCalibrationProfilePlan(
        RuntimeProfile(
            "runtime_sensitivity_balanced_1s_1000offsets",
            1_000,
            4,
            "1 second",
            "local[4]",
        ),
        RUNTIME_SENSITIVITY_SCREENING_TARGET_RPS,
    ),
    CapacityCalibrationProfilePlan(
        RuntimeProfile(
            "runtime_sensitivity_balanced_high_1s_2000offsets",
            2_000,
            4,
            "1 second",
            "local[4]",
        ),
        RUNTIME_SENSITIVITY_SCREENING_TARGET_RPS,
    ),
)
CALIBRATION_MODES = (
    ("pass_through", PRIMARY_STREAMING_MODEL_NAME, PRIMARY_STREAMING_FEATURE_SET),
    ("model", PRIMARY_STREAMING_MODEL_NAME, PRIMARY_STREAMING_FEATURE_SET),
)
MODEL_ONLY_CALIBRATION_MODES = (
    ("model", PRIMARY_STREAMING_MODEL_NAME, PRIMARY_STREAMING_FEATURE_SET),
)
RUNTIME_SENSITIVITY_MODES = (
    ("pass_through", PRIMARY_STREAMING_MODEL_NAME, PRIMARY_STREAMING_FEATURE_SET),
    ("model", PRIMARY_STREAMING_MODEL_NAME, PRIMARY_STREAMING_FEATURE_SET),
    ("model", PRIMARY_STREAMING_MODEL_NAME, "reduced"),
)
RUNTIME_SENSITIVITY_SCREENING_MODES = (
    ("pass_through", PRIMARY_STREAMING_MODEL_NAME, PRIMARY_STREAMING_FEATURE_SET),
    ("model", PRIMARY_STREAMING_MODEL_NAME, PRIMARY_STREAMING_FEATURE_SET),
)
RUNTIME_SENSITIVITY_CANDIDATE_PROFILE_PLANS = (
    CapacityCalibrationProfilePlan(
        RuntimeProfile(
            "runtime_candidate_latency_500ms_500offsets",
            500,
            4,
            "500 milliseconds",
            "local[4]",
        ),
        (300,),
    ),
    CapacityCalibrationProfilePlan(
        RuntimeProfile(
            "runtime_candidate_balanced_1s_1000offsets",
            1_000,
            4,
            "1 second",
            "local[4]",
        ),
        (500,),
    ),
)
KAFKA_PARTITION_SENSITIVITY_PARTITIONS = (1, 2, 4)
KAFKA_PARTITION_SENSITIVITY_MODES = (
    ("pass_through", PRIMARY_STREAMING_MODEL_NAME, PRIMARY_STREAMING_FEATURE_SET),
    ("model", PRIMARY_STREAMING_MODEL_NAME, PRIMARY_STREAMING_FEATURE_SET),
)
CAPACITY_THRESHOLD_WORKLOAD_PLANS = (
    CapacityCalibrationWorkloadPlan(
        "pass_through",
        PRIMARY_STREAMING_MODEL_NAME,
        PRIMARY_STREAMING_FEATURE_SET,
        (500, 600, 750),
    ),
    CapacityCalibrationWorkloadPlan(
        "model",
        PRIMARY_STREAMING_MODEL_NAME,
        PRIMARY_STREAMING_FEATURE_SET,
        (300, 500, 600),
    ),
)
CAPACITY_CALIBRATION_PRACTICAL_SLO = CapacitySloConfig(
    max_p50_e2e_ms=2_000.0,
    max_p95_e2e_ms=2_000.0,
)
CAPACITY_CALIBRATION_SMOKE_SLO = CapacitySloConfig(
    min_throughput_ratio=0.50,
    min_rows_ratio=1.0,
    max_p50_e2e_ms=2_000.0,
    max_p95_e2e_ms=3_000.0,
)


def _rate_plan(target_rps: float, duration_sec: float, *, traffic_mode: str = "ticked") -> ReplayRatePlan:
    return ReplayRatePlan(
        rows_per_sec=0.0,
        schedule=(RateStep(rows_per_sec=float(target_rps), duration_sec=float(duration_sec)),),
        traffic_mode=traffic_mode,
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
    collector_timeout_sec: int = 180,
    collector_idle_sec: int = 5,
    stream_startup_wait_sec: int = 10,
    stream_wait_timeout_sec: int = 0,
    slo: CapacitySloConfig = CapacitySloConfig(),
) -> CapacityCalibrationConfig:
    runs: list[CapacityCalibrationRun] = []

    for repeat_index in range(1, max(int(repeats), 1) + 1):
        run_index = 0
        for profile_plan in profile_plans:
            profile = profile_plan.profile
            input_partitions = max(int(profile_plan.topic_partitions), 1)
            for target_rps in profile_plan.target_rps_values:
                expected_rows = max(int(float(target_rps) * float(duration_sec)), 1)
                source = ReplaySourceFactory(
                    batch_size=source_batch_size,
                    row_limit=expected_rows,
                ).plan()
                rate = _rate_plan(float(target_rps), float(duration_sec))
                warmup_source = None
                warmup_rate = None
                if float(warmup_duration_sec) > 0:
                    warmup_rows = max(int(float(target_rps) * float(warmup_duration_sec)), 1)
                    warmup_source = ReplaySourceFactory(
                        batch_size=source_batch_size,
                        row_limit=warmup_rows,
                    ).plan()
                    warmup_rate = _rate_plan(float(target_rps), float(warmup_duration_sec))

                for mode, model_name, feature_set in modes:
                    run_index += 1
                    label = (
                        f"{profile.name}_"
                        f"{input_partitions}partition_"
                        f"{_mode_label(mode=mode, model_name=model_name, feature_set=feature_set)}_"
                        f"{target_rps}rps"
                    )
                    run_tag = benchmark_run_tag(
                        prefix=run_prefix,
                        repeat_index=repeat_index,
                        run_index=run_index,
                        label=label,
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
                        collector_timeout_sec=collector_timeout_sec,
                        collector_idle_sec=collector_idle_sec,
                        stream_startup_wait_sec=stream_startup_wait_sec,
                        stream_wait_timeout_sec=stream_wait_timeout_sec,
                        topic_partitions=input_partitions,
                        load_profile=label,
                        warmup_source=warmup_source,
                        warmup_rate=warmup_rate,
                        warmup_load_profile=f"{label}_warmup",
                    )
                    runs.append(
                        CapacityCalibrationRun(
                            benchmark=run_plan.benchmark,
                            target_rps=float(target_rps),
                            expected_rows=source.expected_rows,
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


def build_capacity_workload_calibration_config(
    *,
    name: str,
    profile: RuntimeProfile,
    workload_plans: tuple[CapacityCalibrationWorkloadPlan, ...],
    duration_sec: float,
    summary_csv: str,
    source_batch_size: int = CAPACITY_CALIBRATION_BATCH_SIZE,
    topic_partitions: int = 1,
    warmup_duration_sec: float = 0.0,
    run_prefix: str = "capacityCalibration",
    repeats: int = 1,
    collector_timeout_sec: int = 180,
    collector_idle_sec: int = 5,
    stream_startup_wait_sec: int = 10,
    stream_wait_timeout_sec: int = 0,
    slo: CapacitySloConfig = CapacitySloConfig(),
) -> CapacityCalibrationConfig:
    runs: list[CapacityCalibrationRun] = []

    for repeat_index in range(1, max(int(repeats), 1) + 1):
        run_index = 0
        for workload in workload_plans:
            input_partitions = max(int(topic_partitions), 1)
            mode_label = _mode_label(
                mode=workload.mode,
                model_name=workload.model_name,
                feature_set=workload.feature_set,
            )
            for target_rps in workload.target_rps_values:
                run_index += 1
                expected_rows = max(int(float(target_rps) * float(duration_sec)), 1)
                source = ReplaySourceFactory(
                    batch_size=source_batch_size,
                    row_limit=expected_rows,
                ).plan()
                rate = _rate_plan(float(target_rps), float(duration_sec))
                warmup_source = None
                warmup_rate = None
                if float(warmup_duration_sec) > 0:
                    warmup_rows = max(int(float(target_rps) * float(warmup_duration_sec)), 1)
                    warmup_source = ReplaySourceFactory(
                        batch_size=source_batch_size,
                        row_limit=warmup_rows,
                    ).plan()
                    warmup_rate = _rate_plan(float(target_rps), float(warmup_duration_sec))

                label = f"{profile.name}_{input_partitions}partition_{mode_label}_{target_rps}rps"
                run_tag = benchmark_run_tag(
                    prefix=run_prefix,
                    repeat_index=repeat_index,
                    run_index=run_index,
                    label=label,
                )
                run_plan = build_benchmark_run_plan(
                    run_tag=run_tag,
                    repeat_index=repeat_index,
                    profile=profile,
                    source=source,
                    rate=rate,
                    timing=DEFAULT_TIMING_CONFIG,
                    model_name=workload.model_name,
                    feature_set=workload.feature_set,
                    mode=workload.mode,
                    collector_timeout_sec=collector_timeout_sec,
                    collector_idle_sec=collector_idle_sec,
                    stream_startup_wait_sec=stream_startup_wait_sec,
                    stream_wait_timeout_sec=stream_wait_timeout_sec,
                    topic_partitions=input_partitions,
                    load_profile=label,
                    warmup_source=warmup_source,
                    warmup_rate=warmup_rate,
                    warmup_load_profile=f"{label}_warmup",
                )
                runs.append(
                    CapacityCalibrationRun(
                        benchmark=run_plan.benchmark,
                        target_rps=float(target_rps),
                        expected_rows=source.expected_rows,
                        mode=mode_label,
                        model_name=workload.model_name,
                        feature_set=workload.feature_set,
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
    modes=(("pass_through", PRIMARY_STREAMING_MODEL_NAME, PRIMARY_STREAMING_FEATURE_SET),),
    source_batch_size=SMOKE_BATCH_SIZE,
    warmup_duration_sec=2,
    collector_timeout_sec=SMOKE_COLLECTOR_TIMEOUT_SEC,
    collector_idle_sec=SMOKE_COLLECTOR_IDLE_SEC,
    stream_startup_wait_sec=SMOKE_STREAM_STARTUP_WAIT_SEC,
    stream_wait_timeout_sec=SMOKE_STREAM_WAIT_TIMEOUT_SEC,
    slo=CAPACITY_CALIBRATION_SMOKE_SLO,
)


def build_capacity_load_threshold_config(
    *,
    repeats: int = 1,
    summary_csv: str = "artifacts/streaming/evaluation/capacity_load_threshold.csv",
) -> CapacityCalibrationConfig:
    return build_capacity_workload_calibration_config(
        name="capacity_load_threshold",
        profile=CAPACITY_THRESHOLD_PROFILE,
        workload_plans=CAPACITY_THRESHOLD_WORKLOAD_PLANS,
        duration_sec=CAPACITY_CALIBRATION_DURATION_SEC,
        summary_csv=summary_csv,
        source_batch_size=CAPACITY_CALIBRATION_BATCH_SIZE,
        warmup_duration_sec=CAPACITY_CALIBRATION_WARMUP_DURATION_SEC,
        repeats=repeats,
        collector_timeout_sec=420,
        collector_idle_sec=5,
        stream_startup_wait_sec=10,
        stream_wait_timeout_sec=240,
        slo=CAPACITY_CALIBRATION_PRACTICAL_SLO,
    )


def build_capacity_runtime_sensitivity_config(
    *,
    repeats: int = 1,
    summary_csv: str = "artifacts/streaming/evaluation/capacity_runtime_sensitivity.csv",
) -> CapacityCalibrationConfig:
    return build_capacity_calibration_config(
        name="capacity_runtime_sensitivity",
        profile_plans=RUNTIME_SENSITIVITY_PROFILE_PLANS,
        duration_sec=CAPACITY_CALIBRATION_DURATION_SEC,
        summary_csv=summary_csv,
        modes=RUNTIME_SENSITIVITY_MODES,
        source_batch_size=CAPACITY_CALIBRATION_BATCH_SIZE,
        warmup_duration_sec=CAPACITY_CALIBRATION_WARMUP_DURATION_SEC,
        repeats=repeats,
        collector_timeout_sec=420,
        collector_idle_sec=5,
        stream_startup_wait_sec=10,
        stream_wait_timeout_sec=240,
        slo=CAPACITY_CALIBRATION_PRACTICAL_SLO,
    )


def build_capacity_runtime_sensitivity_screening_config(
    *,
    summary_csv: str = "artifacts/streaming/evaluation/capacity_runtime_sensitivity_screening.csv",
) -> CapacityCalibrationConfig:
    return build_capacity_calibration_config(
        name="capacity_runtime_sensitivity_screening",
        profile_plans=RUNTIME_SENSITIVITY_SCREENING_PROFILE_PLANS,
        duration_sec=CAPACITY_CALIBRATION_DURATION_SEC,
        summary_csv=summary_csv,
        modes=RUNTIME_SENSITIVITY_SCREENING_MODES,
        source_batch_size=CAPACITY_CALIBRATION_BATCH_SIZE,
        warmup_duration_sec=CAPACITY_CALIBRATION_WARMUP_DURATION_SEC,
        repeats=1,
        collector_timeout_sec=420,
        collector_idle_sec=5,
        stream_startup_wait_sec=10,
        stream_wait_timeout_sec=240,
        slo=CAPACITY_CALIBRATION_PRACTICAL_SLO,
    )


def build_capacity_runtime_sensitivity_candidate_confirmation_config(
    *,
    repeats: int = 3,
    summary_csv: str = (
        "artifacts/streaming/evaluation/"
        "capacity_runtime_sensitivity_candidate_confirmation.csv"
    ),
) -> CapacityCalibrationConfig:
    return build_capacity_calibration_config(
        name="capacity_runtime_sensitivity_candidate_confirmation",
        profile_plans=RUNTIME_SENSITIVITY_CANDIDATE_PROFILE_PLANS,
        duration_sec=CAPACITY_CALIBRATION_DURATION_SEC,
        summary_csv=summary_csv,
        modes=RUNTIME_SENSITIVITY_SCREENING_MODES,
        source_batch_size=CAPACITY_CALIBRATION_BATCH_SIZE,
        warmup_duration_sec=CAPACITY_CALIBRATION_WARMUP_DURATION_SEC,
        repeats=repeats,
        collector_timeout_sec=420,
        collector_idle_sec=5,
        stream_startup_wait_sec=10,
        stream_wait_timeout_sec=240,
        slo=CAPACITY_CALIBRATION_PRACTICAL_SLO,
    )


def build_capacity_runtime_sensitivity_300_fresh_config(
    *,
    summary_csv: str = "artifacts/streaming/evaluation/capacity_runtime_sensitivity_300_fresh.csv",
) -> CapacityCalibrationConfig:
    return build_capacity_calibration_config(
        name="capacity_runtime_sensitivity_300_fresh",
        profile_plans=_profile_plans_at_target(RUNTIME_SENSITIVITY_PROFILE_PLANS, (300,)),
        duration_sec=CAPACITY_CALIBRATION_DURATION_SEC,
        summary_csv=summary_csv,
        modes=RUNTIME_SENSITIVITY_MODES,
        source_batch_size=CAPACITY_CALIBRATION_BATCH_SIZE,
        warmup_duration_sec=CAPACITY_CALIBRATION_WARMUP_DURATION_SEC,
        repeats=1,
        collector_timeout_sec=420,
        collector_idle_sec=5,
        stream_startup_wait_sec=10,
        stream_wait_timeout_sec=240,
        slo=CAPACITY_CALIBRATION_PRACTICAL_SLO,
    )


def build_kafka_partition_sensitivity_config(
    *,
    repeats: int = 1,
    summary_csv: str = "artifacts/streaming/evaluation/kafka_partition_sensitivity.csv",
    profile: RuntimeProfile = CAPACITY_THRESHOLD_PROFILE,
    target_rps: int = 500,
) -> CapacityCalibrationConfig:
    profile_plans = tuple(
        CapacityCalibrationProfilePlan(profile, (int(target_rps),), topic_partitions=int(partitions))
        for partitions in KAFKA_PARTITION_SENSITIVITY_PARTITIONS
    )
    return build_capacity_calibration_config(
        name="kafka_partition_sensitivity",
        profile_plans=profile_plans,
        duration_sec=CAPACITY_CALIBRATION_DURATION_SEC,
        summary_csv=summary_csv,
        modes=KAFKA_PARTITION_SENSITIVITY_MODES,
        source_batch_size=CAPACITY_CALIBRATION_BATCH_SIZE,
        warmup_duration_sec=CAPACITY_CALIBRATION_WARMUP_DURATION_SEC,
        repeats=repeats,
        collector_timeout_sec=420,
        collector_idle_sec=5,
        stream_startup_wait_sec=10,
        stream_wait_timeout_sec=240,
        slo=CAPACITY_CALIBRATION_PRACTICAL_SLO,
    )


def _profile_plans_at_target(
    profile_plans: tuple[CapacityCalibrationProfilePlan, ...],
    target_rps_values: tuple[int, ...],
) -> tuple[CapacityCalibrationProfilePlan, ...]:
    return tuple(
        CapacityCalibrationProfilePlan(profile_plan.profile, target_rps_values)
        for profile_plan in profile_plans
    )


def build_capacity_load_threshold_diagnostic_config() -> CapacityCalibrationConfig:
    return build_capacity_load_threshold_config(
        summary_csv="artifacts/streaming/evaluation/capacity_load_threshold.csv",
    )


def build_capacity_overload_probe_config() -> CapacityCalibrationConfig:
    return build_capacity_calibration_config(
        name="capacity_load_threshold_diagnostic",
        profile_plans=_profile_plans_at_target(CAPACITY_LOAD_THRESHOLD_PROFILE_PLANS, (750,)),
        duration_sec=CAPACITY_CALIBRATION_DURATION_SEC,
        summary_csv="artifacts/streaming/evaluation/capacity_load_threshold.csv",
        modes=MODEL_ONLY_CALIBRATION_MODES,
        source_batch_size=CAPACITY_CALIBRATION_BATCH_SIZE,
        warmup_duration_sec=CAPACITY_CALIBRATION_WARMUP_DURATION_SEC,
        repeats=1,
        collector_timeout_sec=420,
        collector_idle_sec=5,
        stream_startup_wait_sec=10,
        stream_wait_timeout_sec=240,
        slo=CAPACITY_CALIBRATION_PRACTICAL_SLO,
    )


def build_capacity_runtime_sensitivity_diagnostic_config() -> CapacityCalibrationConfig:
    return build_capacity_calibration_config(
        name="capacity_runtime_sensitivity_diagnostic",
        profile_plans=_profile_plans_at_target((RUNTIME_SENSITIVITY_PROFILE_PLANS[-1],), (500,)),
        duration_sec=CAPACITY_CALIBRATION_DURATION_SEC,
        summary_csv="artifacts/streaming/evaluation/capacity_runtime_sensitivity.csv",
        modes=MODEL_ONLY_CALIBRATION_MODES,
        source_batch_size=CAPACITY_CALIBRATION_BATCH_SIZE,
        warmup_duration_sec=CAPACITY_CALIBRATION_WARMUP_DURATION_SEC,
        repeats=1,
        collector_timeout_sec=420,
        collector_idle_sec=5,
        stream_startup_wait_sec=10,
        stream_wait_timeout_sec=240,
        slo=CAPACITY_CALIBRATION_PRACTICAL_SLO,
    )


def build_capacity_calibration_diagnostic_configs() -> tuple[CapacityCalibrationConfig, ...]:
    return (
        build_capacity_load_threshold_diagnostic_config(),
        build_capacity_runtime_sensitivity_diagnostic_config(),
    )


def build_capacity_calibration_main_config() -> CapacityCalibrationConfig:
    return build_capacity_load_threshold_config(
        summary_csv="artifacts/streaming/evaluation/capacity_calibration.csv",
    )


def build_capacity_calibration_main_configs(
    *,
    repeats: int = 1,
    summary_suffix: str = "",
) -> tuple[CapacityCalibrationConfig, ...]:
    suffix = str(summary_suffix).strip()
    load_summary = (
        f"artifacts/streaming/evaluation/capacity_load_threshold{suffix}.csv"
    )
    sensitivity_summary = (
        f"artifacts/streaming/evaluation/capacity_runtime_sensitivity{suffix}.csv"
    )
    return (
        build_capacity_load_threshold_config(
            repeats=repeats,
            summary_csv=load_summary,
        ),
        build_capacity_runtime_sensitivity_config(
            repeats=repeats,
            summary_csv=sensitivity_summary,
        ),
    )


CAPACITY_CALIBRATION_CONFIG = CAPACITY_CALIBRATION_SMOKE_CONFIG
