from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path

from ids_platform.common.paths import resolve_project_path
from ids_platform.streaming.config.calibration import CAPACITY_CALIBRATION_PROFILE
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
    build_benchmark_run_from_source,
)
from ids_platform.streaming.replay.config import RateStep, ReplayConfig, ReplayRatePlan, ReplaySourceFactory
from ids_platform.streaming.runtime.config import RuntimeConfig


@dataclass(frozen=True)
class FaultRecoveryScenarioConfig:
    scenario: str
    fault_kind: str
    run_tag: str
    repeat_index: int
    target_rps: int
    runtime: RuntimeConfig
    initial_replay: ReplayConfig
    recovery_replay: ReplayConfig | None
    artifact_output: Path
    collector_timeout_sec: int
    collector_idle_sec: int
    stream_startup_wait_sec: int
    stream_wait_timeout_sec: int
    topic_partitions: int = 1
    pre_fault_min_rows_ratio: float = 0.95
    post_fault_min_rows_ratio: float = 0.95


@dataclass(frozen=True)
class FaultRecoveryMatrixConfig:
    name: str
    scenarios: tuple[FaultRecoveryScenarioConfig, ...]
    summary_csv: Path


FAULT_RECOVERY_PROFILE = RuntimeProfile(
    "fault_recovery_local4_1s",
    CAPACITY_CALIBRATION_PROFILE.max_offsets_per_trigger,
    CAPACITY_CALIBRATION_PROFILE.shuffle_partitions,
    CAPACITY_CALIBRATION_PROFILE.trigger_interval,
    CAPACITY_CALIBRATION_PROFILE.spark_master,
)


def _rate_plan(target_rps: int, duration_sec: int) -> ReplayRatePlan:
    return ReplayRatePlan(
        rows_per_sec=0.0,
        schedule=(RateStep(rows_per_sec=float(target_rps), duration_sec=float(duration_sec)),),
    )


def _replay_for_phase(
    *,
    run_tag: str,
    repeat_index: int,
    profile: RuntimeProfile,
    target_rps: int,
    duration_sec: int,
    batch_size: int,
    model_name: str,
    feature_set: str,
    phase: str,
    load_profile: str,
    collector_timeout_sec: int,
    collector_idle_sec: int,
    stream_startup_wait_sec: int,
    stream_wait_timeout_sec: int,
) -> tuple[RuntimeConfig, ReplayConfig, Path, int]:
    source = ReplaySourceFactory(
        batch_size=batch_size,
        row_limit=max(int(target_rps) * int(duration_sec), 1),
    ).plan()
    benchmark = build_benchmark_run_from_source(
        run_tag=run_tag,
        repeat_index=repeat_index,
        profile=profile,
        source=source,
        rate=_rate_plan(target_rps, duration_sec),
        timing=DEFAULT_TIMING_CONFIG,
        model_name=model_name,
        feature_set=feature_set,
        collector_timeout_sec=collector_timeout_sec,
        collector_idle_sec=collector_idle_sec,
        stream_startup_wait_sec=stream_startup_wait_sec,
        stream_wait_timeout_sec=stream_wait_timeout_sec,
        load_profile=load_profile,
    )
    runtime = replace(
        benchmark.runtime,
        kafka=replace(benchmark.runtime.kafka, starting_offsets="earliest"),
    )
    return runtime, replace(benchmark.replay, phase=phase), benchmark.artifact_output, benchmark.topic_partitions


def _scenario_config(
    *,
    scenario: str,
    fault_kind: str,
    repeat_index: int,
    run_index: int,
    run_prefix: str,
    target_rps: int,
    initial_duration_sec: int,
    recovery_duration_sec: int | None,
    batch_size: int,
    model_name: str,
    feature_set: str,
    collector_timeout_sec: int,
    collector_idle_sec: int,
    stream_startup_wait_sec: int,
    stream_wait_timeout_sec: int,
    created_ms: int,
) -> FaultRecoveryScenarioConfig:
    run_tag = benchmark_run_tag(
        prefix=run_prefix,
        repeat_index=repeat_index,
        run_index=run_index,
        label=scenario,
        created_ms=created_ms,
    )
    phase = "cold_start" if fault_kind == "none" else "pre_fault"
    runtime, initial_replay, artifact_output, topic_partitions = _replay_for_phase(
        run_tag=run_tag,
        repeat_index=repeat_index,
        profile=FAULT_RECOVERY_PROFILE,
        target_rps=target_rps,
        duration_sec=initial_duration_sec,
        batch_size=batch_size,
        model_name=model_name,
        feature_set=feature_set,
        phase=phase,
        load_profile=scenario,
        collector_timeout_sec=collector_timeout_sec,
        collector_idle_sec=collector_idle_sec,
        stream_startup_wait_sec=stream_startup_wait_sec,
        stream_wait_timeout_sec=stream_wait_timeout_sec,
    )
    recovery_replay = None
    if fault_kind != "none":
        if recovery_duration_sec is None:
            raise ValueError("recovery_duration_sec is required for fault scenarios")
        _, recovery_replay, _, _ = _replay_for_phase(
            run_tag=run_tag,
            repeat_index=repeat_index,
            profile=FAULT_RECOVERY_PROFILE,
            target_rps=target_rps,
            duration_sec=recovery_duration_sec,
            batch_size=batch_size,
            model_name=model_name,
            feature_set=feature_set,
            phase="post_fault",
            load_profile=scenario,
            collector_timeout_sec=collector_timeout_sec,
            collector_idle_sec=collector_idle_sec,
            stream_startup_wait_sec=stream_startup_wait_sec,
            stream_wait_timeout_sec=stream_wait_timeout_sec,
        )

    return FaultRecoveryScenarioConfig(
        scenario=scenario,
        fault_kind=fault_kind,
        run_tag=run_tag,
        repeat_index=repeat_index,
        target_rps=target_rps,
        runtime=runtime,
        initial_replay=initial_replay,
        recovery_replay=recovery_replay,
        artifact_output=artifact_output,
        collector_timeout_sec=collector_timeout_sec,
        collector_idle_sec=collector_idle_sec,
        stream_startup_wait_sec=stream_startup_wait_sec,
        stream_wait_timeout_sec=stream_wait_timeout_sec,
        topic_partitions=topic_partitions,
    )


def build_fault_recovery_config(
    *,
    name: str,
    summary_csv: str,
    scenario_specs: tuple[tuple[str, str, int, int, int | None], ...],
    model_name: str = PRIMARY_STREAMING_MODEL_NAME,
    feature_set: str = PRIMARY_STREAMING_FEATURE_SET,
    batch_size: int = 500,
    collector_timeout_sec: int = 180,
    collector_idle_sec: int = 5,
    stream_startup_wait_sec: int = 10,
    stream_wait_timeout_sec: int = 180,
    run_prefix: str = "faultRecovery",
    repeats: int = 1,
) -> FaultRecoveryMatrixConfig:
    created_ms = 0
    scenarios: list[FaultRecoveryScenarioConfig] = []
    for repeat_index in range(1, max(int(repeats), 1) + 1):
        for run_index, (
            scenario,
            fault_kind,
            target_rps,
            initial_duration_sec,
            recovery_duration_sec,
        ) in enumerate(scenario_specs, start=1):
            scenarios.append(
                _scenario_config(
                    scenario=scenario,
                    fault_kind=fault_kind,
                    repeat_index=repeat_index,
                    run_index=run_index,
                    run_prefix=run_prefix,
                    target_rps=target_rps,
                    initial_duration_sec=initial_duration_sec,
                    recovery_duration_sec=recovery_duration_sec,
                    batch_size=batch_size,
                    model_name=model_name,
                    feature_set=feature_set,
                    collector_timeout_sec=collector_timeout_sec,
                    collector_idle_sec=collector_idle_sec,
                    stream_startup_wait_sec=stream_startup_wait_sec,
                    stream_wait_timeout_sec=stream_wait_timeout_sec,
                    created_ms=created_ms,
                )
            )
    return FaultRecoveryMatrixConfig(
        name=name,
        scenarios=tuple(scenarios),
        summary_csv=resolve_project_path(summary_csv),
    )


FAULT_RECOVERY_SMOKE_CONFIG = build_fault_recovery_config(
    name="fault_recovery_smoke",
    summary_csv="artifacts/streaming/evaluation/fault_recovery_smoke.csv",
    scenario_specs=(
        ("spark_process_crash_100rps_smoke", "spark_process_crash", 100, 5, 5),
    ),
    batch_size=SMOKE_BATCH_SIZE,
    collector_timeout_sec=SMOKE_COLLECTOR_TIMEOUT_SEC,
    collector_idle_sec=SMOKE_COLLECTOR_IDLE_SEC,
    stream_startup_wait_sec=SMOKE_STREAM_STARTUP_WAIT_SEC,
    stream_wait_timeout_sec=SMOKE_STREAM_WAIT_TIMEOUT_SEC,
)


def build_fault_recovery_main_config() -> FaultRecoveryMatrixConfig:
    return build_fault_recovery_config(
        name="fault_recovery_main",
        summary_csv="artifacts/streaming/evaluation/fault_recovery.csv",
        scenario_specs=(
            ("cold_start_300rps", "none", 300, 20, None),
            ("spark_process_crash_300rps", "spark_process_crash", 300, 20, 20),
            ("spark_process_crash_500rps", "spark_process_crash", 500, 20, 20),
        ),
        batch_size=500,
        collector_timeout_sec=420,
        collector_idle_sec=5,
        stream_startup_wait_sec=10,
        stream_wait_timeout_sec=420,
        repeats=3,
    )


FAULT_RECOVERY_CONFIG = FAULT_RECOVERY_SMOKE_CONFIG
