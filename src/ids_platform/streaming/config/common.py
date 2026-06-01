from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from ids_platform.streaming.config.runtime import build_runtime_config as _build_runtime_config
from ids_platform.streaming.replay.config import (
    RateStep,
    ReplayConfig,
    ReplayRatePlan,
    ReplayRuntimeConfig,
    ReplaySource,
    ReplayTimingConfig,
)
from ids_platform.streaming.runtime.config import RuntimeConfig
from ids_platform.streaming.runtime.query import safe_tag

if TYPE_CHECKING:
    from ids_platform.streaming.evaluation.matrices.throughput.benchmark_run import (
        BenchmarkRunConfig,
    )


SMOKE_ROW_LIMIT = 1_000
SMOKE_BATCH_SIZE = 500
SMOKE_RPS = 500
SMOKE_DURATION_SEC = 2
SMOKE_STREAM_STARTUP_WAIT_SEC = 10
SMOKE_STREAM_WAIT_TIMEOUT_SEC = 90
SMOKE_METRICS_TIMEOUT_SEC = 120
SMOKE_METRICS_IDLE_SEC = 5
SMOKE_REPLAY_RATE = ReplayRatePlan(
    rows_per_sec=0.0,
    schedule=(RateStep(rows_per_sec=SMOKE_RPS, duration_sec=SMOKE_DURATION_SEC),),
)


@dataclass(frozen=True)
class RuntimeProfile:
    name: str
    max_offsets_per_trigger: int
    shuffle_partitions: int
    trigger_interval: str = "10 seconds"

    def to_legacy_dict(self) -> dict:
        return {
            "name": self.name,
            "max_offsets_per_trigger": int(self.max_offsets_per_trigger),
            "shuffle_partitions": int(self.shuffle_partitions),
            "trigger_interval": self.trigger_interval,
        }


@dataclass(frozen=True)
class BenchmarkRunPlan:
    benchmark: "BenchmarkRunConfig"
    warmup: "BenchmarkRunConfig | None" = None
    summary_context: dict | None = None


@dataclass(frozen=True)
class BenchmarkMatrixConfig:
    name: str
    runs: tuple[BenchmarkRunPlan, ...]
    summary_csv: Path


DEFAULT_TIMING_CONFIG = ReplayTimingConfig(
    random_seed=42,
    trace_order_column="timestamp",
    reorder_window_size=0,
    late_event_ratio=0.0,
    late_event_max_sec=0.0,
)

def benchmark_run_tag(
    *,
    prefix: str,
    repeat_index: int,
    run_index: int,
    label: str,
    created_ms: int | None = None,
) -> str:
    suffix_ms = int(time.time() * 1000) if created_ms is None else int(created_ms)
    clean_label = safe_tag(label)
    return f"{prefix}_r{repeat_index:02d}_{run_index:02d}_{clean_label}_{suffix_ms}"


def stream_wait_timeout(*, row_count: int, rate: ReplayRatePlan, override_seconds: int = 0) -> int:
    if override_seconds > 0:
        return max(int(override_seconds), 30)
    expected_replay_seconds = rate.expected_elapsed_for_rows(max(int(row_count), 0))
    return max(int(expected_replay_seconds) + 120, 120)


def runtime_mode(mode: str) -> str:
    return "pass_through" if str(mode).strip().lower() == "pass_through" else "model"


def model_label(*, model_name: str, mode: str) -> str:
    return "pass_through" if runtime_mode(mode) == "pass_through" else model_name


def make_replay_config(
    *,
    run_tag: str,
    runtime: RuntimeConfig,
    source: ReplaySource,
    rate: ReplayRatePlan,
    timing: ReplayTimingConfig,
) -> ReplayConfig:
    return ReplayConfig(
        source=source,
        runtime=ReplayRuntimeConfig(
            run_tag=run_tag,
            bootstrap_servers=runtime.kafka.bootstrap_servers,
            topic=runtime.kafka.input_topic,
        ),
        rate=rate,
        timing=timing,
    )


def build_benchmark_run_config(
    *,
    run_tag: str,
    repeat_index: int,
    profile: RuntimeProfile,
    source: ReplaySource,
    rate: ReplayRatePlan,
    timing: ReplayTimingConfig,
    model_name: str,
    feature_set: str,
    mode: str = "model",
    metrics_timeout_sec: int,
    metrics_idle_sec: int,
    stream_startup_wait_sec: int,
    stream_wait_timeout_sec: int,
    load_profile: str = "",
    watermark_delay_sec: int = 0,
    drop_late_events: bool = False,
) -> "BenchmarkRunConfig":
    from ids_platform.streaming.evaluation.matrices.throughput.benchmark_run import (
        BenchmarkRunConfig,
    )

    runtime = _build_runtime_config(
        model_name=model_name,
        feature_set=feature_set,
        run_tag=run_tag,
        input_run_tag=run_tag,
        load_profile=load_profile or profile.name,
        mode=runtime_mode(mode),
        starting_offsets="latest",
        max_offsets_per_trigger=profile.max_offsets_per_trigger,
        shuffle_partitions=profile.shuffle_partitions,
        trigger_interval=profile.trigger_interval,
        watermark_delay_sec=watermark_delay_sec,
        drop_late_events=drop_late_events,
        reset_outputs=True,
    )
    return BenchmarkRunConfig(
        run_tag=run_tag,
        runtime=runtime,
        replay=make_replay_config(
            run_tag=run_tag,
            runtime=runtime,
            source=source,
            rate=rate,
            timing=timing,
        ),
        profile=profile,
        repeat_index=repeat_index,
        model_label=model_label(model_name=model_name, mode=mode),
        feature_set=feature_set,
        metrics_timeout_sec=metrics_timeout_sec,
        metrics_idle_sec=metrics_idle_sec,
        stream_startup_wait_sec=stream_startup_wait_sec,
        stream_wait_timeout_sec=stream_wait_timeout_sec,
    )
