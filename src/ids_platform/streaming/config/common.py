from __future__ import annotations

import os
import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING

from ids_platform.streaming.config.runtime import (
    PRIMARY_STREAMING_FEATURE_SET,
    PRIMARY_STREAMING_MODEL_LABEL,
    PRIMARY_STREAMING_MODEL_NAME,
    STREAMING_PREDICTION_ROOT,
    build_runtime_config as _build_runtime_config,
)
from ids_platform.streaming.replay.config import (
    RateStep,
    ReplayConfig,
    ReplayRatePlan,
    ReplayRuntimeConfig,
    ReplaySource,
    ReplaySourcePlan,
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
SMOKE_COLLECTOR_TIMEOUT_SEC = 120
SMOKE_COLLECTOR_IDLE_SEC = 5
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
    spark_master: str = "local[1]"


@dataclass(frozen=True)
class BenchmarkRunPlan:
    benchmark: "BenchmarkRunConfig"
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
TOPIC_NAMESPACE = safe_tag(
    os.environ.get("IDS_STREAMING_TOPIC_NAMESPACE", "").strip()
    or str(int(time.time() * 1000))
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
    source: ReplaySource | ReplaySourcePlan,
    rate: ReplayRatePlan,
    timing: ReplayTimingConfig,
    phase: str = "measure",
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
        phase=phase,
    )


def run_input_topic(run_tag: str) -> str:
    return f"ids.raw.flows.{safe_tag(run_tag)}.{TOPIC_NAMESPACE}"


def run_prediction_topic(run_tag: str) -> str:
    return f"ids.predictions.{safe_tag(run_tag)}.{TOPIC_NAMESPACE}"


def run_artifact_output(run_tag: str) -> Path:
    return STREAMING_PREDICTION_ROOT / safe_tag(run_tag)


def build_benchmark_run_from_source(
    *,
    run_tag: str,
    repeat_index: int,
    profile: RuntimeProfile,
    source: ReplaySource | ReplaySourcePlan,
    rate: ReplayRatePlan,
    timing: ReplayTimingConfig,
    model_name: str,
    feature_set: str,
    mode: str = "model",
    collector_timeout_sec: int,
    collector_idle_sec: int,
    stream_startup_wait_sec: int,
    stream_wait_timeout_sec: int = 0,
    load_profile: str = "",
    topic_partitions: int = 1,
) -> "BenchmarkRunConfig":
    from ids_platform.streaming.evaluation.matrices.throughput.benchmark_run import (
        BenchmarkRunConfig,
    )

    input_topic = run_input_topic(run_tag)
    prediction_topic = run_prediction_topic(run_tag)
    runtime = _build_runtime_config(
        model_name=model_name,
        feature_set=feature_set,
        run_tag=run_tag,
        input_run_tag=run_tag,
        load_profile=load_profile or profile.name,
        mode=runtime_mode(mode),
        input_topic=input_topic,
        prediction_topic=prediction_topic,
        starting_offsets="latest",
        spark_master=profile.spark_master,
        max_offsets_per_trigger=profile.max_offsets_per_trigger,
        shuffle_partitions=profile.shuffle_partitions,
        trigger_interval=profile.trigger_interval,
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
        collector_timeout_sec=collector_timeout_sec,
        collector_idle_sec=collector_idle_sec,
        stream_startup_wait_sec=stream_startup_wait_sec,
        stream_wait_timeout_sec=stream_wait_timeout(
            row_count=source.expected_rows,
            rate=rate,
            override_seconds=stream_wait_timeout_sec,
        ),
        artifact_output=run_artifact_output(run_tag),
        topic_partitions=max(int(topic_partitions), 1),
    )


def build_benchmark_run_plan(
    *,
    run_tag: str,
    repeat_index: int,
    profile: RuntimeProfile,
    source: ReplaySource | ReplaySourcePlan,
    rate: ReplayRatePlan,
    timing: ReplayTimingConfig,
    model_name: str,
    feature_set: str,
    collector_timeout_sec: int,
    collector_idle_sec: int,
    stream_startup_wait_sec: int,
    stream_wait_timeout_sec: int = 0,
    mode: str = "model",
    load_profile: str = "",
    warmup_source: ReplaySource | ReplaySourcePlan | None = None,
    warmup_rate: ReplayRatePlan | None = None,
    warmup_load_profile: str = "",
    warmup_stream_wait_timeout_sec: int = 0,
    topic_partitions: int = 1,
    summary_context: dict | None = None,
) -> BenchmarkRunPlan:
    benchmark = build_benchmark_run_from_source(
        run_tag=run_tag,
        repeat_index=repeat_index,
        profile=profile,
        source=source,
        rate=rate,
        timing=timing,
        model_name=model_name,
        feature_set=feature_set,
        mode=mode,
        collector_timeout_sec=collector_timeout_sec,
        collector_idle_sec=collector_idle_sec,
        stream_startup_wait_sec=stream_startup_wait_sec,
        stream_wait_timeout_sec=stream_wait_timeout_sec,
        load_profile=load_profile,
        topic_partitions=topic_partitions,
    )
    if warmup_source is not None and warmup_rate is not None:
        benchmark = replace(
            benchmark,
            warmup_replay=make_replay_config(
                run_tag=run_tag,
                runtime=benchmark.runtime,
                source=warmup_source,
                rate=warmup_rate,
                timing=timing,
                phase="warmup",
            ),
            warmup_wait_timeout_sec=stream_wait_timeout(
                row_count=warmup_source.expected_rows,
                rate=warmup_rate,
                override_seconds=warmup_stream_wait_timeout_sec,
            ),
        )
    return BenchmarkRunPlan(
        benchmark=benchmark,
        summary_context=summary_context,
    )
