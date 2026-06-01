from __future__ import annotations

import time
from dataclasses import dataclass

from ids_platform.streaming.config.common import RuntimeProfile
from ids_platform.streaming.evaluation.matrices.common import collect_matching_metrics
from ids_platform.streaming.replay.config import ReplayConfig
from ids_platform.streaming.replay.runner import publish_input_sentinel, run_replay_job
from ids_platform.streaming.runtime.config import RuntimeConfig
from ids_platform.streaming.runtime.structured_streaming_job import (
    start_structured_streaming_job,
)


@dataclass(frozen=True)
class BenchmarkRunConfig:
    run_tag: str
    runtime: RuntimeConfig
    replay: ReplayConfig
    profile: RuntimeProfile
    repeat_index: int
    model_label: str
    feature_set: str
    metrics_timeout_sec: int
    metrics_idle_sec: int
    stream_startup_wait_sec: int
    stream_wait_timeout_sec: int


@dataclass(frozen=True)
class BenchmarkRunResult:
    metrics_rows: list[dict]
    run_started_at: float
    run_start_timestamp_ms: int


def _post_replay_settle_seconds(trigger_interval: str) -> float:
    parts = str(trigger_interval or "").strip().split()
    try:
        interval_seconds = float(parts[0])
    except Exception:
        interval_seconds = 5.0
    return min(max(interval_seconds * 3.0, 5.0), 30.0)


def run_benchmark_run(config: BenchmarkRunConfig) -> BenchmarkRunResult:
    run_started_at = time.time()
    run_start_timestamp_ms = int(run_started_at * 1000)

    job = start_structured_streaming_job(config.runtime)
    try:
        startup_wait_sec = max(int(config.stream_startup_wait_sec), 0)
        if startup_wait_sec:
            time.sleep(startup_wait_sec)

        run_replay_job(config.replay)
        publish_input_sentinel(config.replay.runtime)
        time.sleep(_post_replay_settle_seconds(config.runtime.spark.trigger_interval))
        job.wait(timeout_sec=max(int(config.stream_wait_timeout_sec), 1))
    except Exception:
        job.stop()
        raise

    metrics_rows: list[dict] = []
    if int(config.metrics_timeout_sec) > 0:
        metrics_rows = collect_matching_metrics(
            bootstrap_servers=config.runtime.kafka.bootstrap_servers,
            topic=config.runtime.kafka.metrics_topic,
            run_tag=config.run_tag,
            timeout_sec=int(config.metrics_timeout_sec),
            idle_sec=int(config.metrics_idle_sec),
            group_prefix="layer-a-metrics",
            start_timestamp_ms=max(run_start_timestamp_ms - 30_000, 0),
        )

    return BenchmarkRunResult(
        metrics_rows=metrics_rows,
        run_started_at=run_started_at,
        run_start_timestamp_ms=run_start_timestamp_ms,
    )
