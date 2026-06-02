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
    warmup_replay: ReplayConfig | None = None
    warmup_wait_timeout_sec: int = 0


@dataclass(frozen=True)
class BenchmarkRunResult:
    metrics_rows: list[dict]
    run_started_at: float
    run_start_timestamp_ms: int


def _metric_phase(payload: dict) -> str:
    phase = str(payload.get("benchmark_phase") or "").strip().lower()
    return "warmup" if phase == "warmup" else "measure"


def _phase_row_count(metrics_rows: list[dict], phase: str) -> int:
    expected_phase = _metric_phase({"benchmark_phase": phase})
    return sum(
        int(payload.get("rows") or 0)
        for payload in metrics_rows
        if _metric_phase(payload) == expected_phase
    )


def _wait_for_warmup(config: BenchmarkRunConfig, *, start_timestamp_ms: int) -> None:
    warmup_replay = config.warmup_replay
    if warmup_replay is None:
        return

    run_replay_job(warmup_replay)
    expected_rows = int(warmup_replay.source.table.num_rows)
    if expected_rows <= 0 or int(config.metrics_timeout_sec) <= 0:
        return

    metrics_rows = collect_matching_metrics(
        bootstrap_servers=config.runtime.kafka.bootstrap_servers,
        topic=config.runtime.kafka.metrics_topic,
        run_tag=config.run_tag,
        timeout_sec=max(int(config.warmup_wait_timeout_sec), 30),
        idle_sec=int(config.metrics_idle_sec),
        group_prefix="benchmark-warmup",
        start_timestamp_ms=max(start_timestamp_ms - 30_000, 0),
    )
    processed_rows = _phase_row_count(metrics_rows, "warmup")
    if processed_rows < expected_rows:
        raise RuntimeError(
            f"warmup phase did not finish for run_tag={config.run_tag}: "
            f"processed_rows={processed_rows} expected_rows={expected_rows}"
        )


def run_benchmark_run(config: BenchmarkRunConfig) -> BenchmarkRunResult:
    run_started_at = time.time()
    run_start_timestamp_ms = int(run_started_at * 1000)

    job = start_structured_streaming_job(config.runtime)
    try:
        startup_wait_sec = max(int(config.stream_startup_wait_sec), 0)
        if startup_wait_sec:
            time.sleep(startup_wait_sec)

        _wait_for_warmup(config, start_timestamp_ms=run_start_timestamp_ms)
        run_replay_job(config.replay)
        publish_input_sentinel(config.replay.runtime)
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
