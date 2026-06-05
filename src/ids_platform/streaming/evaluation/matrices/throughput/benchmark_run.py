from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from ids_platform.streaming.config.common import RuntimeProfile
from ids_platform.streaming.evaluation.matrices.common import (
    summarize_prediction_latency,
)
from ids_platform.streaming.replay.config import ReplayConfig
from ids_platform.streaming.replay.runner import (
    publish_input_sentinel,
    run_replay_job_with_summary,
)
from ids_platform.streaming.runtime.config import RuntimeConfig
from ids_platform.streaming.runtime.structured_streaming_job import (
    start_structured_streaming_job,
)
from ids_platform.streaming.evaluation.matrices.throughput.collectors import (
    KafkaLagSampler,
    ResourceSampler,
    summarize_kafka_offset_lag,
)
from ids_platform.streaming.evaluation.matrices.throughput.prediction_artifact import (
    collect_prediction_response_artifact,
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
    collector_timeout_sec: int
    collector_idle_sec: int
    stream_startup_wait_sec: int
    stream_wait_timeout_sec: int
    artifact_output: Path
    warmup_replay: ReplayConfig | None = None
    warmup_wait_timeout_sec: int = 0
    topic_partitions: int = 1


@dataclass(frozen=True)
class BenchmarkRunResult:
    run_started_at: float
    run_start_timestamp_ms: int
    kafka_lag_summary: dict
    resource_summary: dict
    replay_summary: dict
    warmup_replay_summary: dict
    barrier_summary: dict


def _wait_admin_futures(futures: dict, *, ignore_errors: bool = False) -> None:
    for future in futures.values():
        try:
            future.result()
        except Exception:
            if not ignore_errors:
                raise


def _wait_for_topics_ready(admin, topics: tuple[str, ...], *, timeout_sec: int = 30) -> None:
    deadline = time.time() + max(int(timeout_sec), 1)
    pending = set(topics)
    last_error: Exception | None = None
    while pending and time.time() < deadline:
        try:
            metadata = admin.list_topics(timeout=5.0)
            ready = {
                topic
                for topic in pending
                if _topic_is_ready(metadata.topics.get(topic))
            }
            pending.difference_update(ready)
        except Exception as exc:
            last_error = exc
        if pending:
            time.sleep(0.5)

    if pending:
        details = f"; last_error={last_error}" if last_error is not None else ""
        raise RuntimeError(f"Kafka topic(s) not ready: {', '.join(sorted(pending))}{details}")


def _wait_for_topics_absent(
    admin,
    topics: tuple[str, ...],
    *,
    timeout_sec: int = 30,
    ignore_errors: bool = False,
) -> None:
    deadline = time.time() + max(int(timeout_sec), 1)
    pending = set(topics)
    last_error: Exception | None = None
    while pending and time.time() < deadline:
        try:
            metadata = admin.list_topics(timeout=5.0)
            pending = {topic for topic in pending if topic in metadata.topics}
        except Exception as exc:
            last_error = exc
        if pending:
            time.sleep(0.5)

    if pending and not ignore_errors:
        details = f"; last_error={last_error}" if last_error is not None else ""
        raise RuntimeError(f"Kafka topic(s) still present: {', '.join(sorted(pending))}{details}")


def _topic_is_ready(topic_metadata) -> bool:
    if topic_metadata is None:
        return False
    topic_error = getattr(topic_metadata, "error", None)
    topic_error = topic_error() if callable(topic_error) else topic_error
    if topic_error is not None:
        return False
    partitions = getattr(topic_metadata, "partitions", None)
    if not partitions:
        return False
    for partition_metadata in partitions.values():
        partition_error = getattr(partition_metadata, "error", None)
        partition_error = partition_error() if callable(partition_error) else partition_error
        if partition_error is not None:
            return False
        leader = getattr(partition_metadata, "leader", None)
        if leader is None or int(leader) < 0:
            return False
    return True


def _run_topics(config: BenchmarkRunConfig) -> tuple[str, ...]:
    return (config.runtime.kafka.input_topic, config.runtime.kafka.prediction_topic)


def reset_run_topics(config: BenchmarkRunConfig) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic

    topics = _run_topics(config)
    admin = AdminClient({"bootstrap.servers": config.runtime.kafka.bootstrap_servers})
    _wait_admin_futures(admin.delete_topics(list(topics), operation_timeout=15), ignore_errors=True)
    _wait_for_topics_absent(admin, topics, ignore_errors=True)
    new_topics = []
    for topic in topics:
        partitions = (
            max(int(config.topic_partitions), 1)
            if topic in {config.runtime.kafka.input_topic, config.runtime.kafka.prediction_topic}
            else 1
        )
        new_topics.append(NewTopic(topic, num_partitions=partitions, replication_factor=1))
    _wait_admin_futures(admin.create_topics(new_topics, operation_timeout=15), ignore_errors=False)
    _wait_for_topics_ready(admin, topics)


def cleanup_run_topics(
    config: BenchmarkRunConfig,
    *,
    ignore_errors: bool = True,
    attempts: int = 3,
    settle_sec: float = 2.0,
) -> None:
    from confluent_kafka.admin import AdminClient

    topics = _run_topics(config)
    admin = AdminClient({"bootstrap.servers": config.runtime.kafka.bootstrap_servers})
    _delete_topics_until_absent(
        admin,
        topics,
        ignore_errors=ignore_errors,
        attempts=attempts,
        settle_sec=settle_sec,
    )


def cleanup_benchmark_run_topics(
    configs: Iterable[BenchmarkRunConfig],
    *,
    ignore_errors: bool = True,
    settle_sec: float = 2.0,
    attempts: int = 3,
) -> None:
    from confluent_kafka.admin import AdminClient

    topics_by_bootstrap: dict[str, set[str]] = {}
    for config in configs:
        bootstrap_servers = config.runtime.kafka.bootstrap_servers
        topics_by_bootstrap.setdefault(bootstrap_servers, set()).update(_run_topics(config))

    if float(settle_sec) > 0:
        time.sleep(float(settle_sec))

    for bootstrap_servers, topics in topics_by_bootstrap.items():
        if not topics:
            continue
        admin = AdminClient({"bootstrap.servers": bootstrap_servers})
        _delete_topics_until_absent(
            admin,
            tuple(sorted(topics)),
            ignore_errors=ignore_errors,
            attempts=attempts,
            settle_sec=settle_sec,
        )


def _delete_topics_until_absent(
    admin,
    topics: tuple[str, ...],
    *,
    ignore_errors: bool,
    attempts: int,
    settle_sec: float,
) -> None:
    pending = tuple(sorted(set(topics)))
    if not pending:
        return

    max_attempts = max(int(attempts), 1)
    for attempt_index in range(max_attempts):
        _wait_admin_futures(
            admin.delete_topics(list(pending), operation_timeout=15),
            ignore_errors=True,
        )
        _wait_for_topics_absent(
            admin,
            pending,
            timeout_sec=15,
            ignore_errors=True,
        )
        pending = tuple(topic for topic in pending if _topic_exists(admin, topic))
        if not pending:
            return
        if attempt_index < max_attempts - 1 and float(settle_sec) > 0:
            time.sleep(float(settle_sec))

    if pending and not ignore_errors:
        raise RuntimeError(f"Kafka topic(s) still present: {', '.join(pending)}")


def _topic_exists(admin, topic: str) -> bool:
    try:
        metadata = admin.list_topics(timeout=5.0)
        return str(topic) in metadata.topics
    except Exception:
        return True


def _expected_replay_rows(replay: ReplayConfig) -> int:
    return int(replay.source.expected_rows)


def _int_value(value) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _verify_artifact_phase_rows(config: BenchmarkRunConfig, *, phase: str, expected_rows: int) -> dict:
    summary = summarize_prediction_latency(config.artifact_output, phase=phase)
    actual_rows = int(summary.get("artifact_rows_total") or 0)
    if actual_rows != expected_rows:
        raise RuntimeError(
            f"{phase} phase artifact row barrier failed for run_tag={config.run_tag}: "
            f"artifact_rows={actual_rows} expected_rows={expected_rows}"
        )
    return {
        f"{phase}_artifact_barrier_status": "ok",
        f"{phase}_artifact_rows_expected": int(expected_rows),
        f"{phase}_artifact_rows_actual": actual_rows,
    }


def _collect_prediction_artifact(
    config: BenchmarkRunConfig,
    *,
    expected_phase_rows: dict[str, int],
    phase: str,
) -> dict:
    summary = collect_prediction_response_artifact(
        bootstrap_servers=config.runtime.kafka.bootstrap_servers,
        input_topic=config.runtime.kafka.input_topic,
        prediction_topic=config.runtime.kafka.prediction_topic,
        run_tag=config.run_tag,
        artifact_output=config.artifact_output,
        expected_phase_rows=expected_phase_rows,
        timeout_sec=max(int(config.stream_wait_timeout_sec), int(config.collector_timeout_sec), 30),
        idle_sec=max(int(config.collector_idle_sec), 5),
    )
    return {f"{phase}_{key}": value for key, value in summary.items()}


def _verify_kafka_drain(summary: dict, *, run_tag: str, phase: str) -> dict:
    status = str(summary.get("kafka_lag_status") or "").strip()
    if status != "ok":
        raise RuntimeError(
            f"{phase} Kafka drain barrier failed for run_tag={run_tag}: "
            f"kafka_lag_status={status or 'missing'}"
        )

    lag_end = _int_value(summary.get("kafka_lag_records_end"))
    if lag_end != 0:
        raise RuntimeError(
            f"{phase} Kafka drain barrier failed for run_tag={run_tag}: "
            f"kafka_lag_records_end={lag_end}"
        )

    sampler_status = str(summary.get("kafka_lag_sampler_status") or "").strip()
    sampler_lag = _int_value(summary.get("kafka_lag_end_after_drain_records"))
    if sampler_status and sampler_status != "ok":
        raise RuntimeError(
            f"{phase} Kafka lag sampler failed for run_tag={run_tag}: "
            f"kafka_lag_sampler_status={sampler_status}"
        )
    if sampler_status == "ok" and sampler_lag != 0:
        raise RuntimeError(
            f"{phase} Kafka drain sampler barrier failed for run_tag={run_tag}: "
            f"kafka_lag_end_after_drain_records={sampler_lag}"
        )

    return {
        f"{phase}_kafka_drain_barrier_status": "ok",
        f"{phase}_kafka_lag_records_end": lag_end,
        f"{phase}_kafka_lag_end_after_drain_records": sampler_lag
        if sampler_status == "ok"
        else "",
    }


def _wait_for_warmup(
    config: BenchmarkRunConfig,
    *,
    job,
    lag_sampler: KafkaLagSampler,
) -> tuple[dict, dict]:
    warmup_replay = config.warmup_replay
    if warmup_replay is None:
        return {}, {}

    lag_sampler.set_phase("warmup_replay")
    replay_summary = run_replay_job_with_summary(warmup_replay).as_dict(
        prefix="warmup_replay_"
    )
    lag_sampler.mark("warmup_replay_end")
    lag_sampler.set_phase("warmup_drain")
    job.drain_data_queries()
    expected_rows = _expected_replay_rows(warmup_replay)
    barrier_summary = _collect_prediction_artifact(
        config,
        expected_phase_rows={"warmup": expected_rows},
        phase="warmup",
    )
    barrier_summary.update(
        _verify_artifact_phase_rows(
            config,
            phase="warmup",
            expected_rows=expected_rows,
        )
    )
    warmup_lag_summary = summarize_kafka_offset_lag(
        bootstrap_servers=config.runtime.kafka.bootstrap_servers,
        topic=config.runtime.kafka.input_topic,
        artifact_output=config.artifact_output,
        phase="",
        control_tail_records=0,
    )
    barrier_summary.update(
        _verify_kafka_drain(warmup_lag_summary, run_tag=config.run_tag, phase="warmup")
    )
    lag_sampler.mark("warmup_drain_end")
    return replay_summary, barrier_summary


def run_benchmark_run(config: BenchmarkRunConfig) -> BenchmarkRunResult:
    reset_run_topics(config)
    run_started_at = time.time()
    run_start_timestamp_ms = int(run_started_at * 1000)
    resource_sampler = ResourceSampler().start()
    lag_sampler = KafkaLagSampler(
        bootstrap_servers=config.runtime.kafka.bootstrap_servers,
        topic=config.runtime.kafka.input_topic,
        artifact_output=config.artifact_output,
        run_tag=config.run_tag,
        prediction_topic=config.runtime.kafka.prediction_topic,
    ).start()
    kafka_lag_summary: dict = {}
    resource_summary: dict = {}
    replay_summary: dict = {}
    warmup_replay_summary: dict = {}
    barrier_summary: dict = {}

    try:
        job = start_structured_streaming_job(config.runtime)
    except Exception:
        resource_summary = resource_sampler.stop()
        kafka_lag_summary = lag_sampler.stop()
        raise

    try:
        startup_wait_sec = max(int(config.stream_startup_wait_sec), 0)
        if startup_wait_sec:
            time.sleep(startup_wait_sec)

        warmup_replay_summary, warmup_barrier_summary = _wait_for_warmup(
            config,
            job=job,
            lag_sampler=lag_sampler,
        )
        barrier_summary.update(warmup_barrier_summary)
        lag_sampler.set_phase("measure_start")
        job.drain_data_queries()
        lag_sampler.set_phase("measure_replay")
        replay_summary = run_replay_job_with_summary(config.replay).as_dict()
        lag_sampler.mark("measure_replay_end")
        publish_input_sentinel(config.replay.runtime)
        lag_sampler.set_control_tail_records(1)
        lag_sampler.mark("measure_sentinel_published")
        lag_sampler.set_phase("measure_drain")
        job.wait(timeout_sec=max(int(config.stream_wait_timeout_sec), 1))
        barrier_summary.update(
            _collect_prediction_artifact(
                config,
                expected_phase_rows={"measure": _expected_replay_rows(config.replay)},
                phase="measure",
            )
        )
        barrier_summary.update(
            _verify_artifact_phase_rows(
                config,
                phase="measure",
                expected_rows=_expected_replay_rows(config.replay),
            )
        )
        lag_sampler.mark("measure_drain_end")
        kafka_lag_summary = summarize_kafka_offset_lag(
            bootstrap_servers=config.runtime.kafka.bootstrap_servers,
            topic=config.runtime.kafka.input_topic,
            artifact_output=config.artifact_output,
            phase="measure",
            control_tail_records=1,
        )
        kafka_lag_summary.update(lag_sampler.stop())
        barrier_summary.update(
            _verify_kafka_drain(
                kafka_lag_summary,
                run_tag=config.run_tag,
                phase="measure",
            )
        )
        resource_summary = resource_sampler.stop()
    except Exception:
        resource_summary = resource_sampler.stop()
        kafka_lag_summary = lag_sampler.stop()
        job.stop()
        raise

    return BenchmarkRunResult(
        run_started_at=run_started_at,
        run_start_timestamp_ms=run_start_timestamp_ms,
        kafka_lag_summary=kafka_lag_summary,
        resource_summary=resource_summary,
        replay_summary=replay_summary,
        warmup_replay_summary=warmup_replay_summary,
        barrier_summary=barrier_summary,
    )
