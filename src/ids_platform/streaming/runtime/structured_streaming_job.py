from __future__ import annotations

import shutil
import time
from dataclasses import dataclass, field

from ids_platform.streaming.runtime.assembly import build_runtime_streams
from ids_platform.streaming.runtime.config import RuntimeConfig
from ids_platform.streaming.runtime.runner import RuntimeQueries, StructuredStreamingRunner
from ids_platform.streaming.runtime.sentinel import InputSentinelWatcher
from ids_platform.streaming.runtime.session import create_spark_session
from ids_platform.streaming.runtime.sinks import (
    build_prediction_kafka_writer,
    group_runtime_queries,
    start_runtime_queries,
)


@dataclass
class RunningStructuredStreamingJob:
    spark: object
    runner: StructuredStreamingRunner
    sentinel_watcher: InputSentinelWatcher | None = None
    _stopped: bool = field(default=False, init=False)

    def wait(self, *, timeout_sec: float | None = None) -> None:
        try:
            self.runner.wait(timeout_sec=timeout_sec)
        finally:
            self.stop()

    def drain_data_queries(self) -> None:
        self.runner.drain_data_queries()

    def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        try:
            self.runner.stop()
        finally:
            if self.sentinel_watcher is not None:
                self.sentinel_watcher.stop()
            self.spark.stop()


def _reset_runtime_outputs(config: RuntimeConfig) -> None:
    for path in (
        config.output.prediction_checkpoint,
    ):
        if path.exists():
            shutil.rmtree(path)


def start_structured_streaming_job(config: RuntimeConfig) -> RunningStructuredStreamingJob:
    if config.lifecycle.reset_outputs:
        _reset_runtime_outputs(config)

    spark = create_spark_session(config)
    stream_started_epoch_ms = int(time.time() * 1000)

    try:
        scored_stream = build_runtime_streams(
            spark=spark,
            config=config,
            stream_started_epoch_ms=stream_started_epoch_ms,
        )

        prediction_writer = build_prediction_kafka_writer(
            scored_stream,
            bootstrap_servers=config.kafka.bootstrap_servers,
            prediction_topic=config.kafka.prediction_topic,
            checkpoint_path=config.output.prediction_checkpoint,
            model_name=config.model.name,
        )

        sentinel_watcher = InputSentinelWatcher(
            bootstrap_servers=config.kafka.bootstrap_servers,
            topic=config.kafka.input_topic,
            run_tag=config.run.input_run_tag or config.run.run_tag,
        ).start()

        prediction_query, active_queries = start_runtime_queries(
            prediction_writer=prediction_writer,
            trigger_interval=config.spark.trigger_interval,
        )
        data_queries, all_queries = group_runtime_queries(
            prediction_query=prediction_query,
        )
        runner = StructuredStreamingRunner(
            queries=RuntimeQueries(
                data_queries=data_queries,
                all_queries=all_queries,
                active_queries=active_queries,
            ),
            sentinel_seen=sentinel_watcher.seen,
            sentinel_error=sentinel_watcher.error,
        )
        return RunningStructuredStreamingJob(
            spark=spark,
            runner=runner,
            sentinel_watcher=sentinel_watcher,
        )
    except Exception:
        spark.stop()
        raise


def run_structured_streaming_job(config: RuntimeConfig) -> int:
    job = start_structured_streaming_job(config)
    job.wait()
    return 0
