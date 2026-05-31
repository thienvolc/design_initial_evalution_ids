from __future__ import annotations

import shutil
import threading
import time
from dataclasses import dataclass, field

from ids_platform.streaming.runtime.assembly import build_runtime_streams
from ids_platform.streaming.runtime.config import RuntimeConfig
from ids_platform.streaming.runtime.metrics import make_metrics_batch_writer
from ids_platform.streaming.runtime.runner import RuntimeQueries, StructuredStreamingRunner
from ids_platform.streaming.runtime.session import create_spark_session
from ids_platform.streaming.runtime.sinks import (
    build_input_sentinel_writer,
    build_metrics_writer,
    build_prediction_parquet_writer,
    group_runtime_queries,
    start_runtime_queries,
)


@dataclass
class RunningStructuredStreamingJob:
    spark: object
    runner: StructuredStreamingRunner
    _stopped: bool = field(default=False, init=False)

    def wait(self, *, timeout_sec: float | None = None) -> None:
        try:
            self.runner.wait(timeout_sec=timeout_sec)
        finally:
            self.stop()

    def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        try:
            self.runner.stop()
        finally:
            self.spark.stop()


def _reset_runtime_outputs(config: RuntimeConfig) -> None:
    for path in (
        config.output.parquet_checkpoint,
        config.output.metrics_checkpoint,
        config.output.sentinel_checkpoint,
        config.output.artifact_output,
    ):
        if path.exists():
            shutil.rmtree(path)


def start_structured_streaming_job(config: RuntimeConfig) -> RunningStructuredStreamingJob:
    if config.lifecycle.reset_outputs:
        _reset_runtime_outputs(config)

    spark = create_spark_session(config)
    stream_started_epoch_ms = int(time.time() * 1000)

    try:
        streams = build_runtime_streams(
            spark=spark,
            config=config,
            stream_started_epoch_ms=stream_started_epoch_ms,
        )

        parquet_writer = build_prediction_parquet_writer(
            streams.scored_stream,
            output_path=config.output.artifact_output,
            checkpoint_path=config.output.parquet_checkpoint,
            model_name=config.model.name,
        )

        metrics_writer = None
        if config.metrics.enabled:
            write_metrics_batch = make_metrics_batch_writer(
                spark=spark,
                config=config,
                bootstrap_servers=config.kafka.bootstrap_servers,
                input_topic=config.kafka.input_topic,
                metrics_topic=config.kafka.metrics_topic,
                resolved_load_profile=config.resolved_load_profile,
                reported_model_name=config.model.reported_name,
                max_offsets_per_trigger=config.kafka.max_offsets_per_trigger,
                shuffle_partitions=config.spark.shuffle_partitions,
                trigger_interval=config.spark.trigger_interval,
                watermark_delay_sec=config.lifecycle.watermark_delay_sec,
            )
            metrics_writer = build_metrics_writer(
                streams.scored_stream,
                write_metrics_batch=write_metrics_batch,
                checkpoint_path=config.output.metrics_checkpoint,
                model_name=config.model.reported_name,
            )

        sentinel_seen = threading.Event()

        def mark_input_sentinel(batch_df, _batch_id: int) -> None:
            if int(batch_df.count()) > 0:
                sentinel_seen.set()

        sentinel_writer = build_input_sentinel_writer(
            streams.control_stream,
            checkpoint_path=config.output.sentinel_checkpoint,
            model_name=config.model.reported_name,
            mark_input_sentinel=mark_input_sentinel,
        )

        parquet_query, metrics_query, sentinel_query, active_queries = start_runtime_queries(
            parquet_writer=parquet_writer,
            metrics_writer=metrics_writer,
            sentinel_writer=sentinel_writer,
            trigger_interval=config.spark.trigger_interval,
        )
        data_queries, all_queries = group_runtime_queries(
            parquet_query=parquet_query,
            metrics_query=metrics_query,
            sentinel_query=sentinel_query,
        )
        runner = StructuredStreamingRunner(
            queries=RuntimeQueries(
                data_queries=data_queries,
                all_queries=all_queries,
                active_queries=active_queries,
                sentinel_query=sentinel_query,
            ),
            sentinel_seen=sentinel_seen,
        )
        return RunningStructuredStreamingJob(spark=spark, runner=runner)
    except Exception:
        spark.stop()
        raise


def run_structured_streaming_job(config: RuntimeConfig) -> int:
    job = start_structured_streaming_job(config)
    job.wait()
    return 0
