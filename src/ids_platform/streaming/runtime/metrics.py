from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol, cast

from ids_platform.streaming.runtime.system_metrics import (
    probe_executor_memory_utilization,
    probe_kafka_lag,
    probe_process_metrics,
)


class _KafkaProducerLike(Protocol):
    def produce(self, *, topic: str, value: str) -> None: ...
    def poll(self, timeout: float) -> int: ...
    def flush(self, timeout: float | None = None) -> int: ...


_METRICS_PRODUCER_CACHE: dict[str, _KafkaProducerLike] = {}
_METRICS_PUBLISH_FLUSH_TIMEOUT_SEC = 1.0

METRICS_BATCH_COLUMNS = (
    "kafka_partition",
    "kafka_offset",
    "source_to_ingest_ms",
    "event_lateness_ms",
    "is_late_event",
)


def collect_batch_offsets(batch_df):
    from pyspark.sql import functions as F

    return [
        (int(row["kafka_partition"]), int(row["max_offset"]))
        for row in batch_df.groupBy("kafka_partition").agg(F.max("kafka_offset").alias("max_offset")).collect()
        if row["kafka_partition"] is not None and row["max_offset"] is not None
    ]


def collect_batch_statistics(batch_df):
    from pyspark.sql import functions as F

    return batch_df.agg(
        F.count("*").alias("rows"),
        F.expr("percentile_approx(source_to_ingest_ms, 0.5)").alias("source_p50_ms"),
        F.expr("percentile_approx(source_to_ingest_ms, 0.95)").alias("source_p95_ms"),
        F.expr("percentile_approx(source_to_ingest_ms, 0.99)").alias("source_p99_ms"),
        F.expr("percentile_approx(event_lateness_ms, 0.95)").alias("event_lateness_p95_ms"),
        F.avg(F.col("is_late_event").cast("double")).alias("late_event_ratio"),
    ).collect()[0]


def _build_latency_summary(*, stat, batch_wall_ms: float) -> dict:
    source_p50_ms = float(stat["source_p50_ms"] or 0.0)
    source_p95_ms = float(stat["source_p95_ms"] or 0.0)
    source_p99_ms = float(stat["source_p99_ms"] or 0.0)
    ingest_to_emit_ms = float(batch_wall_ms)
    return {
        "source_to_ingest": {
            "p50": source_p50_ms,
            "p95": source_p95_ms,
            "p99": source_p99_ms,
        },
        "processing": {
            "semantic": "legacy_alias_for_batch_wall_time",
            "p50": ingest_to_emit_ms,
            "p95": ingest_to_emit_ms,
            "p99": ingest_to_emit_ms,
        },
        "ingest_to_emit": {
            "p50": ingest_to_emit_ms,
            "p95": ingest_to_emit_ms,
            "p99": ingest_to_emit_ms,
        },
        "end_to_end": {
            "semantic": "legacy_alias_for_source_to_ingest_plus_batch_wall_time",
            "p50": source_p50_ms + ingest_to_emit_ms,
            "p95": source_p95_ms + ingest_to_emit_ms,
            "p99": source_p99_ms + ingest_to_emit_ms,
        },
        "source_to_emit": {
            "p50": source_p50_ms + ingest_to_emit_ms,
            "p95": source_p95_ms + ingest_to_emit_ms,
            "p99": source_p99_ms + ingest_to_emit_ms,
        },
        "event_lateness": {
            "p95": float(stat["event_lateness_p95_ms"] or 0.0),
        },
    }


def _build_batch_metrics_payload(
    *,
    batch_id: int,
    stat,
    row_count: int,
    batch_wall_seconds: float,
    metric_ts_epoch_ms: int,
    config,
    resolved_load_profile: str,
    reported_model_name: str,
    max_offsets_per_trigger: int,
    shuffle_partitions: int,
    trigger_interval: str,
    watermark_delay_sec: int,
    kafka_lag: dict,
    proc_metrics: dict,
    executor_metrics: dict,
) -> dict:
    batch_wall_ms = batch_wall_seconds * 1000.0
    payload = {
        "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "ts_epoch_ms": metric_ts_epoch_ms,
        "batch_id": int(batch_id),
        "run_tag": config.run.run_tag,
        "load_profile": resolved_load_profile,
        "model_name": reported_model_name,
        "feature_set": config.features.feature_set,
        "metric_sources": {
            "batch_operational": "spark_foreachBatch_aggregation",
            "kafka_lag": "kafka_topic_high_watermark_minus_batch_offsets",
            "system_utilization": "driver_process_and_spark_executor_memory",
        },
        "system_knobs": {
            "max_offsets_per_trigger": max_offsets_per_trigger,
            "shuffle_partitions": shuffle_partitions,
            "trigger_interval": trigger_interval,
            "watermark_delay_sec": watermark_delay_sec,
            "drop_late_events": bool(config.lifecycle.drop_late_events),
        },
        "rows": int(row_count),
        "latency_ms": _build_latency_summary(stat=stat, batch_wall_ms=batch_wall_ms),
        "event_time": {
            "late_event_ratio": float(stat["late_event_ratio"] or 0.0),
        },
        "kafka": {
            "lag_records_total": kafka_lag.get("lag_records_total"),
            "lag_records_max_partition": kafka_lag.get("lag_records_max_partition"),
        },
        "system": {
            "driver_cpu_percent": proc_metrics.get("driver_cpu_percent"),
            "driver_rss_mb": proc_metrics.get("driver_rss_mb"),
            "executor_mem_util_avg": executor_metrics.get("executor_mem_util_avg"),
            "executor_mem_util_p95": executor_metrics.get("executor_mem_util_p95"),
                "executor_count": executor_metrics.get("executor_count"),
        },
    }
    payload["batch_wall_ms"] = batch_wall_ms
    payload["rows_per_sec"] = float(payload["rows"]) / batch_wall_seconds
    return payload


@dataclass(frozen=True)
class MetricsPublisher:
    bootstrap_servers: str
    metrics_topic: str

    def publish(self, payload: dict) -> bool:
        return publish_metrics_payload(
            payload=payload,
            bootstrap_servers=self.bootstrap_servers,
            metrics_topic=self.metrics_topic,
        )


@dataclass(frozen=True)
class MetricsBatchWriter:
    spark: object
    config: object
    input_topic: str
    publisher: MetricsPublisher
    resolved_load_profile: str
    reported_model_name: str
    max_offsets_per_trigger: int
    shuffle_partitions: int
    trigger_interval: str
    watermark_delay_sec: int

    def write(self, batch_df, batch_id: int) -> None:
        metrics_batch_df = batch_df.select(*METRICS_BATCH_COLUMNS).cache()
        batch_start = time.perf_counter()
        metric_ts_epoch_ms = int(time.time() * 1000)

        try:
            offsets = collect_batch_offsets(metrics_batch_df)
            stat = collect_batch_statistics(metrics_batch_df)
            row_count = int(stat["rows"] or 0)
            if row_count == 0:
                return

            kafka_lag = probe_kafka_lag(
                bootstrap_servers=self.publisher.bootstrap_servers,
                topic=self.input_topic,
                offsets=offsets,
            )
            proc_metrics = probe_process_metrics()
            executor_metrics = probe_executor_memory_utilization(self.spark)
            batch_wall_seconds = max(time.perf_counter() - batch_start, 1e-9)
            payload = _build_batch_metrics_payload(
                batch_id=batch_id,
                stat=stat,
                row_count=row_count,
                batch_wall_seconds=batch_wall_seconds,
                metric_ts_epoch_ms=metric_ts_epoch_ms,
                config=self.config,
                resolved_load_profile=self.resolved_load_profile,
                reported_model_name=self.reported_model_name,
                max_offsets_per_trigger=self.max_offsets_per_trigger,
                shuffle_partitions=self.shuffle_partitions,
                trigger_interval=self.trigger_interval,
                watermark_delay_sec=self.watermark_delay_sec,
                kafka_lag=kafka_lag,
                proc_metrics=proc_metrics,
                executor_metrics=executor_metrics,
            )

            self.publisher.publish(payload)
        finally:
            metrics_batch_df.unpersist()


def make_metrics_batch_writer(
    *,
    spark,
    config,
    bootstrap_servers: str,
    input_topic: str,
    metrics_topic: str,
    resolved_load_profile: str,
    reported_model_name: str,
    max_offsets_per_trigger: int,
    shuffle_partitions: int,
    trigger_interval: str,
    watermark_delay_sec: int,
):
    writer = MetricsBatchWriter(
        spark=spark,
        config=config,
        input_topic=input_topic,
        publisher=MetricsPublisher(
            bootstrap_servers=bootstrap_servers,
            metrics_topic=metrics_topic,
        ),
        resolved_load_profile=resolved_load_profile,
        reported_model_name=reported_model_name,
        max_offsets_per_trigger=max_offsets_per_trigger,
        shuffle_partitions=shuffle_partitions,
        trigger_interval=trigger_interval,
        watermark_delay_sec=watermark_delay_sec,
    )
    return writer.write


def _get_metrics_producer(bootstrap_servers: str) -> _KafkaProducerLike:
    producer = _METRICS_PRODUCER_CACHE.get(bootstrap_servers)
    if producer is not None:
        return producer

    from confluent_kafka import Producer

    producer = cast(_KafkaProducerLike, Producer({"bootstrap.servers": bootstrap_servers}))
    _METRICS_PRODUCER_CACHE[bootstrap_servers] = producer
    return producer


def publish_metrics_payload(
    *,
    payload: dict,
    bootstrap_servers: str,
    metrics_topic: str,
) -> bool:
    producer = _get_metrics_producer(bootstrap_servers)
    producer.produce(topic=metrics_topic, value=json.dumps(payload, ensure_ascii=False))
    producer.poll(0.0)
    remaining = producer.flush(timeout=_METRICS_PUBLISH_FLUSH_TIMEOUT_SEC)
    if remaining:
        raise RuntimeError(f"metrics producer failed to flush {remaining} queued message(s)")
    return True
