from __future__ import annotations

import json
import os
import shutil
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol, cast

from ids_platform.common.paths import PROJECT_ROOT, resolve_project_path
from ids_platform.streaming.artifacts import (
    load_thresholds,
    resolve_feature_set_path,
    resolve_model_artifact_path,
)
from ids_platform.streaming.config import load_structured_streaming_app_config
from ids_platform.streaming.metrics.system import (
    probe_executor_memory_utilization,
    probe_kafka_lag,
    probe_process_metrics,
    safe_ratio,
)
from ids_platform.streaming.runtime.pipeline import (
    add_event_timing_columns,
    add_processing_latency_columns,
    add_source_latency_columns,
    build_parsed_stream,
    build_prediction_payload,
    build_raw_schema,
    filter_input_run_tag,
    prepare_feature_columns,
)
from ids_platform.streaming.runtime.query import apply_trigger, safe_tag
from ids_platform.streaming.runtime.scoring import add_prediction_columns, make_score_udf
from ids_platform.offline.config import load_feature_list, load_json

class _KafkaProducerLike(Protocol):
    def produce(self, *, topic: str, value: str) -> None: ...
    def poll(self, timeout: float) -> int: ...
    def flush(self, timeout: float | None = None) -> int: ...


_METRICS_PRODUCER_CACHE: dict[str, _KafkaProducerLike] = {}
_METRICS_PUBLISH_FLUSH_TIMEOUT_SEC = 1.0
_QUERY_STOP_TIMEOUT_SEC = 30


def _log_runtime_event(event: str, **fields) -> None:
    parts = [f"[stream] event={event}"]
    for key, value in fields.items():
        if value is None:
            continue
        parts.append(f"{key}={value}")
    print(" ".join(parts), flush=True)


@dataclass(frozen=True)
class StructuredStreamingJobOptions:
    config_path: str = "configs/streaming/online.yaml"
    model_name: str = "logistic_regression"
    feature_set: str = "full"
    run_tag: str = ""
    input_run_tag: str = ""
    load_profile: str = ""

    override_max_offsets: int = 0
    override_shuffle_partitions: int = 0
    override_trigger_interval: str = ""
    override_starting_offsets: str = ""

    override_watermark_delay_sec: int = -1
    drop_late_events: bool = False

    reset_checkpoint: bool = False
    available_now: bool = False
    run_seconds: int = 0


def _collect_batch_offsets(batch_df):
    from pyspark.sql import functions as F

    return [
        (int(row["kafka_partition"]), int(row["max_offset"]))
        for row in batch_df.groupBy("kafka_partition").agg(F.max("kafka_offset").alias("max_offset")).collect()
        if row["kafka_partition"] is not None and row["max_offset"] is not None
    ]


def _collect_batch_statistics(batch_df):
    from pyspark.sql import functions as F

    return batch_df.agg(
        F.count("*").alias("rows"),
        F.expr("percentile_approx(source_to_ingest_ms, 0.5)").alias("source_p50_ms"),
        F.expr("percentile_approx(source_to_ingest_ms, 0.95)").alias("source_p95_ms"),
        F.expr("percentile_approx(source_to_ingest_ms, 0.99)").alias("source_p99_ms"),
        F.expr("percentile_approx(processing_ms, 0.5)").alias("proc_p50_ms"),
        F.expr("percentile_approx(processing_ms, 0.95)").alias("proc_p95_ms"),
        F.expr("percentile_approx(processing_ms, 0.99)").alias("proc_p99_ms"),
        F.expr("percentile_approx(end_to_end_ms, 0.5)").alias("e2e_p50_ms"),
        F.expr("percentile_approx(end_to_end_ms, 0.95)").alias("e2e_p95_ms"),
        F.expr("percentile_approx(end_to_end_ms, 0.99)").alias("e2e_p99_ms"),
        F.expr("percentile_approx(event_lateness_ms, 0.95)").alias("event_lateness_p95_ms"),
        F.avg(F.col("is_late_event").cast("double")).alias("late_event_ratio"),
        F.avg("prediction_score").alias("avg_prediction_score"),
        F.avg(F.col("prediction_label").cast("double")).alias("attack_ratio"),
        F.sum(F.when((F.col("label_binary") == 1) & (F.col("prediction_label") == 1), F.lit(1)).otherwise(F.lit(0))).alias("tp"),
        F.sum(F.when((F.col("label_binary") == 0) & (F.col("prediction_label") == 0), F.lit(1)).otherwise(F.lit(0))).alias("tn"),
        F.sum(F.when((F.col("label_binary") == 0) & (F.col("prediction_label") == 1), F.lit(1)).otherwise(F.lit(0))).alias("fp"),
        F.sum(F.when((F.col("label_binary") == 1) & (F.col("prediction_label") == 0), F.lit(1)).otherwise(F.lit(0))).alias("fn"),
        F.sum(F.when(F.isnotnull(F.col("label_binary")), F.lit(1)).otherwise(F.lit(0))).alias("labeled_rows"),
    ).collect()[0]


def _get_metrics_producer(bootstrap_servers: str) -> _KafkaProducerLike:
    producer = _METRICS_PRODUCER_CACHE.get(bootstrap_servers)
    if producer is not None:
        return producer

    from confluent_kafka import Producer

    producer = cast(_KafkaProducerLike, Producer({"bootstrap.servers": bootstrap_servers}))
    _METRICS_PRODUCER_CACHE[bootstrap_servers] = producer
    return producer


def _reset_metrics_producer(bootstrap_servers: str) -> None:
    producer = _METRICS_PRODUCER_CACHE.pop(bootstrap_servers, None)
    if producer is None:
        return
    try:
        producer.flush(0.0)
    except Exception:
        pass


def _ensure_kafka_topics_exist(*, bootstrap_servers: str, topic_names: list[str]) -> None:
    try:
        from confluent_kafka.admin import AdminClient, NewTopic
    except Exception:
        return

    unique_topic_names = [topic_name.strip() for topic_name in topic_names if topic_name.strip()]
    if not unique_topic_names:
        return

    admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})
    try:
        metadata = admin_client.list_topics(timeout=10.0)
        existing_topics = set(metadata.topics.keys())
    except Exception:
        existing_topics = set()

    missing_topics = [
        NewTopic(topic_name, num_partitions=1, replication_factor=1)
        for topic_name in unique_topic_names
        if topic_name not in existing_topics
    ]
    if not missing_topics:
        return

    try:
        futures = admin_client.create_topics(missing_topics, operation_timeout=10.0, request_timeout=15.0)
        for topic_name, future in futures.items():
            try:
                future.result()
            except Exception as exc:
                message = str(exc)
                if "TopicAlreadyExists" not in message:
                    print(
                        f"[warn] topic preflight failed topic={topic_name}: {exc}",
                        file=sys.stderr,
                        flush=True,
                    )
    except Exception as exc:
        print(f"[warn] topic preflight failed: {exc}", file=sys.stderr, flush=True)


def _publish_metrics_payload(*, payload: dict, bootstrap_servers: str, metrics_topic: str) -> bool:
    try:
        producer = _get_metrics_producer(bootstrap_servers)
        producer.produce(topic=metrics_topic, value=json.dumps(payload, ensure_ascii=False))
        producer.poll(0.0)
        remaining = producer.flush(timeout=_METRICS_PUBLISH_FLUSH_TIMEOUT_SEC)
        if remaining:
            _reset_metrics_producer(bootstrap_servers)
            print(
                f"[warn] metrics publish timed out topic={metrics_topic} queued_messages={remaining}",
                file=sys.stderr,
                flush=True,
            )
            _log_runtime_event(
                "metrics_publish_timeout",
                run_tag=payload.get("run_tag"),
                topic=metrics_topic,
                queued_messages=remaining,
                batch_id=payload.get("batch_id"),
                event_type=payload.get("event_type"),
            )
            return False
        _log_runtime_event(
            "metrics_publish_ok",
            run_tag=payload.get("run_tag"),
            topic=metrics_topic,
            batch_id=payload.get("batch_id"),
            event_type=payload.get("event_type", "batch_metrics"),
        )
        return True
    except Exception as exc:
        _reset_metrics_producer(bootstrap_servers)
        print(
            f"[warn] metrics publish failed topic={metrics_topic}: {exc}",
            file=sys.stderr,
            flush=True,
        )
        _log_runtime_event(
            "metrics_publish_failed",
            run_tag=payload.get("run_tag"),
            topic=metrics_topic,
            batch_id=payload.get("batch_id"),
            event_type=payload.get("event_type"),
            error=type(exc).__name__,
        )
        return False


def _publish_run_completion_metric(
    *,
    bootstrap_servers: str,
    metrics_topic: str,
    run_tag: str,
    model_name: str,
    feature_set: str,
    load_profile: str,
) -> bool:
    payload = {
        "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "ts_epoch_ms": int(time.time() * 1000),
        "run_tag": run_tag,
        "load_profile": load_profile,
        "model_name": model_name,
        "feature_set": feature_set,
        "event_type": "run_completed",
        "final": True,
    }
    return _publish_metrics_payload(
        payload=payload,
        bootstrap_servers=bootstrap_servers,
        metrics_topic=metrics_topic,
    )


def _stop_query_gracefully(query, *, name: str) -> None:
    try:
        if query.isActive:
            query.stop()
        query.awaitTermination(_QUERY_STOP_TIMEOUT_SEC)
    except Exception as exc:
        print(
            f"[warn] query shutdown issue name={name}: {exc}",
            file=sys.stderr,
            flush=True,
        )


def run_structured_streaming_job(options: StructuredStreamingJobOptions) -> int:

    # ── configs ─────────────────────────────────────────
    app_config = load_structured_streaming_app_config(options.config_path)
    spark_config = app_config.runtime
    latency_config = app_config.latency
    kafka_config = app_config.kafka
    paths_config = app_config.paths
    model_configs = app_config.models

    model_name = options.model_name.strip()
    model_entry = None
    for item in model_configs:
        if item.name == model_name and item.enabled:
            model_entry = item
            break

    if model_entry is None:
        raise ValueError(f"Model {model_name} is not enabled in config")


    # ── paths ───────────────────────────────────────────
    manifest_path = resolve_feature_set_path(paths_config.raw, "feature_manifest", options.feature_set)
    valid_metrics_csv = resolve_feature_set_path(paths_config.raw, "valid_metrics_csv", options.feature_set)
    checkpoint_root = resolve_project_path(paths_config.checkpoint_root or "artifacts/streaming/checkpoints")
    prediction_artifact_dir = resolve_project_path(
        paths_config.prediction_artifact_dir or "artifacts/streaming/predictions"
    )

    bootstrap_servers = kafka_config.bootstrap_servers
    input_topic = kafka_config.input_topic
    output_topic = kafka_config.output_topic
    metrics_topic = kafka_config.metrics_topic
    starting_offsets = kafka_config.starting_offsets
    if options.override_starting_offsets.strip():
        starting_offsets = options.override_starting_offsets.strip()
    fail_on_data_loss = str(kafka_config.fail_on_data_loss).lower()

    max_offsets_per_trigger = kafka_config.max_offsets_per_trigger
    if options.override_max_offsets > 0:
        max_offsets_per_trigger = int(options.override_max_offsets)

    shuffle_partitions = spark_config.shuffle_partitions
    if options.override_shuffle_partitions > 0:
        shuffle_partitions = int(options.override_shuffle_partitions)

    trigger_interval = spark_config.trigger_interval
    if options.override_trigger_interval.strip():
        trigger_interval = options.override_trigger_interval.strip()

    watermark_delay_sec = latency_config.watermark_delay_sec
    if options.override_watermark_delay_sec >= 0:
        watermark_delay_sec = int(options.override_watermark_delay_sec)
    watermark_delay_sec = max(watermark_delay_sec, 0)


    # ── features ────────────────────────────────────────
    manifest = load_json(manifest_path)
    full_feature_columns = [str(column_name) for column_name in manifest.get("feature_columns", [])]
    if not full_feature_columns:
        raise ValueError("feature_columns missing in feature manifest")

    if options.feature_set == "reduced":
        reduced_registry = PROJECT_ROOT / "configs" / "modeling" / "feature_registry.yaml"
        selected_features = load_feature_list(reduced_registry)
        active_feature_columns = [
            column_name
            for column_name in selected_features
            if column_name in full_feature_columns
        ]
    else:
        active_feature_columns = full_feature_columns

    if not active_feature_columns:
        raise ValueError(f"No active features for feature_set={options.feature_set}")


    # ── impute ──────────────────────────────────────────
    fill_values = {str(key): float(value) for key, value in (manifest.get("imputer_fill_values") or {}).items()}
    thresholds = load_thresholds(valid_metrics_csv)

    threshold = model_entry.threshold
    if threshold is None:
        threshold = thresholds.get(model_name, 0.5)
    threshold = float(threshold)


    # ── runtime ─────────────────────────────────────────
    model_path = resolve_model_artifact_path(model_entry.raw, options.feature_set)

    _ensure_kafka_topics_exist(
        bootstrap_servers=bootstrap_servers,
        topic_names=[input_topic, output_topic, metrics_topic],
    )

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    os.environ.setdefault("SPARK_LOCAL_IP", spark_config.driver_host)

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName(spark_config.app_name)
        .master(spark_config.master)
        .config("spark.driver.host", spark_config.driver_host)
        .config("spark.driver.bindAddress", spark_config.driver_bind_address)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.execution.arrow.pyspark.enabled", str(spark_config.arrow_enabled).lower())
    )

    kafka_packages = spark_config.kafka_packages or "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
    if kafka_packages:
        spark = spark.config("spark.jars.packages", kafka_packages)

    spark = spark.getOrCreate()


    # ── stream ──────────────────────────────────────────
    raw_schema = build_raw_schema(full_feature_columns)
    parsed_stream = build_parsed_stream(
        spark,
        bootstrap_servers=bootstrap_servers,
        input_topic=input_topic,
        starting_offsets=starting_offsets,
        fail_on_data_loss=fail_on_data_loss,
        max_offsets_per_trigger=max_offsets_per_trigger,
        raw_schema=raw_schema,
    )
    parsed_stream = filter_input_run_tag(parsed_stream, options.input_run_tag)

    prepared_stream = prepare_feature_columns(
        parsed_stream,
        full_feature_columns=full_feature_columns,
        active_feature_columns=active_feature_columns,
        fill_values=fill_values,
    )
    prepared_stream = add_event_timing_columns(
        prepared_stream,
        watermark_delay_sec=watermark_delay_sec,
        drop_late_events=bool(options.drop_late_events),
    )

    score_udf = make_score_udf(str(model_path), full_feature_columns, fill_values)
    scored_stream = add_prediction_columns(
        prepared_stream,
        score_udf=score_udf,
        feature_columns=full_feature_columns,
        threshold=threshold,
        model_name=model_name,
        feature_set=options.feature_set,
        run_tag=options.run_tag,
    )

    scored_stream = add_source_latency_columns(scored_stream, source_mode=latency_config.source_to_ingest_mode)
    scored_stream = add_processing_latency_columns(scored_stream)
    prediction_payload = build_prediction_payload(scored_stream)


    # ── checkpoints ─────────────────────────────────────
    run_suffix = safe_tag(options.run_tag) \
        if options.run_tag.strip() \
        else safe_tag(f"{model_name}_{options.feature_set}")

    kafka_checkpoint = checkpoint_root / run_suffix / "kafka"
    parquet_checkpoint = checkpoint_root / run_suffix / "parquet"
    metrics_checkpoint = checkpoint_root / run_suffix / "metrics"
    artifact_output = prediction_artifact_dir / run_suffix

    if options.reset_checkpoint:
        for path in (kafka_checkpoint, parquet_checkpoint, metrics_checkpoint, artifact_output):
            if path.exists():
                shutil.rmtree(path)


    # ── scoring ─────────────────────────────────────────
    kafka_writer = (
        prediction_payload.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("topic", output_topic)
        .option("checkpointLocation", str(kafka_checkpoint))
        .outputMode("append")
        .queryName(f"ids_predictions_kafka_{model_name}")
    )
    parquet_writer = (
        scored_stream.select(
            "flow_id",
            "model_name",
            "feature_set",
            "run_tag",
            "prediction_label",
            "prediction_score",
            "threshold_used",
            "event_time_ts",
            "ingest_time",
            "emit_time",
            "source_to_ingest_ms",
            "processing_ms",
            "end_to_end_ms",
            "event_lateness_ms",
            "is_late_event",
            "label_binary",
            "label",
        )
        .writeStream
        .format("parquet")
        .option("path", str(artifact_output))
        .option("checkpointLocation", str(parquet_checkpoint))
        .outputMode("append")
        .queryName(f"ids_predictions_parquet_{model_name}")
    )

    def write_metrics_batch(batch_df, batch_id: int) -> None:
        metrics_batch_df = batch_df.select(
            "kafka_partition",
            "kafka_offset",
            "source_to_ingest_ms",
            "processing_ms",
            "end_to_end_ms",
            "event_lateness_ms",
            "is_late_event",
            "prediction_score",
            "prediction_label",
            "label_binary",
        ).cache()
        batch_start = time.perf_counter()
        metric_ts_epoch_ms = int(time.time() * 1000)

        try:
            offsets = _collect_batch_offsets(metrics_batch_df)
            stat = _collect_batch_statistics(metrics_batch_df)
            row_count = int(stat["rows"] or 0)
            if row_count == 0:
                return

            kafka_lag = probe_kafka_lag(bootstrap_servers=bootstrap_servers, topic=input_topic, offsets=offsets)
            proc_metrics = probe_process_metrics()
            executor_metrics = probe_executor_memory_utilization(spark)

            tp = float(stat["tp"] or 0.0)
            tn = float(stat["tn"] or 0.0)
            fp = float(stat["fp"] or 0.0)
            fn = float(stat["fn"] or 0.0)
            labeled_rows = float(stat["labeled_rows"] or 0.0)
            precision = safe_ratio(tp, tp + fp)
            recall = safe_ratio(tp, tp + fn)
            f1 = safe_ratio(2.0 * precision * recall, precision + recall)
            fpr = safe_ratio(fp, fp + tn)
            fnr = safe_ratio(fn, fn + tp)

            payload = {
                "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "ts_epoch_ms": metric_ts_epoch_ms,
                "batch_id": int(batch_id),
                "run_tag": options.run_tag,
                "load_profile": options.load_profile,
                "model_name": model_name,
                "feature_set": options.feature_set,
                "metric_sources": {
                    "latency_detection": "spark_foreachBatch_aggregation",
                    "kafka_lag": "kafka_topic_high_watermark_minus_batch_offsets",
                    "system_utilization": "driver_process_and_spark_executor_memory",
                },
                "system_knobs": {
                    "max_offsets_per_trigger": max_offsets_per_trigger,
                    "shuffle_partitions": shuffle_partitions,
                    "trigger_interval": trigger_interval,
                    "watermark_delay_sec": watermark_delay_sec,
                    "drop_late_events": bool(options.drop_late_events),
                },
                "rows": int(row_count),
                "latency_ms": {
                    "source_to_ingest": {
                        "p50": float(stat["source_p50_ms"] or 0.0),
                        "p95": float(stat["source_p95_ms"] or 0.0),
                        "p99": float(stat["source_p99_ms"] or 0.0),
                    },
                    "processing": {
                        "p50": float(stat["proc_p50_ms"] or 0.0),
                        "p95": float(stat["proc_p95_ms"] or 0.0),
                        "p99": float(stat["proc_p99_ms"] or 0.0),
                    },
                    "end_to_end": {
                        "p50": float(stat["e2e_p50_ms"] or 0.0),
                        "p95": float(stat["e2e_p95_ms"] or 0.0),
                        "p99": float(stat["e2e_p99_ms"] or 0.0),
                    },
                    "event_lateness": {
                        "p95": float(stat["event_lateness_p95_ms"] or 0.0),
                    },
                },
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
                "reliability": {
                    "failed_batches": 0,
                    "metrics_publish_retry_count": 0,
                    "metrics_publish_failure_count": 0,
                },
                "detection": {
                    "labeled_rows": int(labeled_rows),
                    "tp": int(tp),
                    "tn": int(tn),
                    "fp": int(fp),
                    "fn": int(fn),
                    "precision": float(precision),
                    "recall": float(recall),
                    "f1": float(f1),
                    "fpr": float(fpr),
                    "fnr": float(fnr),
                },
                "avg_prediction_score": float(stat["avg_prediction_score"] or 0.0),
                "attack_ratio": float(stat["attack_ratio"] or 0.0),
            }

            batch_wall_seconds = max(time.perf_counter() - batch_start, 1e-9)
            payload["batch_wall_ms"] = batch_wall_seconds * 1000.0
            payload["rows_per_sec"] = float(payload["rows"]) / batch_wall_seconds

            publish_ok = _publish_metrics_payload(
                payload=payload,
                bootstrap_servers=bootstrap_servers,
                metrics_topic=metrics_topic,
            )
            _log_runtime_event(
                "batch_metrics",
                run_tag=options.run_tag,
                batch_id=batch_id,
                rows=payload["rows"],
                rows_per_sec=f"{payload['rows_per_sec']:.2f}",
                fpr=f"{fpr:.6f}",
                fnr=f"{fnr:.6f}",
                publish_ok=publish_ok,
            )
        finally:
            metrics_batch_df.unpersist()

    metrics_writer = (
        scored_stream.writeStream
        .foreachBatch(write_metrics_batch)
        .option("checkpointLocation", str(metrics_checkpoint))
        .queryName(f"ids_metrics_{model_name}")
    )


    # ── stop ────────────────────────────────────────────
    kafka_query = apply_trigger(kafka_writer,
                                available_now=options.available_now,
                                trigger_interval=trigger_interval).start()
    parquet_query = apply_trigger(parquet_writer,
                                  available_now=options.available_now,
                                  trigger_interval=trigger_interval).start()
    metrics_query = apply_trigger(metrics_writer,
                                  available_now=options.available_now,
                                  trigger_interval=trigger_interval).start()

    print(
        f"Started structured streaming model={model_name} input_topic={input_topic} output_topic={output_topic} "
        f"metrics_topic={metrics_topic} feature_set={options.feature_set} run_tag={options.run_tag} "
        f"max_offsets={max_offsets_per_trigger} shuffle_partitions={shuffle_partitions} trigger={trigger_interval} "
        f"watermark_delay_sec={watermark_delay_sec} drop_late_events={bool(options.drop_late_events)} "
        f"available_now={options.available_now}",
        flush=True,
    )
    _log_runtime_event(
        "job_start",
        run_tag=options.run_tag,
        input_run_tag=options.input_run_tag,
        model=model_name,
        feature_set=options.feature_set,
        max_offsets=max_offsets_per_trigger,
        shuffle_partitions=shuffle_partitions,
        trigger_interval=trigger_interval,
        watermark_delay_sec=watermark_delay_sec,
        available_now=options.available_now,
        run_seconds=options.run_seconds,
    )

    if options.available_now:
        kafka_query.awaitTermination()
        parquet_query.awaitTermination()
        metrics_query.awaitTermination()
    elif options.run_seconds and options.run_seconds > 0:
        time.sleep(options.run_seconds)
        _stop_query_gracefully(kafka_query, name="predictions_kafka")
        _stop_query_gracefully(parquet_query, name="predictions_parquet")
        _stop_query_gracefully(metrics_query, name="metrics")
    else:
        spark.streams.awaitAnyTermination()

    _publish_run_completion_metric(
        bootstrap_servers=bootstrap_servers,
        metrics_topic=metrics_topic,
        run_tag=options.run_tag,
        model_name=model_name,
        feature_set=options.feature_set,
        load_profile=options.load_profile,
    )
    _log_runtime_event(
        "job_stop",
        run_tag=options.run_tag,
        model=model_name,
        feature_set=options.feature_set,
    )

    spark.stop()
    return 0
