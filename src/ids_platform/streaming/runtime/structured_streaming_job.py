from __future__ import annotations

import json
import os
import re
import signal
import shutil
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol, cast

from ids_platform.common.paths import resolve_project_path
from ids_platform.streaming.artifacts import (
    load_thresholds,
    resolve_feature_set_path,
    resolve_model_artifact_path,
)
from ids_platform.streaming.config import load_structured_streaming_app_config
from ids_platform.streaming.metrics.system import (
    prime_process_metrics_probe,
    probe_executor_memory_utilization,
    probe_kafka_lag,
    probe_process_metrics,
    safe_ratio_or_none,
)
from ids_platform.streaming.runtime.control import clear_shutdown_request, shutdown_request_path
from ids_platform.streaming.runtime.pipeline import (
    add_event_timing_columns,
    add_processing_latency_columns,
    add_source_latency_columns,
    build_parsed_stream,
    build_prediction_payload,
    build_raw_schema,
    filter_input_run_tag,
    prepare_feature_columns,
    split_control_and_data_records,
)
from ids_platform.streaming.runtime.query import apply_trigger, safe_tag
from ids_platform.streaming.runtime.scoring import (
    add_prediction_columns,
    make_score_udf,
    prewarm_score_udf_model,
)
from ids_platform.offline.config import load_json

class _KafkaProducerLike(Protocol):
    def produce(self, *, topic: str, value: str) -> None: ...
    def poll(self, timeout: float) -> int: ...
    def flush(self, timeout: float | None = None) -> int: ...


_METRICS_PRODUCER_CACHE: dict[str, _KafkaProducerLike] = {}
_METRICS_PUBLISH_FLUSH_TIMEOUT_SEC = 1.0
_QUERY_STOP_TIMEOUT_SEC = 30
_INPUT_SENTINEL_GRACEFUL_SHUTDOWN_TIMEOUT_SEC = 300
_INPUT_SENTINEL_QUIESCENCE_TIMEOUT_SEC = 60
_INPUT_SENTINEL_PRESTOP_WAIT_SEC = 15


def _log_runtime_event(event: str, **fields) -> None:
    parts = [f"[stream] event={event}"]
    for key, value in fields.items():
        if value is None:
            continue
        parts.append(f"{key}={value}")
    print(" ".join(parts), flush=True)


def _resolve_kafka_packages(configured_packages: str) -> str:
    package_text = str(configured_packages or "").strip()
    if not package_text:
        package_text = "org.apache.spark:spark-sql-kafka-0-10"

    try:
        import pyspark

        pyspark_version = str(pyspark.__version__).strip()
        pyspark_home = Path(pyspark.__file__).resolve().parent
    except Exception:
        pyspark_version = ""
        pyspark_home = None

    scala_suffix = ""
    if pyspark_home is not None:
        spark_sql_jars = sorted((pyspark_home / "jars").glob("spark-sql_*.jar"))
        if spark_sql_jars:
            match = re.search(r"spark-sql(_2\.\d+)-", spark_sql_jars[0].name)
            if match:
                scala_suffix = match.group(1)

    if not pyspark_version:
        return package_text

    resolved_packages: list[str] = []
    for package_entry in package_text.split(","):
        entry = package_entry.strip()
        if not entry:
            continue
        if entry.startswith("org.apache.spark:spark-sql-kafka-0-10"):
            artifact_base = "org.apache.spark:spark-sql-kafka-0-10"
            version_suffix = ""
            if ":" in entry:
                artifact_base, version_suffix = entry.rsplit(":", 1)
            artifact_base = re.sub(r"_2\.\d+$", "", artifact_base)
            if scala_suffix:
                artifact_base = f"{artifact_base}{scala_suffix}"
            entry = f"{artifact_base}:{pyspark_version}"
        resolved_packages.append(entry)
    return ",".join(resolved_packages)


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
    stop_on_input_sentinel: bool = False


def resolve_load_profile(raw_load_profile: str, *, model_name: str, feature_set: str) -> str:
    text = str(raw_load_profile).strip()
    if text:
        return text
    model_text = str(model_name).strip() or "unknown_model"
    feature_text = str(feature_set).strip() or "unknown_feature_set"
    return f"{model_text}:{feature_text}"


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


def _compute_f1_score(precision: float | None, recall: float | None) -> float | None:
    if precision is None or recall is None:
        return None
    denominator = precision + recall
    if denominator <= 0:
        return 0.0
    return float((2.0 * precision * recall) / denominator)


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


def _await_query_termination(query, *, name: str, timeout_sec: float) -> bool:
    try:
        if not query.isActive:
            return True
        return bool(query.awaitTermination(timeout_sec))
    except Exception as exc:
        print(
            f"[warn] query await termination issue name={name}: {exc}",
            file=sys.stderr,
            flush=True,
        )
        return False


def _stop_query_with_soft_wait(
    query,
    *,
    name: str,
    prewait_sec: float,
    stop_timeout_sec: float = _QUERY_STOP_TIMEOUT_SEC,
) -> None:
    try:
        if query is None:
            return
        if _await_query_termination(query, name=name, timeout_sec=max(float(prewait_sec), 0.0)):
            return
        if query.isActive:
            query.stop()
        query.awaitTermination(stop_timeout_sec)
    except Exception as exc:
        print(
            f"[warn] query shutdown issue name={name}: {exc}",
            file=sys.stderr,
            flush=True,
        )


def _latest_query_progress_signature(query) -> str:
    try:
        progress = query.lastProgress
    except Exception:
        progress = None
    if not progress:
        return ""
    if isinstance(progress, dict):
        batch_id = progress.get("batchId")
        timestamp = progress.get("timestamp")
        num_input_rows = progress.get("numInputRows")
        return f"{batch_id}:{timestamp}:{num_input_rows}"
    try:
        batch_id = getattr(progress, "batchId", None)
        timestamp = getattr(progress, "timestamp", None)
        num_input_rows = getattr(progress, "numInputRows", None)
        return f"{batch_id}:{timestamp}:{num_input_rows}"
    except Exception:
        return str(progress)


def _wait_for_query_progress_quiescence(
    queries: list[tuple[object, str]],
    *,
    idle_sec: float,
    timeout_sec: float,
    poll_sec: float = 0.5,
) -> None:
    active_queries = [(query, name) for query, name in queries if query is not None]
    if not active_queries:
        return

    observed = {name: _latest_query_progress_signature(query) for query, name in active_queries}
    last_change_at = time.time()
    deadline = time.time() + max(float(timeout_sec), 0.0)
    while time.time() < deadline:
        any_active = False
        changed = False
        for query, name in active_queries:
            try:
                is_active = bool(query.isActive)
            except Exception:
                is_active = False
            if is_active:
                any_active = True
            signature = _latest_query_progress_signature(query)
            if signature != observed.get(name, ""):
                observed[name] = signature
                changed = True
        if changed:
            last_change_at = time.time()
        if not any_active or (time.time() - last_change_at) >= max(float(idle_sec), 0.0):
            return
        time.sleep(max(float(poll_sec), 0.1))


def _wait_for_queries_inactive(
    queries: list[tuple[object, str]],
    *,
    timeout_sec: float,
    poll_sec: float = 0.5,
) -> bool:
    active_queries = [(query, name) for query, name in queries if query is not None]
    if not active_queries:
        return True

    deadline = time.time() + max(float(timeout_sec), 0.0)
    while time.time() < deadline:
        any_active = False
        for query, _name in active_queries:
            try:
                if query.isActive:
                    any_active = True
                    break
            except Exception:
                continue
        if not any_active:
            return True
        time.sleep(max(float(poll_sec), 0.1))
    return False


def _wait_with_stop_signal(stop_requested: threading.Event, *, timeout_sec: float, poll_sec: float = 0.5) -> bool:
    deadline = time.time() + max(float(timeout_sec), 0.0)
    while time.time() < deadline:
        if stop_requested.is_set():
            return True
        remaining = deadline - time.time()
        time.sleep(min(max(float(poll_sec), 0.1), max(remaining, 0.0)))
    return stop_requested.is_set()


def _drain_query_process_all_available(query, *, name: str) -> None:
    try:
        if not query.isActive:
            return
        query.processAllAvailable()
    except Exception as exc:
        print(
            f"[warn] query drain issue name={name}: {exc}",
            file=sys.stderr,
            flush=True,
        )


def _shutdown_request_seen(run_tag: str) -> bool:
    normalized_run_tag = str(run_tag or "").strip()
    if not normalized_run_tag:
        return False
    return shutdown_request_path(normalized_run_tag).exists()


def _shutdown_after_input_sentinel(
    *,
    data_queries: list[tuple[object, str]],
    sentinel_query,
    run_tag: str,
) -> None:
    active_data_queries = [(query, name) for query, name in data_queries if query is not None]

    for query, name in active_data_queries:
        try:
            if query.isActive:
                _drain_query_process_all_available(query, name=name)
        except Exception:
            continue

    _wait_for_query_progress_quiescence(
        active_data_queries,
        idle_sec=15.0,
        timeout_sec=float(_INPUT_SENTINEL_QUIESCENCE_TIMEOUT_SEC),
        poll_sec=0.5,
    )

    if sentinel_query is not None and sentinel_query.isActive:
        _stop_query_gracefully(sentinel_query, name="input_sentinel")

    for query, name in active_data_queries:
        try:
            if query.isActive:
                _stop_query_with_soft_wait(
                    query,
                    name=name,
                    prewait_sec=float(_INPUT_SENTINEL_PRESTOP_WAIT_SEC),
                )
        except Exception:
            continue

    drained_to_inactive = _wait_for_queries_inactive(
        active_data_queries,
        timeout_sec=float(_QUERY_STOP_TIMEOUT_SEC),
        poll_sec=0.5,
    )
    if not drained_to_inactive:
        print(
            (
                "[warn] data queries still active after sentinel shutdown "
                f"run_tag={run_tag} timeout_sec={_QUERY_STOP_TIMEOUT_SEC}"
            ),
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
    resolved_load_profile = resolve_load_profile(
        options.load_profile,
        model_name=model_name,
        feature_set=options.feature_set,
    )
    clear_shutdown_request(options.run_tag)


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
    model_feature_columns = [str(column_name) for column_name in manifest.get("feature_columns", [])]
    if not model_feature_columns:
        raise ValueError("feature_columns missing in feature manifest")
    active_feature_columns = model_feature_columns


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
        .config("spark.python.worker.reuse", "true")
        .config("spark.hadoop.io.native.lib.available", "false")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.LocalFs")
        .config("spark.hadoop.fs.file.impl.disable.cache", "true")
    )

    kafka_packages = _resolve_kafka_packages(
        spark_config.kafka_packages or "org.apache.spark:spark-sql-kafka-0-10_2.13"
    )
    if kafka_packages:
        spark = spark.config("spark.jars.packages", kafka_packages)

    spark = spark.getOrCreate()
    stream_started_epoch_ms = int(time.time() * 1000)
    prime_process_metrics_probe()


    # ── stream ──────────────────────────────────────────
    raw_schema = build_raw_schema(model_feature_columns)
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
    parsed_stream, control_stream = split_control_and_data_records(parsed_stream)

    prepared_stream = prepare_feature_columns(
        parsed_stream,
        full_feature_columns=model_feature_columns,
        active_feature_columns=active_feature_columns,
        fill_values=fill_values,
    )
    prepared_stream = add_event_timing_columns(
        prepared_stream,
        watermark_delay_sec=watermark_delay_sec,
        drop_late_events=bool(options.drop_late_events),
    )

    score_udf = make_score_udf(str(model_path), model_feature_columns, fill_values)
    prewarm_elapsed_ms: float | None = None
    try:
        prewarm_elapsed_ms = prewarm_score_udf_model(
            spark,
            score_udf=score_udf,
            feature_columns=model_feature_columns,
            fill_values=fill_values,
        )
        _log_runtime_event(
            "model_prewarm_done",
            run_tag=options.run_tag,
            model=model_name,
            feature_set=options.feature_set,
            prewarm_elapsed_ms=f"{prewarm_elapsed_ms:.3f}",
        )
    except Exception as exc:
        _log_runtime_event(
            "model_prewarm_failed",
            run_tag=options.run_tag,
            model=model_name,
            feature_set=options.feature_set,
            error=type(exc).__name__,
        )
    scored_stream = add_prediction_columns(
        prepared_stream,
        score_udf=score_udf,
        feature_columns=model_feature_columns,
        threshold=threshold,
        model_name=model_name,
        feature_set=options.feature_set,
        run_tag=options.run_tag,
    )

    scored_stream = add_source_latency_columns(scored_stream, source_mode=latency_config.source_to_ingest_mode)
    scored_stream = add_processing_latency_columns(
        scored_stream,
        stream_started_epoch_ms=stream_started_epoch_ms,
    )
    prediction_payload = build_prediction_payload(scored_stream)


    # ── checkpoints ─────────────────────────────────────
    run_suffix = safe_tag(options.run_tag) \
        if options.run_tag.strip() \
        else safe_tag(f"{model_name}_{options.feature_set}")

    kafka_checkpoint = checkpoint_root / run_suffix / "kafka"
    parquet_checkpoint = checkpoint_root / run_suffix / "parquet"
    metrics_checkpoint = checkpoint_root / run_suffix / "metrics"
    sentinel_checkpoint = checkpoint_root / run_suffix / "sentinel"
    artifact_output = prediction_artifact_dir / run_suffix

    if options.reset_checkpoint:
        for path in (kafka_checkpoint, parquet_checkpoint, metrics_checkpoint, sentinel_checkpoint, artifact_output):
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
            precision = safe_ratio_or_none(tp, tp + fp)
            recall = safe_ratio_or_none(tp, tp + fn)
            f1 = _compute_f1_score(precision, recall)
            fpr = safe_ratio_or_none(fp, fp + tn)
            fnr = safe_ratio_or_none(fn, fn + tp)
            batch_wall_seconds = max(time.perf_counter() - batch_start, 1e-9)
            batch_wall_ms = batch_wall_seconds * 1000.0
            source_p50_ms = float(stat["source_p50_ms"] or 0.0)
            source_p95_ms = float(stat["source_p95_ms"] or 0.0)
            source_p99_ms = float(stat["source_p99_ms"] or 0.0)

            payload = {
                "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "ts_epoch_ms": metric_ts_epoch_ms,
                "batch_id": int(batch_id),
                "run_tag": options.run_tag,
                "load_profile": resolved_load_profile,
                "model_name": model_name,
                "feature_set": options.feature_set,
                "metric_sources": {
                    "latency_detection": "spark_foreachBatch_aggregation",
                    "kafka_lag": "kafka_topic_high_watermark_minus_batch_offsets",
                    "system_utilization": "driver_process_and_spark_executor_memory",
                },
                "metric_warnings": sorted(
                    {
                        str(warning).strip()
                        for warning in [
                            *((proc_metrics.get("metric_warnings") or [])),
                            *((executor_metrics.get("metric_warnings") or [])),
                            *((kafka_lag.get("metric_warnings") or [])),
                            *(
                                ["load_profile_missing_fallback_used"]
                                if not str(options.load_profile).strip()
                                else []
                            ),
                            *(
                                ["latency_excludes_pre_stream_queue_dwell"]
                                if options.available_now
                                else []
                            ),
                            *(
                                ["model_score_udf_prewarm_failed"]
                                if prewarm_elapsed_ms is None
                                else []
                            ),
                        ]
                        if str(warning).strip()
                    }
                ),
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
                        "p50": source_p50_ms,
                        "p95": source_p95_ms,
                        "p99": source_p99_ms,
                    },
                    "processing": {
                        "semantic": "legacy_alias_for_batch_wall_time",
                        "p50": batch_wall_ms,
                        "p95": batch_wall_ms,
                        "p99": batch_wall_ms,
                    },
                    "ingest_to_emit": {
                        "p50": batch_wall_ms,
                        "p95": batch_wall_ms,
                        "p99": batch_wall_ms,
                    },
                    "end_to_end": {
                        "semantic": "legacy_alias_for_source_to_ingest_plus_batch_wall_time",
                        "p50": source_p50_ms + batch_wall_ms,
                        "p95": source_p95_ms + batch_wall_ms,
                        "p99": source_p99_ms + batch_wall_ms,
                    },
                    "source_to_emit": {
                        "p50": source_p50_ms + batch_wall_ms,
                        "p95": source_p95_ms + batch_wall_ms,
                        "p99": source_p99_ms + batch_wall_ms,
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
                    "precision": float(precision) if precision is not None else None,
                    "recall": float(recall) if recall is not None else None,
                    "f1": float(f1) if f1 is not None else None,
                    "fpr": float(fpr) if fpr is not None else None,
                    "fnr": float(fnr) if fnr is not None else None,
                },
                "avg_prediction_score": float(stat["avg_prediction_score"] or 0.0),
                "attack_ratio": float(stat["attack_ratio"] or 0.0),
            }

            payload["batch_wall_ms"] = batch_wall_ms
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
                fpr=(f"{fpr:.6f}" if fpr is not None else "n/a"),
                fnr=(f"{fnr:.6f}" if fnr is not None else "n/a"),
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

    sentinel_seen = threading.Event()

    def mark_input_sentinel(batch_df, batch_id: int) -> None:
        row_count = int(batch_df.count())
        if row_count <= 0:
            return
        sentinel_seen.set()
        _log_runtime_event(
            "input_sentinel_seen",
            run_tag=options.run_tag,
            batch_id=batch_id,
            rows=row_count,
        )

    sentinel_writer = None
    if options.stop_on_input_sentinel:
        sentinel_writer = (
            control_stream.filter(control_stream.control_type == "input_sentinel")
            .writeStream
            .foreachBatch(mark_input_sentinel)
            .option("checkpointLocation", str(sentinel_checkpoint))
            .queryName(f"ids_input_sentinel_{model_name}")
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
    sentinel_query = None
    if sentinel_writer is not None:
        sentinel_query = apply_trigger(
            sentinel_writer,
            available_now=options.available_now,
            trigger_interval=trigger_interval,
        ).start()
    active_queries = [kafka_query, parquet_query, metrics_query]
    if sentinel_query is not None:
        active_queries.append(sentinel_query)
    stop_requested = threading.Event()

    def _handle_stop_signal(signum, _frame) -> None:
        stop_requested.set()
        _log_runtime_event(
            "shutdown_signal_received",
            run_tag=options.run_tag,
            signal=signum,
        )

    for handled_signal in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(handled_signal, _handle_stop_signal)
        except Exception:
            continue

    print(
        f"Started structured streaming model={model_name} input_topic={input_topic} output_topic={output_topic} "
        f"metrics_topic={metrics_topic} feature_set={options.feature_set} run_tag={options.run_tag} "
        f"max_offsets={max_offsets_per_trigger} shuffle_partitions={shuffle_partitions} trigger={trigger_interval} "
        f"watermark_delay_sec={watermark_delay_sec} drop_late_events={bool(options.drop_late_events)} "
        f"available_now={options.available_now} stop_on_input_sentinel={bool(options.stop_on_input_sentinel)}",
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
        stop_on_input_sentinel=options.stop_on_input_sentinel,
    )

    if options.available_now:
        for query in active_queries:
            while query.isActive and not stop_requested.is_set() and not _shutdown_request_seen(options.run_tag):
                query.awaitTermination(1)
        if stop_requested.is_set() or _shutdown_request_seen(options.run_tag):
            for query, name in [
                (sentinel_query, "input_sentinel"),
                (kafka_query, "predictions_kafka"),
                (parquet_query, "predictions_parquet"),
                (metrics_query, "metrics"),
            ]:
                if query is not None:
                    _stop_query_gracefully(query, name=name)
    elif options.stop_on_input_sentinel:
        deadline = (time.time() + options.run_seconds) if options.run_seconds and options.run_seconds > 0 else None
        stop_reason = "queries_inactive"
        while True:
            if stop_requested.is_set():
                stop_reason = "signal"
                break
            if _shutdown_request_seen(options.run_tag):
                stop_reason = "external_request"
                break
            if sentinel_seen.is_set():
                stop_reason = "input_sentinel"
                break
            if deadline is not None and time.time() >= deadline:
                stop_reason = "watchdog_timeout"
                break
            if not any(query.isActive for query in (kafka_query, parquet_query, metrics_query)):
                stop_reason = "queries_inactive"
                break
            time.sleep(0.5)
        _log_runtime_event(
            "stop_condition_met",
            run_tag=options.run_tag,
            reason=stop_reason,
            sentinel_seen=sentinel_seen.is_set(),
        )
        if stop_reason == "watchdog_timeout":
            print(
                f"[warn] watchdog timeout before input sentinel run_tag={options.run_tag}",
                file=sys.stderr,
                flush=True,
            )
        data_queries = [
            (kafka_query, "predictions_kafka"),
            (parquet_query, "predictions_parquet"),
            (metrics_query, "metrics"),
        ]
        managed_queries = list(data_queries)
        if sentinel_query is not None:
            managed_queries.insert(0, (sentinel_query, "input_sentinel"))
        if stop_reason == "input_sentinel":
            _shutdown_after_input_sentinel(
                data_queries=data_queries,
                sentinel_query=sentinel_query,
                run_tag=options.run_tag,
            )
        else:
            if sentinel_query is not None:
                _stop_query_gracefully(sentinel_query, name="input_sentinel")
        for query, name in managed_queries:
            if query is not None and query.isActive:
                _stop_query_with_soft_wait(
                    query,
                    name=name,
                    prewait_sec=(
                        float(_INPUT_SENTINEL_GRACEFUL_SHUTDOWN_TIMEOUT_SEC)
                        if stop_reason == "input_sentinel"
                        else 15.0
                    ),
                )
    elif options.run_seconds and options.run_seconds > 0:
        deadline = time.time() + options.run_seconds
        stop_reason = "watchdog_timeout"
        while time.time() < deadline:
            if stop_requested.is_set():
                stop_reason = "signal"
                break
            if _shutdown_request_seen(options.run_tag):
                stop_reason = "external_request"
                break
            time.sleep(0.5)
        interrupted = stop_reason != "watchdog_timeout"
        if interrupted:
            _log_runtime_event(
                "stop_condition_met",
                run_tag=options.run_tag,
                reason=stop_reason,
                sentinel_seen=sentinel_seen.is_set(),
            )
        _stop_query_gracefully(kafka_query, name="predictions_kafka")
        _stop_query_gracefully(parquet_query, name="predictions_parquet")
        _stop_query_gracefully(metrics_query, name="metrics")
    else:
        while not stop_requested.is_set():
            if _shutdown_request_seen(options.run_tag):
                break
            if not any(query.isActive for query in active_queries):
                break
            spark.streams.awaitAnyTermination(1)
        if stop_requested.is_set() or _shutdown_request_seen(options.run_tag):
            _log_runtime_event(
                "stop_condition_met",
                run_tag=options.run_tag,
                reason=("signal" if stop_requested.is_set() else "external_request"),
                sentinel_seen=sentinel_seen.is_set(),
            )
            for query, name in [
                (sentinel_query, "input_sentinel"),
                (kafka_query, "predictions_kafka"),
                (parquet_query, "predictions_parquet"),
                (metrics_query, "metrics"),
            ]:
                if query is not None:
                    _stop_query_gracefully(query, name=name)

    _publish_run_completion_metric(
        bootstrap_servers=bootstrap_servers,
        metrics_topic=metrics_topic,
        run_tag=options.run_tag,
        model_name=model_name,
        feature_set=options.feature_set,
        load_profile=resolved_load_profile,
    )
    _log_runtime_event(
        "job_stop",
        run_tag=options.run_tag,
        model=model_name,
        feature_set=options.feature_set,
    )
    clear_shutdown_request(options.run_tag)

    spark.stop()
    return 0
