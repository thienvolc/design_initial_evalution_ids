from __future__ import annotations

from pathlib import Path

from ids_platform.streaming.runtime.query import apply_trigger

PREDICTION_RESPONSE_COLUMNS = (
    "flow_id",
    "model_name",
    "feature_set",
    "run_tag",
    "benchmark_phase",
    "prediction_label",
    "prediction_score",
    "threshold_used",
    "kafka_partition",
    "kafka_offset",
    "event_time_ts",
    "ingest_time",
    "emit_time",
    "source_to_ingest_ms",
    "processing_ms",
    "end_to_end_ms",
    "event_lateness_ms",
    "is_late_event",
)


def build_prediction_kafka_writer(
    scored_stream,
    *,
    bootstrap_servers: str,
    prediction_topic: str,
    checkpoint_path: Path,
    model_name: str,
):
    from pyspark.sql import functions as F

    return (
        scored_stream
        .select(
            F.col("flow_id").cast("string").alias("key"),
            F.to_json(F.struct(*[F.col(column_name) for column_name in PREDICTION_RESPONSE_COLUMNS])).alias("value"),
        )
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("topic", prediction_topic)
        .option("checkpointLocation", str(checkpoint_path))
        .outputMode("append")
        .queryName(f"ids_prediction_response_{model_name}")
    )


def start_runtime_queries(
    *,
    prediction_writer,
    trigger_interval: str,
) -> tuple[object, list[object]]:
    prediction_query = apply_trigger(
        prediction_writer,
        trigger_interval=trigger_interval,
    ).start()

    active_queries = [prediction_query]

    return prediction_query, active_queries


def group_runtime_queries(
    *,
    prediction_query,
) -> tuple[list[tuple[object, str]], list[tuple[object, str]]]:
    data_queries = [
        (prediction_query, "prediction_response"),
    ]
    all_queries = list(data_queries)
    return data_queries, all_queries
