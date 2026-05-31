from __future__ import annotations

from pathlib import Path
from typing import Callable

from ids_platform.streaming.runtime.query import apply_trigger

PREDICTION_PARQUET_COLUMNS = (
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


def build_prediction_parquet_writer(
    scored_stream,
    *,
    output_path: Path,
    checkpoint_path: Path,
    model_name: str,
):
    return (
        scored_stream.select(*PREDICTION_PARQUET_COLUMNS)
        .writeStream
        .format("parquet")
        .option("path", str(output_path))
        .option("checkpointLocation", str(checkpoint_path))
        .outputMode("append")
        .queryName(f"ids_predictions_parquet_{model_name}")
    )


def build_input_sentinel_writer(
    control_stream,
    *,
    checkpoint_path: Path,
    model_name: str,
    mark_input_sentinel: Callable,
):
    return (
        control_stream.filter(control_stream.control_type == "input_sentinel")
        .writeStream
        .foreachBatch(mark_input_sentinel)
        .option("checkpointLocation", str(checkpoint_path))
        .queryName(f"ids_input_sentinel_{model_name}")
    )


def build_metrics_writer(
    scored_stream,
    *,
    write_metrics_batch: Callable,
    checkpoint_path: Path,
    model_name: str,
):
    return (
        scored_stream.writeStream
        .foreachBatch(write_metrics_batch)
        .option("checkpointLocation", str(checkpoint_path))
        .queryName(f"ids_metrics_{model_name}")
    )


def start_runtime_queries(
    *,
    parquet_writer,
    metrics_writer,
    sentinel_writer,
    trigger_interval: str,
) -> tuple[object, object | None, object | None, list[object]]:
    parquet_query = apply_trigger(
        parquet_writer,
        trigger_interval=trigger_interval,
    ).start()
    metrics_query = None
    if metrics_writer is not None:
        metrics_query = apply_trigger(
            metrics_writer,
            trigger_interval=trigger_interval,
        ).start()

    sentinel_query = None
    if sentinel_writer is not None:
        sentinel_query = apply_trigger(
            sentinel_writer,
            trigger_interval=trigger_interval,
        ).start()

    active_queries = [parquet_query]
    if metrics_query is not None:
        active_queries.append(metrics_query)
    if sentinel_query is not None:
        active_queries.append(sentinel_query)

    return parquet_query, metrics_query, sentinel_query, active_queries


def group_runtime_queries(
    *,
    parquet_query,
    metrics_query,
    sentinel_query,
) -> tuple[list[tuple[object, str]], list[tuple[object, str]]]:
    data_queries = [
        (parquet_query, "predictions_parquet"),
    ]
    if metrics_query is not None:
        data_queries.append((metrics_query, "metrics"))
    all_queries = (
        [(sentinel_query, "input_sentinel")] + data_queries
        if sentinel_query is not None
        else list(data_queries)
    )
    return data_queries, all_queries
