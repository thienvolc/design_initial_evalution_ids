from __future__ import annotations

import json
import math
import os
import shutil
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import reduce
from pathlib import Path
from typing import TYPE_CHECKING, Callable, cast

from ids_platform.common.paths import PROJECT_ROOT, resolve_project_path
from ids_platform.streaming.core.artifacts import (
    load_thresholds,
    resolve_feature_set_path,
    resolve_model_artifact_path,
)
from ids_platform.streaming.core.config import load_batch_benchmark_app_config
from ids_platform.streaming.benchmark.runtime.benchmark import (
    SOURCE_LATENCY_MODES,
    build_active_model_specs,
    first_existing,
    make_struct_score_udf,
    metrics_from_counts,
    unique_preserve,
)
from ids_platform.offline.config import load_json

if TYPE_CHECKING:
    from pyspark.sql.column import Column


@dataclass(frozen=True)
class PandasUdfBenchmarkOptions:
    config_path: str = "configs/streaming/detect.yaml"
    input_parquet: str | None = None
    max_rows: int = 0
    batch_size: int = 50000
    models: tuple[str, ...] | None = None
    feature_set: str = "full"
    output_parquet_dir: str | None = None
    summary_json: str | None = None
    run_tag: str = ""


def _quantile_triplet(values) -> tuple[float, float, float]:
    if not values:
        return 0.0, 0.0, 0.0
    padded_values = list(values[:3])
    while len(padded_values) < 3:
        padded_values.append(0.0)
    return float(padded_values[0]), float(padded_values[1]), float(padded_values[2])


def _coerce_threshold(value: object) -> float:
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float, str)):
        return float(value)
    raise TypeError(f"Unsupported threshold value: {value!r}")


def run_pandas_udf_benchmark(options: PandasUdfBenchmarkOptions) -> int:
    run_started = time.perf_counter()


    # ── configs ─────────────────────────────────────────
    app_config = load_batch_benchmark_app_config(options.config_path)
    spark_config = app_config.runtime
    latency_config = app_config.latency
    paths_config = app_config.paths
    model_configs = app_config.models

    source_latency_mode = latency_config.source_to_ingest_mode
    if source_latency_mode not in SOURCE_LATENCY_MODES:
        raise ValueError(
            f"Unsupported source_to_ingest_mode={source_latency_mode}. "
            f"Expected one of {sorted(SOURCE_LATENCY_MODES)}"
        )

    event_time_candidates = [
        str(column_name)
        for column_name in (latency_config.event_time_columns or ["timestamp", "Timestamp"])
        if str(column_name).strip()
    ]
    source_time_candidates = [
        str(column_name)
        for column_name in (
            latency_config.source_time_columns
            or ["source_ingest_ts", "ingest_ts", "kafka_ingest_ts", "producer_ts"]
        )
        if str(column_name).strip()
    ]


    # ── paths ───────────────────────────────────────────
    manifest_path = resolve_feature_set_path(paths_config.raw, "feature_manifest", options.feature_set)
    valid_metrics_csv = resolve_feature_set_path(paths_config.raw, "valid_metrics_csv", options.feature_set)
    input_parquet = resolve_project_path(str(options.input_parquet or paths_config.input_parquet))
    output_parquet_dir = resolve_project_path(
        str(options.output_parquet_dir or paths_config.output_parquet_dir or "artifacts/streaming/benchmark/detect_output")
    )


    # ── features ────────────────────────────────────────
    manifest = load_json(manifest_path)
    model_feature_columns = [str(column_name) for column_name in manifest.get("feature_columns", [])]
    if not model_feature_columns:
        raise ValueError("feature_columns is missing in feature manifest")
    active_feature_columns = model_feature_columns


    # ── impute ──────────────────────────────────────────
    fill_values = {str(key): float(value) for key, value in (manifest.get("imputer_fill_values") or {}).items()}
    thresholds = load_thresholds(valid_metrics_csv)


    # ── runtime ─────────────────────────────────────────
    selected_models = set(options.models) if options.models else None
    active_models = build_active_model_specs(
        model_configs,
        selected_names=selected_models,
        thresholds=thresholds,
        feature_set=options.feature_set,
        resolve_model_path=resolve_model_artifact_path,
    )

    if not active_models:
        raise ValueError("No active models selected")

    try:
        from pyspark.sql import SparkSession, functions as F
        from pyspark.storagelevel import StorageLevel
    except ImportError as exc:
        raise RuntimeError("pyspark is required for pandas UDF execution") from exc

    python_exec = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_exec
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_exec
    os.environ.setdefault("SPARK_LOCAL_IP", spark_config.driver_host)
    os.environ.setdefault("OMP_NUM_THREADS", "1")
    os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
    os.environ.setdefault("MKL_NUM_THREADS", "1")
    os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")

    arrow_batch_size = int(options.batch_size) \
        if options.batch_size > 0 \
        else spark_config.arrow_max_records_per_batch

    spark = (
        SparkSession.builder
        .appName(spark_config.app_name)
        .master(spark_config.master)
        .config("spark.driver.host", spark_config.driver_host)
        .config("spark.driver.bindAddress", spark_config.driver_bind_address)
        .config("spark.sql.execution.arrow.pyspark.enabled", str(spark_config.arrow_enabled).lower())
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", str(max(1, arrow_batch_size)))
        .config("spark.sql.shuffle.partitions", str(spark_config.shuffle_partitions))
        .config("spark.pyspark.python", python_exec)
        .config("spark.pyspark.driver.python", python_exec)
        .config("spark.executorEnv.PYSPARK_PYTHON", python_exec)
        .config("spark.executorEnv.OMP_NUM_THREADS", "1")
        .config("spark.executorEnv.OPENBLAS_NUM_THREADS", "1")
        .config("spark.executorEnv.MKL_NUM_THREADS", "1")
        .config("spark.executorEnv.NUMEXPR_NUM_THREADS", "1")
        .config("spark.python.worker.faulthandler.enabled", "true")
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
        .getOrCreate()
    )

    run_wall_start = datetime.now(timezone.utc)

    keep_meta_candidates = unique_preserve(
        ["label_binary", "label", "Label", *event_time_candidates, *source_time_candidates]
    )
    requested_cols = unique_preserve([*active_feature_columns, *keep_meta_candidates])


    # ── input ───────────────────────────────────────────
    read_started = time.perf_counter()

    dataframe = spark.read.parquet(str(input_parquet))
    existing_columns = set(dataframe.columns)
    projected_columns = [column_name for column_name in requested_cols if column_name in existing_columns]
    if projected_columns:
        dataframe = dataframe.select(*projected_columns)
    if options.max_rows and options.max_rows > 0:
        dataframe = dataframe.limit(int(options.max_rows))
    read_seconds = time.perf_counter() - read_started

    if "label_binary" in dataframe.columns:
        truth_column_name = "label_binary"
    elif "Label" in dataframe.columns:
        truth_column_name = "Label"
    elif "label" in dataframe.columns:
        truth_column_name = "label"
    else:
        truth_column_name = None

    def build_truth_column():
        if truth_column_name == "label_binary":
            return F.col("label_binary").cast("int")
        if truth_column_name == "Label":
            return F.when(F.lower(F.trim(F.col("Label"))) == F.lit("benign"), F.lit(0)).otherwise(F.lit(1))
        if truth_column_name == "label":
            return F.when(F.lower(F.trim(F.col("label"))) == F.lit("benign"), F.lit(0)).otherwise(F.lit(1))
        return None

    transform_started = time.perf_counter()
    existing_dataframe_columns = set(dataframe.columns)
    feature_projection = []
    for column_name in model_feature_columns:
        if column_name in existing_dataframe_columns:
            feature_projection.append(F.col(column_name).cast("double").alias(column_name))
        else:
            feature_projection.append(F.lit(None).cast("double").alias(column_name))

    keep_meta = [column_name for column_name in keep_meta_candidates if column_name in dataframe.columns]
    base_projection = [
        F.monotonically_increasing_id().alias("row_id"),
        *feature_projection,
        *[F.col(column_name) for column_name in keep_meta],
        F.current_timestamp().alias("_ingest_ts"),
    ]
    base_df = dataframe.select(*base_projection)
    base_df = base_df.select(
        *[F.col(column_name) for column_name in base_df.columns],
        F.struct(*[F.col(column_name).alias(column_name) for column_name in model_feature_columns]).alias("_features"),
    )

    available_columns = set(base_df.columns)
    event_time_col = first_existing(event_time_candidates, available_columns)
    source_time_col = first_existing(source_time_candidates, available_columns)

    latency_reference = "disabled"
    effective_source_mode = source_latency_mode

    if source_latency_mode == "disabled":
        base_df = base_df.withColumn("_source_to_ingest_ms", F.lit(0.0).cast("double"))
    elif source_time_col is not None and source_latency_mode in {"auto", "source_timestamp"}:
        source_ts = F.col(source_time_col).cast("timestamp")
        base_df = base_df.withColumn(
            "_source_to_ingest_ms",
            F.when(F.isnull(source_ts), F.lit(0.0))
            .otherwise(
                F.greatest(
                    F.lit(0.0),
                    (F.unix_millis("_ingest_ts") - F.unix_millis(source_ts)).cast("double"),
                )
            )
            .cast("double"),
        )
        latency_reference = "source_timestamp"
        effective_source_mode = "source_timestamp"
    elif event_time_col is not None and source_latency_mode in {"auto", "replay_relative"}:
        base_df = base_df.withColumn("_event_ts", F.col(event_time_col).cast("timestamp"))
        anchor = base_df.agg(
            F.min(F.unix_millis("_event_ts")).alias("event_anchor_ms"),
            F.min(F.unix_millis("_ingest_ts")).alias("ingest_anchor_ms"),
        ).collect()[0]
        event_anchor_ms = anchor["event_anchor_ms"]
        ingest_anchor_ms = anchor["ingest_anchor_ms"]

        if event_anchor_ms is not None and ingest_anchor_ms is not None:
            base_df = base_df.withColumn(
                "_source_to_ingest_ms",
                F.greatest(
                    F.lit(0.0),
                    (F.unix_millis("_ingest_ts") - F.lit(int(ingest_anchor_ms)))
                    - (F.unix_millis("_event_ts") - F.lit(int(event_anchor_ms))),
                ).cast("double"),
            )
            latency_reference = "replay_relative_event_time"
            effective_source_mode = "replay_relative"
        else:
            base_df = base_df.withColumn("_source_to_ingest_ms", F.lit(0.0).cast("double"))
            latency_reference = "replay_relative_no_event_values"
            effective_source_mode = "replay_relative"
    elif source_latency_mode == "source_timestamp":
        raise ValueError(
            f"source_to_ingest_mode=source_timestamp requires one of source_time_columns={source_time_candidates} "
            f"in input parquet"
        )
    elif source_latency_mode == "replay_relative":
        raise ValueError(
            f"source_to_ingest_mode=replay_relative requires one of event_time_columns={event_time_candidates} "
            f"in input parquet"
        )
    else:
        base_df = base_df.withColumn("_source_to_ingest_ms", F.lit(0.0).cast("double"))
        latency_reference = "disabled_no_timestamp_columns"
        effective_source_mode = "disabled"

    rows_scored = int(base_df.count())
    target_partitions = max(1, math.ceil(rows_scored / max(options.batch_size, 1)))
    base_df = base_df.repartition(target_partitions)
    base_df = base_df.persist(StorageLevel.DISK_ONLY)
    transform_seconds = time.perf_counter() - transform_started

    scored_base_df = base_df.select("row_id", "_source_to_ingest_ms", *keep_meta)
    score_input_columns = ["row_id", "_features"]
    if truth_column_name is not None:
        score_input_columns.append(truth_column_name)
    score_input_df = base_df.select(*score_input_columns)
    models_summary: dict[str, dict] = {}
    inference_total_seconds = 0.0
    model_processing_columns: list[str] = []
    model_output_frames = []
    persisted_model_frames = []


    # ── scoring ─────────────────────────────────────────
    for model_spec in active_models:
        model_name = str(model_spec["name"])
        threshold = _coerce_threshold(model_spec["threshold"])
        score_udf = make_struct_score_udf(str(model_spec["joblib_path"]), model_feature_columns, fill_values)
        score_udf_column_fn = cast(Callable[["Column"], "Column"], score_udf)
        score_output_column = score_udf_column_fn(F.col("_features"))
        prediction_condition = cast("Column", F.col(f"score_{model_name}") >= F.lit(threshold))

        model_started = time.perf_counter()
        model_base_df = score_input_df
        model_df = (
            model_base_df.select(
                *[F.col(column_name) for column_name in model_base_df.columns],
                score_output_column.alias(f"_score_output_{model_name}"),
            )
            .select(
                "row_id",
                *[F.col(column_name) for column_name in keep_meta],
                F.col(f"_score_output_{model_name}.score").alias(f"score_{model_name}"),
                F.col(f"_score_output_{model_name}.processing_ms").alias(f"processing_ms_{model_name}"),
            )
            .select(
                "row_id",
                *[F.col(column_name) for column_name in keep_meta],
                F.col(f"score_{model_name}"),
                F.col(f"processing_ms_{model_name}"),
                F.when(prediction_condition, F.lit(1)).otherwise(F.lit(0)).alias(f"pred_{model_name}"),
            )
        )

        model_df = model_df.persist(StorageLevel.DISK_ONLY)
        persisted_model_frames.append(model_df)
        inference_seconds = time.perf_counter() - model_started
        inference_total_seconds += inference_seconds
        model_processing_columns.append(f"processing_ms_{model_name}")

        aggregate_expressions = [F.sum(F.when(F.col(f"pred_{model_name}") == 1, 1).otherwise(0)).alias("pred_attack")]
        truth_column = build_truth_column()
        if truth_column is not None:
            aggregate_expressions.extend(
                [
                    F.sum(F.when((truth_column == 0) & (F.col(f"pred_{model_name}") == 0), 1).otherwise(0)).alias("tn"),
                    F.sum(F.when((truth_column == 0) & (F.col(f"pred_{model_name}") == 1), 1).otherwise(0)).alias("fp"),
                    F.sum(F.when((truth_column == 1) & (F.col(f"pred_{model_name}") == 0), 1).otherwise(0)).alias("fn"),
                    F.sum(F.when((truth_column == 1) & (F.col(f"pred_{model_name}") == 1), 1).otherwise(0)).alias("tp"),
                ]
            )

        aggregate_row = model_df.agg(*aggregate_expressions).collect()[0]
        aggregate_values = aggregate_row.asDict()
        pred_attack = int(aggregate_values.get("pred_attack") or 0)
        tn = int(aggregate_values.get("tn") or 0)
        fp = int(aggregate_values.get("fp") or 0)
        fn = int(aggregate_values.get("fn") or 0)
        tp = int(aggregate_values.get("tp") or 0)

        model_metrics = metrics_from_counts(tn, fp, fn, tp) if truth_column is not None else {}
        models_summary[model_name] = {
            "rows": rows_scored,
            "attack_preds": pred_attack,
            "attack_rate_pct": round((100.0 * pred_attack / rows_scored) if rows_scored else 0.0, 4),
            "confusion_matrix": [[tn, fp], [fn, tp]],
            "metrics": model_metrics,
            "inference_seconds": round(float(inference_seconds), 6),
            "threshold": threshold,
        }

        model_output_frames.append(
            model_df.select(
                "row_id",
                F.col(f"score_{model_name}"),
                F.col(f"pred_{model_name}"),
                F.col(f"processing_ms_{model_name}"),
            )
        )

    if model_output_frames:
        scored_df = reduce(
            lambda left_df, right_df: left_df.join(right_df, on="row_id", how="inner"),
            model_output_frames,
            scored_base_df,
        )
    else:
        scored_df = scored_base_df

    if model_processing_columns:
        if len(model_processing_columns) == 1:
            processing_expression = F.coalesce(F.col(model_processing_columns[0]), F.lit(0.0))
        else:
            processing_expression = F.lit(0.0)
            for column_name in model_processing_columns:
                processing_expression = processing_expression + F.coalesce(F.col(column_name), F.lit(0.0))
    else:
        processing_expression = F.lit(0.0)

    scored_df = scored_df.withColumn("_processing_ms", processing_expression.cast("double"))
    scored_df = scored_df.withColumn("_end_to_end_ms", (F.col("_source_to_ingest_ms") + F.col("_processing_ms")).cast("double"))

    if rows_scored > 0:
        raw_quantiles = scored_df.approxQuantile(
            ["_source_to_ingest_ms", "_processing_ms", "_end_to_end_ms"],
            [0.5, 0.95, 0.99],
            0.0,
        )
    else:
        raw_quantiles = [[], [], []]
    source_quantiles = _quantile_triplet(raw_quantiles[0] if len(raw_quantiles) > 0 else [])
    processing_quantiles = _quantile_triplet(raw_quantiles[1] if len(raw_quantiles) > 1 else [])
    end_to_end_quantiles = _quantile_triplet(raw_quantiles[2] if len(raw_quantiles) > 2 else [])

    sink_started = time.perf_counter()
    if output_parquet_dir.exists():
        shutil.rmtree(output_parquet_dir)
    output_df = scored_df.drop("_source_to_ingest_ms", "_processing_ms", "_end_to_end_ms", *model_processing_columns)
    output_df.write.mode("overwrite").parquet(str(output_parquet_dir))
    sink_seconds = time.perf_counter() - sink_started

    scored_df.unpersist()
    for model_df in persisted_model_frames:
        model_df.unpersist()
    base_df.unpersist()

    parts_written = len(list(output_parquet_dir.glob("part-*.parquet")))
    total_seconds = time.perf_counter() - run_started
    run_wall_end = datetime.now(timezone.utc)

    summary = {
        "backend": "joblib_pandas_udf",
        "run_tag": options.run_tag,
        "run_started_utc": run_wall_start.isoformat(timespec="seconds"),
        "run_finished_utc": run_wall_end.isoformat(timespec="seconds"),
        "source": {
            "input_parquet": str(input_parquet),
            "max_rows": options.max_rows if options.max_rows and options.max_rows > 0 else None,
            "batch_size": options.batch_size,
            "rows_scored": rows_scored,
            "parts_written": parts_written,
            "output_parquet_dir": str(output_parquet_dir),
        },
        "profile": {
            "feature_set": options.feature_set,
            "active_feature_count": len(active_feature_columns),
            "model_input_feature_count": len(model_feature_columns),
            "sampling_policy": str((manifest.get("sampling") or {}).get("strategy", "unknown")),
            "models": [model["name"] for model in active_models],
        },
        "timing_seconds": {
            "read": round(float(read_seconds), 6),
            "transform": round(float(transform_seconds), 6),
            "inference": round(float(inference_total_seconds), 6),
            "sink": round(float(sink_seconds), 6),
            "total": round(float(total_seconds), 6),
        },
        "latency_ms": {
            "reference": latency_reference,
            "source_to_ingest_mode": effective_source_mode,
            "source_time_column": source_time_col,
            "event_time_column": event_time_col,
            "source_to_ingest": {
                "p50": round(float(source_quantiles[0]), 6),
                "p95": round(float(source_quantiles[1]), 6),
                "p99": round(float(source_quantiles[2]), 6),
            },
            "processing": {
                "p50": round(float(processing_quantiles[0]), 6),
                "p95": round(float(processing_quantiles[1]), 6),
                "p99": round(float(processing_quantiles[2]), 6),
            },
            "end_to_end": {
                "p50": round(float(end_to_end_quantiles[0]), 6),
                "p95": round(float(end_to_end_quantiles[1]), 6),
                "p99": round(float(end_to_end_quantiles[2]), 6),
            },
            "reservoir_size": {
                "source_to_ingest": rows_scored,
                "processing": rows_scored,
                "end_to_end": rows_scored,
            },
        },
        "models": models_summary,
    }


    # ── stop─────────────────────────────────────────────
    summary_path = Path(options.summary_json) if options.summary_json else (output_parquet_dir / "run_summary.json")
    if not summary_path.is_absolute():
        summary_path = PROJECT_ROOT / summary_path
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    with summary_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, ensure_ascii=False, indent=2)

    print(f"Saved predictions to: {output_parquet_dir} (parts={parts_written})")
    print(f"Saved run summary to: {summary_path}")

    spark.stop()
    return 0
