from __future__ import annotations


def build_raw_schema(full_feature_columns: list[str]):
    from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

    raw_schema_fields = [
        StructField("flow_id", StringType(), nullable=True),
        StructField("replay_run_tag", StringType(), nullable=True),
        StructField("is_control_record", IntegerType(), nullable=True),
        StructField("control_type", StringType(), nullable=True),
        StructField("event_time", StringType(), nullable=True),
        StructField("timestamp", StringType(), nullable=True),
        StructField("source_ingest_ts", StringType(), nullable=True),
        StructField("source_ingest_epoch_ms", DoubleType(), nullable=True),
        StructField("label_binary", IntegerType(), nullable=True),
        StructField("label", StringType(), nullable=True),
    ]
    raw_schema_fields.extend(
        StructField(column_name, DoubleType(), nullable=True)
        for column_name in full_feature_columns
    )

    return StructType(raw_schema_fields)


def build_parsed_stream(
    spark,
    *,
    bootstrap_servers: str,
    input_topic: str,
    starting_offsets: str,
    fail_on_data_loss: str,
    max_offsets_per_trigger: int,
    raw_schema,
):
    from pyspark.sql import functions as F

    source_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", input_topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", fail_on_data_loss)
        .option("maxOffsetsPerTrigger", str(max_offsets_per_trigger))
        .load()
    )

    return (
        source_df.selectExpr(
            "CAST(key AS STRING) AS kafka_key",
            "CAST(value AS STRING) AS raw_json",
            "timestamp AS kafka_timestamp",
            "topic AS kafka_topic",
            "partition AS kafka_partition",
            "offset AS kafka_offset",
        )
        .withColumn("obj", F.from_json("raw_json", raw_schema))
        .select(
            "kafka_key",
            "raw_json",
            "kafka_timestamp",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "obj.*"
        )
    )


def filter_input_run_tag(parsed_df, input_run_tag: str):
    from pyspark.sql import functions as F

    normalized_tag = input_run_tag.strip()
    if not normalized_tag:
        return parsed_df
    return parsed_df.filter(F.col("replay_run_tag") == F.lit(normalized_tag))


def split_control_and_data_records(parsed_df):
    from pyspark.sql import functions as F

    control_predicate = (
        F.coalesce(F.col("is_control_record"), F.lit(0)).cast("int") != F.lit(0)
    ) | (F.coalesce(F.col("control_type"), F.lit("")) != F.lit(""))
    control_df = parsed_df.filter(control_predicate)
    data_df = parsed_df.filter(~control_predicate)
    return data_df, control_df


def prepare_feature_columns(
    parsed_df,
    *,
    full_feature_columns: list[str],
    active_feature_columns: list[str],
    fill_values: dict[str, float],
):
    from pyspark.sql import functions as F

    active_feature_set = set(active_feature_columns)
    replacement_columns = {}
    for column_name in full_feature_columns:
        if column_name in active_feature_set:
            replacement_columns[column_name] = F.coalesce(
                F.col(column_name).cast("double"),
                F.lit(fill_values.get(column_name, 0.0)),
            ).alias(column_name)
        else:
            replacement_columns[column_name] = F.lit(None).cast("double").alias(column_name)

    projected_columns = []
    existing_columns = set(parsed_df.columns)

    for column_name in parsed_df.columns:
        projected_columns.append(replacement_columns.get(column_name, F.col(column_name)))

    for column_name in full_feature_columns:
        if column_name not in existing_columns:
            projected_columns.append(replacement_columns[column_name])

    return parsed_df.select(*projected_columns)


def add_event_timing_columns(
    prepared_df,
    *,
    watermark_delay_sec: int,
    drop_late_events: bool,
):
    from pyspark.sql import functions as F

    event_time_ts = F.coalesce(
        F.to_timestamp("event_time"),
        F.to_timestamp("timestamp"),
    )
    source_ingest_time_ts = F.when(
        F.col("source_ingest_epoch_ms").isNotNull(),
        F.timestamp_millis(F.col("source_ingest_epoch_ms").cast("long")),
    ).otherwise(F.to_timestamp("source_ingest_ts"))
    ingest_time = F.coalesce(F.col("kafka_timestamp"), F.current_timestamp())
    initial_projection = [F.col(column_name) for column_name in prepared_df.columns]
    initial_projection.extend(
        [
            event_time_ts.alias("event_time_ts"),
            ingest_time.alias("ingest_time"),
            source_ingest_time_ts.alias("source_ingest_time_ts"),
            F.coalesce(source_ingest_time_ts, event_time_ts).alias("watermark_ref_time"),
        ]
    )
    timed_df = prepared_df.select(*initial_projection)

    event_lateness_ms = (
        F.when(F.isnull(F.col("watermark_ref_time")), F.lit(0.0))
        .otherwise(
            F.greatest(
                F.lit(0.0),
                (
                    F.unix_millis(F.col("ingest_time"))
                    - F.unix_millis(F.col("watermark_ref_time"))
                ).cast("double"),
            )
        )
        .cast("double")
    )
    late_event_threshold_ms = float(watermark_delay_sec * 1000)
    final_projection = [F.col(column_name) for column_name in timed_df.columns]
    final_projection.extend(
        [
            event_lateness_ms.alias("event_lateness_ms"),
            F.when(F.isnull(F.col("watermark_ref_time")), F.lit(0)).otherwise(
                F.when(event_lateness_ms > F.lit(late_event_threshold_ms), F.lit(1)).otherwise(F.lit(0))
            ).alias("is_late_event"),
        ]
    )
    timed_df = timed_df.select(*final_projection)
    if drop_late_events and watermark_delay_sec > 0:
        timed_df = timed_df.filter(F.col("is_late_event") == F.lit(0))
    return timed_df


def add_source_latency_columns(scored_df, *, source_mode: str):
    from pyspark.sql import functions as F

    if source_mode == "source_timestamp":
        return scored_df.withColumn(
            "source_to_ingest_ms",
            F.when(F.isnull(F.col("source_ingest_time_ts")), F.lit(0.0))
            .otherwise(
                F.greatest(
                    F.lit(0.0),
                    F.unix_millis(F.col("ingest_time")) - F.unix_millis(F.col("source_ingest_time_ts")),
                )
            )
            .cast("double"),
        )

    return scored_df.withColumn(
        "source_to_ingest_ms",
        F.when(F.isnull(F.col("event_time_ts")), F.lit(0.0))
        .otherwise(
            F.greatest(
                F.lit(0.0),
                F.unix_millis(F.col("ingest_time")) - F.unix_millis(F.col("event_time_ts")),
            )
        )
        .cast("double"),
    )


def add_processing_latency_columns(scored_df, *, stream_started_epoch_ms: int = 0):
    from pyspark.sql import functions as F

    stream_started_ms = F.lit(int(stream_started_epoch_ms) if int(stream_started_epoch_ms) > 0 else 0).cast("long")
    effective_ingest_ms = F.greatest(
        F.unix_millis(F.col("ingest_time")),
        stream_started_ms,
    )
    effective_source_anchor_ms = F.greatest(
        F.coalesce(
            F.unix_millis(F.col("source_ingest_time_ts")),
            F.unix_millis(F.col("event_time_ts")),
            stream_started_ms,
        ),
        stream_started_ms,
    )
    projected_columns = [F.col(column_name) for column_name in scored_df.columns]
    projected_columns.extend(
        [
            (F.unix_millis(F.col("emit_time")) - effective_ingest_ms).cast("double").alias("processing_ms"),
            (
                F.unix_millis(F.col("emit_time"))
                - effective_source_anchor_ms
            ).cast("double").alias("end_to_end_ms"),
        ]
    )
    return scored_df.select(*projected_columns)


def build_prediction_payload(scored_df):
    from pyspark.sql import functions as F

    return scored_df.select(
        F.to_json(
            F.struct(
                F.coalesce(F.col("flow_id"), F.col("kafka_key")).alias("flow_id"),
                F.col("model_name"),
                F.col("feature_set"),
                F.col("run_tag"),
                F.col("prediction_label"),
                F.col("prediction_score"),
                F.col("threshold_used"),
                F.col("event_time_ts").alias("event_time"),
                F.col("ingest_time"),
                F.col("emit_time"),
                F.col("source_to_ingest_ms"),
                F.col("processing_ms"),
                F.col("end_to_end_ms"),
            )
        ).alias("value")
    )
