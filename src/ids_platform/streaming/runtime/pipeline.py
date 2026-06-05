from __future__ import annotations


RAW_BASE_FIELDS = (
    ("flow_id", "string"),
    ("replay_run_tag", "string"),
    ("benchmark_phase", "string"),
    ("is_control_record", "integer"),
    ("control_type", "string"),
    ("event_time", "string"),
    ("timestamp", "string"),
    ("source_ingest_ts", "string"),
    ("source_ingest_epoch_ms", "double"),
    ("label_binary", "integer"),
    ("label", "string"),
)

KAFKA_METADATA_COLUMNS = (
    "kafka_timestamp",
    "kafka_partition",
    "kafka_offset",
)

def _project_existing_columns(df):
    from pyspark.sql import functions as F

    return [F.col(column_name) for column_name in df.columns]


def _select_with_added_columns(df, added_columns):
    return df.select(*_project_existing_columns(df), *added_columns)


def build_raw_schema(full_feature_columns: list[str]):
    from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

    field_types = {
        "double": DoubleType(),
        "integer": IntegerType(),
        "string": StringType(),
    }
    raw_schema_fields = [
        StructField(name, field_types[type_name], nullable=True)
        for name, type_name in RAW_BASE_FIELDS
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

    return source_df.select(
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.from_json(F.col("value").cast("string"), raw_schema).alias("obj"),
    ).select(*KAFKA_METADATA_COLUMNS, "obj.*")


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


def add_event_timing_columns(prepared_df):
    from pyspark.sql import functions as F

    event_time_ts = F.to_timestamp("event_time")
    source_ingest_time_ts = F.when(
        F.col("source_ingest_epoch_ms").isNotNull(),
        F.timestamp_millis(F.col("source_ingest_epoch_ms").cast("long")),
    ).otherwise(F.to_timestamp("source_ingest_ts"))
    ingest_time = F.coalesce(F.col("kafka_timestamp"), F.current_timestamp())
    timed_df = _select_with_added_columns(
        prepared_df,
        [
            event_time_ts.alias("event_time_ts"),
            ingest_time.alias("ingest_time"),
            source_ingest_time_ts.alias("source_ingest_time_ts"),
            F.coalesce(source_ingest_time_ts, event_time_ts).alias("event_reference_time"),
        ],
    )

    event_lateness_ms = (
        F.when(F.isnull(F.col("event_reference_time")), F.lit(0.0))
        .otherwise(
            F.greatest(
                F.lit(0.0),
                (
                    F.unix_millis(F.col("ingest_time"))
                    - F.unix_millis(F.col("event_reference_time"))
                ).cast("double"),
            )
        )
        .cast("double")
    )
    timed_df = _select_with_added_columns(
        timed_df,
        [
            event_lateness_ms.alias("event_lateness_ms"),
            F.when(F.isnull(F.col("event_reference_time")), F.lit(0)).otherwise(
                F.when(event_lateness_ms > F.lit(0.0), F.lit(1)).otherwise(F.lit(0))
            ).alias("is_late_event"),
        ],
    )
    return timed_df


def add_source_latency_columns(scored_df):
    from pyspark.sql import functions as F

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
    return _select_with_added_columns(
        scored_df,
        [
            (F.unix_millis(F.col("emit_time")) - effective_ingest_ms).cast("double").alias("processing_ms"),
            (
                F.unix_millis(F.col("emit_time"))
                - effective_source_anchor_ms
            ).cast("double").alias("end_to_end_ms"),
        ],
    )
