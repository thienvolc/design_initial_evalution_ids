from __future__ import annotations

from dataclasses import dataclass

from ids_platform.streaming.runtime.pipeline import (
    add_event_timing_columns,
    add_processing_latency_columns,
    add_source_latency_columns,
    build_parsed_stream,
    build_raw_schema,
    filter_input_run_tag,
    prepare_feature_columns,
    split_control_and_data_records,
)
from ids_platform.streaming.runtime.scoring import (
    add_passthrough_prediction_columns,
    add_prediction_columns,
    make_score_udf,
)


@dataclass(frozen=True)
class RuntimeStreams:
    scored_stream: object
    control_stream: object


def build_runtime_streams(
    *,
    spark,
    config,
    stream_started_epoch_ms: int,
) -> RuntimeStreams:
    model_name = config.model.name
    reported_model_name = config.model.reported_name
    model_feature_columns = config.features.feature_columns
    fill_values = config.features.imputer_fill_values

    raw_schema = build_raw_schema(model_feature_columns)
    parsed_stream = build_parsed_stream(
        spark,
        bootstrap_servers=config.kafka.bootstrap_servers,
        input_topic=config.kafka.input_topic,
        starting_offsets=config.kafka.starting_offsets,
        fail_on_data_loss=str(config.kafka.fail_on_data_loss).lower(),
        max_offsets_per_trigger=config.kafka.max_offsets_per_trigger,
        raw_schema=raw_schema,
    )
    parsed_stream = filter_input_run_tag(parsed_stream, config.run.input_run_tag)
    parsed_stream, control_stream = split_control_and_data_records(parsed_stream)

    prepared_stream = prepare_feature_columns(
        parsed_stream,
        feature_columns=model_feature_columns,
        fill_values=fill_values,
    )
    prepared_stream = add_event_timing_columns(
        prepared_stream,
        watermark_delay_sec=config.lifecycle.watermark_delay_sec,
        drop_late_events=bool(config.lifecycle.drop_late_events),
    )

    if config.model.is_pass_through:
        scored_stream = add_passthrough_prediction_columns(
            prepared_stream,
            model_name=reported_model_name,
            feature_set=config.features.feature_set,
            run_tag=config.run.run_tag,
        )
    else:
        score_udf = make_score_udf(str(config.model.artifact_path), model_feature_columns, fill_values)
        scored_stream = add_prediction_columns(
            prepared_stream,
            score_udf=score_udf,
            feature_columns=model_feature_columns,
            threshold=float(config.model.threshold if config.model.threshold is not None else 0.5),
            model_name=model_name,
            feature_set=config.features.feature_set,
            run_tag=config.run.run_tag,
        )

    scored_stream = add_source_latency_columns(
        scored_stream,
    )
    scored_stream = add_processing_latency_columns(
        scored_stream,
        stream_started_epoch_ms=stream_started_epoch_ms,
    )

    return RuntimeStreams(
        scored_stream=scored_stream,
        control_stream=control_stream,
    )
