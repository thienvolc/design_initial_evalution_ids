from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ids_platform.common.config import load_yaml_mapping
from ids_platform.common.paths import resolve_project_path


def resolve_kafka_bootstrap_servers(
    configured_bootstrap_servers: str,
    *,
    execution_mode: str = "",
) -> str:
    override = str(os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "")).strip()
    if override:
        return override

    bootstrap_servers = str(configured_bootstrap_servers or "").strip() or "localhost:9092"
    mode = str(execution_mode or os.environ.get("IDS_EXECUTION_MODE", "")).strip().lower()
    if mode == "host":
        resolved_servers: list[str] = []
        for raw_server in bootstrap_servers.split(","):
            server = raw_server.strip()
            if not server:
                continue
            if server == "kafka" or server.startswith("kafka:"):
                _, separator, port = server.partition(":")
                # Docker-internal listener `kafka:29092` is exposed to the host as `localhost:9092`.
                resolved_servers.append("localhost:9092" if port == "29092" or not separator else f"localhost:{port}")
                continue
            resolved_servers.append(server)
        return ",".join(resolved_servers) or "localhost:9092"
    return bootstrap_servers


@dataclass(frozen=True)
class SparkRuntimeConfig:
    app_name: str
    master: str
    driver_host: str
    driver_bind_address: str
    shuffle_partitions: int
    arrow_enabled: bool
    kafka_packages: str = ""
    trigger_interval: str = "10 seconds"
    arrow_max_records_per_batch: int = 10000


@dataclass(frozen=True)
class LatencyConfig:
    source_to_ingest_mode: str
    watermark_delay_sec: int = 0
    event_time_columns: list[str] = field(default_factory=list)
    source_time_columns: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str
    input_topic: str
    output_topic: str
    metrics_topic: str
    starting_offsets: str
    fail_on_data_loss: bool
    max_offsets_per_trigger: int


@dataclass(frozen=True)
class StreamingPathsConfig:
    raw: dict[str, Any]
    input_parquet: str
    feature_manifest: str
    valid_metrics_csv: str
    prediction_artifact_dir: str | None = None
    checkpoint_root: str | None = None
    output_parquet_dir: str | None = None


@dataclass(frozen=True)
class StreamingModelConfig:
    name: str
    enabled: bool
    joblib_path: str | None
    joblib_path_by_feature_set: dict[str, str]
    threshold: float | None
    raw: dict[str, Any]


@dataclass(frozen=True)
class StructuredStreamingAppConfig:
    config_path: Path
    runtime: SparkRuntimeConfig
    latency: LatencyConfig
    kafka: KafkaConfig
    paths: StreamingPathsConfig
    models: list[StreamingModelConfig]
    raw: dict[str, Any]


@dataclass(frozen=True)
class BatchBenchmarkAppConfig:
    config_path: Path
    runtime: SparkRuntimeConfig
    latency: LatencyConfig
    paths: StreamingPathsConfig
    models: list[StreamingModelConfig]
    raw: dict[str, Any]


def _build_spark_runtime_config(runtime_cfg: dict[str, Any]) -> SparkRuntimeConfig:
    spark_cfg = runtime_cfg.get("spark") or {}
    return SparkRuntimeConfig(
        app_name=str(spark_cfg.get("app_name", "ids_streaming")),
        master=str(spark_cfg.get("master", "local[*]")),
        driver_host=str(spark_cfg.get("driver_host", "127.0.0.1")),
        driver_bind_address=str(spark_cfg.get("driver_bind_address", "127.0.0.1")),
        shuffle_partitions=int(spark_cfg.get("shuffle_partitions", 8)),
        arrow_enabled=bool(spark_cfg.get("arrow_enabled", False)),
        kafka_packages=str(spark_cfg.get("kafka_packages", "")).strip(),
        trigger_interval=str(spark_cfg.get("trigger_interval", "10 seconds")),
        arrow_max_records_per_batch=int(spark_cfg.get("arrow_max_records_per_batch", 10000)),
    )


def _build_latency_config(runtime_cfg: dict[str, Any]) -> LatencyConfig:
    latency_cfg = runtime_cfg.get("latency") or {}
    return LatencyConfig(
        source_to_ingest_mode=str(latency_cfg.get("source_to_ingest_mode", "source_timestamp")).strip().lower(),
        watermark_delay_sec=int(latency_cfg.get("watermark_delay_sec", 0) or 0),
        event_time_columns=[
            str(item).strip()
            for item in latency_cfg.get("event_time_columns", [])
            if str(item).strip()
        ],
        source_time_columns=[
            str(item).strip()
            for item in latency_cfg.get("source_time_columns", [])
            if str(item).strip()
        ],
    )


def _build_kafka_config(raw_cfg: dict[str, Any]) -> KafkaConfig:
    kafka_cfg = raw_cfg.get("kafka") or {}
    return KafkaConfig(
        bootstrap_servers=resolve_kafka_bootstrap_servers(
            str(kafka_cfg.get("bootstrap_servers", "localhost:9092"))
        ),
        input_topic=str(kafka_cfg.get("input_topic", "ids.raw.flows")),
        output_topic=str(kafka_cfg.get("output_topic", "ids.predictions.binary")),
        metrics_topic=str(kafka_cfg.get("metrics_topic", "ids.metrics")),
        starting_offsets=str(kafka_cfg.get("starting_offsets", "earliest")),
        fail_on_data_loss=bool(kafka_cfg.get("fail_on_data_loss", False)),
        max_offsets_per_trigger=int(kafka_cfg.get("max_offsets_per_trigger", 20000)),
    )


def _build_paths_config(raw_cfg: dict[str, Any]) -> StreamingPathsConfig:
    paths_cfg = raw_cfg.get("paths") or {}
    return StreamingPathsConfig(
        raw=paths_cfg,
        input_parquet=str(paths_cfg.get("input_parquet", "data/gold/splits/test.parquet")),
        feature_manifest=str(paths_cfg.get("feature_manifest", "artifacts/offline/preprocessing/feature_manifest.json")),
        valid_metrics_csv=str(paths_cfg.get("valid_metrics_csv", "artifacts/offline/models/valid_metrics.csv")),
        prediction_artifact_dir=_optional_string(paths_cfg.get("prediction_artifact_dir")),
        checkpoint_root=_optional_string(paths_cfg.get("checkpoint_root")),
        output_parquet_dir=_optional_string(paths_cfg.get("output_parquet_dir")),
    )


def _build_model_configs(raw_cfg: dict[str, Any]) -> list[StreamingModelConfig]:
    model_rows = raw_cfg.get("models") or []
    models: list[StreamingModelConfig] = []
    for row in model_rows:
        if not isinstance(row, dict):
            continue
        raw_threshold = row.get("threshold")
        threshold = float(raw_threshold) if (raw_threshold is not None) else None
        feature_set_paths = row.get("joblib_path_by_feature_set") or {}
        joblib_path_by_feature_set = (
            {
                str(key): str(value)
                for key, value in feature_set_paths.items()
                if str(key).strip() and str(value).strip()
            }
            if isinstance(feature_set_paths, dict) else {}
        )

        models.append(
            StreamingModelConfig(
                name=str(row.get("name", "")).strip(),
                enabled=bool(row.get("enabled", True)),
                joblib_path=_optional_string(row.get("joblib_path")),
                joblib_path_by_feature_set=joblib_path_by_feature_set,
                threshold=threshold,
                raw=row,
            )
        )
    return models


def load_structured_streaming_app_config(path: str | Path) -> StructuredStreamingAppConfig:
    config_path = resolve_project_path(str(path))
    raw_cfg = load_yaml_mapping(config_path)
    runtime_cfg = raw_cfg.get("runtime") or {}
    return StructuredStreamingAppConfig(
        config_path=config_path,
        runtime=_build_spark_runtime_config(runtime_cfg),
        latency=_build_latency_config(runtime_cfg),
        kafka=_build_kafka_config(raw_cfg),
        paths=_build_paths_config(raw_cfg),
        models=_build_model_configs(raw_cfg),
        raw=raw_cfg,
    )


def load_batch_benchmark_app_config(path: str | Path) -> BatchBenchmarkAppConfig:
    config_path = resolve_project_path(str(path))
    raw_cfg = load_yaml_mapping(config_path)
    runtime_cfg = raw_cfg.get("runtime") or {}
    return BatchBenchmarkAppConfig(
        config_path=config_path,
        runtime=_build_spark_runtime_config(runtime_cfg),
        latency=_build_latency_config(runtime_cfg),
        paths=_build_paths_config(raw_cfg),
        models=_build_model_configs(raw_cfg),
        raw=raw_cfg,
    )


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
