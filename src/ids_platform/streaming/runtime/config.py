from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class RuntimeRunConfig:
    run_tag: str = ""
    input_run_tag: str = ""
    load_profile: str = ""


@dataclass(frozen=True)
class RuntimeSparkConfig:
    app_name: str = "ids_structured_streaming"
    master: str = "local[1]"
    driver_host: str = "127.0.0.1"
    driver_bind_address: str = "127.0.0.1"
    shuffle_partitions: int = 8
    arrow_enabled: bool = True
    kafka_packages: str = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
    trigger_interval: str = "10 seconds"
    arrow_max_records_per_batch: int = 10000


@dataclass(frozen=True)
class RuntimeKafkaConfig:
    bootstrap_servers: str = "kafka:29092"
    input_topic: str = "ids.raw.flows"
    prediction_topic: str = "ids.predictions"
    starting_offsets: str = "earliest"
    fail_on_data_loss: bool = False
    max_offsets_per_trigger: int = 20_000


@dataclass(frozen=True)
class RuntimeModelConfig:
    name: str = "random_forest"
    mode: str = "model"
    artifact_path: Path | None = None
    threshold: float | None = None

    @property
    def is_pass_through(self) -> bool:
        return self.mode == "pass_through"

    @property
    def reported_name(self) -> str:
        return "pass_through" if self.is_pass_through else self.name


@dataclass(frozen=True)
class RuntimeFeatureConfig:
    feature_set: str = "full"
    columns: list[str] | None = None

    @property
    def feature_columns(self) -> list[str]:
        return list(self.columns or [])


@dataclass(frozen=True)
class RuntimeOutputConfig:
    prediction_checkpoint: Path


@dataclass(frozen=True)
class RuntimeLifecycleConfig:
    reset_outputs: bool = False


@dataclass(frozen=True)
class RuntimeConfig:
    run: RuntimeRunConfig
    spark: RuntimeSparkConfig
    kafka: RuntimeKafkaConfig
    model: RuntimeModelConfig
    features: RuntimeFeatureConfig
    output: RuntimeOutputConfig
    lifecycle: RuntimeLifecycleConfig
