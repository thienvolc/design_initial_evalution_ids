from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest import mock

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.runtime.session import (
    create_spark_session,
    prepare_spark_environment,
)
from ids_platform.streaming.runtime.config import (
    RuntimeConfig,
    RuntimeFeatureConfig,
    RuntimeKafkaConfig,
    RuntimeLifecycleConfig,
    RuntimeMetricsConfig,
    RuntimeModelConfig,
    RuntimeOutputConfig,
    RuntimeRunConfig,
    RuntimeSparkConfig,
)


class _FakeSparkBuilder:
    def __init__(self) -> None:
        self.calls: list[tuple] = []
        self.session = object()

    def appName(self, value: str):
        self.calls.append(("appName", value))
        return self

    def master(self, value: str):
        self.calls.append(("master", value))
        return self

    def config(self, key: str, value: str):
        self.calls.append(("config", key, value))
        return self

    def getOrCreate(self):
        self.calls.append(("getOrCreate",))
        return self.session


class RuntimeSessionTests(unittest.TestCase):
    def test_prepare_spark_environment_sets_defaults(self) -> None:
        spark_config = SimpleNamespace(driver_host="127.0.0.1")
        with mock.patch.dict(os.environ, {}, clear=True):
            prepare_spark_environment(spark_config)

            self.assertEqual(os.environ["PYSPARK_PYTHON"], sys.executable)
            self.assertEqual(os.environ["PYSPARK_DRIVER_PYTHON"], sys.executable)
            self.assertEqual(os.environ["SPARK_LOCAL_IP"], "127.0.0.1")

    def test_create_spark_session_configures_builder_and_primes_metrics(self) -> None:
        builder = _FakeSparkBuilder()
        spark_session_class = SimpleNamespace(builder=builder)
        pyspark_sql_module = ModuleType("pyspark.sql")
        pyspark_sql_module.SparkSession = spark_session_class
        config = RuntimeConfig(
            run=RuntimeRunConfig(),
            spark=RuntimeSparkConfig(
                app_name="ids",
                master="local[*]",
                driver_host="127.0.0.1",
                driver_bind_address="0.0.0.0",
                shuffle_partitions=4,
                arrow_enabled=False,
                kafka_packages="spark-kafka-package",
            ),
            kafka=RuntimeKafkaConfig(
                bootstrap_servers="kafka:29092",
                input_topic="ids.raw",
                metrics_topic="ids.metrics",
            ),
            model=RuntimeModelConfig(),
            features=RuntimeFeatureConfig(columns=["f1"], fill_values={}),
            output=RuntimeOutputConfig(
                parquet_checkpoint=Path("checkpoints/parquet"),
                metrics_checkpoint=Path("checkpoints/metrics"),
                sentinel_checkpoint=Path("checkpoints/sentinel"),
                artifact_output=Path("predictions/run"),
            ),
            lifecycle=RuntimeLifecycleConfig(),
            metrics=RuntimeMetricsConfig(),
        )

        with mock.patch.dict(sys.modules, {"pyspark.sql": pyspark_sql_module}), mock.patch(
            "ids_platform.streaming.runtime.session.prime_process_metrics_probe"
        ) as prime_probe:
            result = create_spark_session(config)

        self.assertIs(result, builder.session)
        self.assertIn(("appName", "ids"), builder.calls)
        self.assertIn(("master", "local[*]"), builder.calls)
        self.assertIn(("config", "spark.sql.shuffle.partitions", "4"), builder.calls)
        self.assertIn(("config", "spark.jars.packages", "spark-kafka-package"), builder.calls)
        self.assertIn(("getOrCreate",), builder.calls)
        prime_probe.assert_called_once()


if __name__ == "__main__":
    unittest.main()
