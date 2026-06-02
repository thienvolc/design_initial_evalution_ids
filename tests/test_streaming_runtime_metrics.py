from __future__ import annotations

import sys
import inspect
import unittest
from pathlib import Path
from unittest import mock

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import ids_platform.streaming.runtime.metrics as runtime_metrics
from ids_platform.streaming.runtime.metrics import (
    METRICS_BATCH_COLUMNS,
    MetricsPublisher,
    _build_latency_summary,
    collect_batch_statistics,
    make_metrics_batch_writer,
    publish_metrics_payload,
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


class _QueuedProducer:
    def __init__(self) -> None:
        self.produced: list[tuple[str, str]] = []

    def produce(self, *, topic: str, value: str) -> None:
        self.produced.append((topic, value))

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float | None = None) -> int:
        return 1 if timeout else 0


class _FakeBatchFrame:
    def __init__(self) -> None:
        self.selected_columns: tuple[str, ...] | None = None
        self.cache_calls = 0
        self.unpersist_calls = 0

    def select(self, *columns: str):
        self.selected_columns = columns
        return self

    def cache(self):
        self.cache_calls += 1
        return self

    def unpersist(self) -> None:
        self.unpersist_calls += 1


def _runtime_config() -> RuntimeConfig:
    return RuntimeConfig(
        run=RuntimeRunConfig(run_tag="run-1", load_profile=""),
        spark=RuntimeSparkConfig(),
        kafka=RuntimeKafkaConfig(),
        model=RuntimeModelConfig(name="rf"),
        features=RuntimeFeatureConfig(feature_set="full", columns=["f1"], fill_values={}),
        output=RuntimeOutputConfig(
            parquet_checkpoint=Path("checkpoints/parquet"),
            metrics_checkpoint=Path("checkpoints/metrics"),
            sentinel_checkpoint=Path("checkpoints/sentinel"),
            artifact_output=Path("predictions/run"),
        ),
        lifecycle=RuntimeLifecycleConfig(drop_late_events=True),
        metrics=RuntimeMetricsConfig(),
    )


class RuntimeMetricsTests(unittest.TestCase):
    def tearDown(self) -> None:
        runtime_metrics._METRICS_PRODUCER_CACHE.clear()

    def test_metrics_batch_columns_preserve_writer_input_contract(self) -> None:
        self.assertEqual(
            METRICS_BATCH_COLUMNS,
            (
                "benchmark_phase",
                "kafka_partition",
                "kafka_offset",
                "source_to_ingest_ms",
                "event_lateness_ms",
                "is_late_event",
            ),
        )

    def test_collect_batch_statistics_uses_only_operational_columns(self) -> None:
        source = inspect.getsource(collect_batch_statistics)

        self.assertNotIn("prediction_label", source)

    def test_build_latency_summary_preserves_legacy_aliases(self) -> None:
        stat = {
            "source_p50_ms": 1.0,
            "source_p95_ms": 2.0,
            "source_p99_ms": 3.0,
            "event_lateness_p95_ms": 4.0,
        }

        latency = _build_latency_summary(stat=stat, batch_wall_ms=10.0)

        self.assertEqual(latency["processing"]["semantic"], "legacy_alias_for_batch_wall_time")
        self.assertEqual(latency["processing"]["p95"], 10.0)
        self.assertEqual(latency["ingest_to_emit"]["p95"], 10.0)
        self.assertEqual(latency["end_to_end"]["p95"], 12.0)
        self.assertEqual(latency["source_to_emit"]["p95"], 12.0)
        self.assertEqual(latency["event_lateness"]["p95"], 4.0)

    def test_publish_metrics_payload_raises_when_flush_leaves_messages(self) -> None:
        producer = _QueuedProducer()
        runtime_metrics._METRICS_PRODUCER_CACHE["kafka:29092"] = producer

        with self.assertRaises(RuntimeError):
            publish_metrics_payload(
                payload={"run_tag": "run-1", "event_type": "batch_metrics", "batch_id": 7},
                bootstrap_servers="kafka:29092",
                metrics_topic="ids.metrics",
            )

        self.assertEqual(producer.produced[0][0], "ids.metrics")
        self.assertIs(runtime_metrics._METRICS_PRODUCER_CACHE["kafka:29092"], producer)

    def test_metrics_publisher_delegates_payload_publish(self) -> None:
        publisher = MetricsPublisher(bootstrap_servers="kafka:29092", metrics_topic="ids.metrics")

        with mock.patch(
            "ids_platform.streaming.runtime.metrics.publish_metrics_payload",
            return_value=True,
        ) as publish:
            result = publisher.publish({"run_tag": "run-1"})

        self.assertTrue(result)
        publish.assert_called_once_with(
            payload={"run_tag": "run-1"},
            bootstrap_servers="kafka:29092",
            metrics_topic="ids.metrics",
        )

    def test_make_metrics_batch_writer_builds_payload(self) -> None:
        batch_df = _FakeBatchFrame()
        config = _runtime_config()

        stat = {
            "rows": 10,
            "source_p50_ms": 1.0,
            "source_p95_ms": 2.0,
            "source_p99_ms": 3.0,
            "event_lateness_p95_ms": 4.0,
            "late_event_ratio": 0.1,
        }

        with mock.patch(
            "ids_platform.streaming.runtime.metrics.collect_batch_offsets",
            return_value=[(0, 9)],
        ), mock.patch(
            "ids_platform.streaming.runtime.metrics.collect_batch_statistics",
            return_value=stat,
        ), mock.patch(
            "ids_platform.streaming.runtime.metrics.probe_kafka_lag",
            return_value={
                "lag_records_total": 5,
                "lag_records_max_partition": 5,
            },
        ), mock.patch(
            "ids_platform.streaming.runtime.metrics.probe_process_metrics",
            return_value={
                "driver_cpu_percent": 12.5,
                "driver_rss_mb": 256,
            },
        ), mock.patch(
            "ids_platform.streaming.runtime.metrics.probe_executor_memory_utilization",
            return_value={
                "executor_mem_util_avg": 0.4,
                "executor_mem_util_p95": 0.8,
                "executor_count": 1,
            },
        ), mock.patch(
            "ids_platform.streaming.runtime.metrics.publish_metrics_payload",
            return_value=True,
        ) as publish:
            writer = make_metrics_batch_writer(
                spark=object(),
                config=config,
                bootstrap_servers="kafka:29092",
                input_topic="ids.raw",
                metrics_topic="ids.metrics",
                resolved_load_profile="rf:full",
                reported_model_name="rf",
                max_offsets_per_trigger=100,
                shuffle_partitions=4,
                trigger_interval="1 second",
                watermark_delay_sec=5,
            )
            writer(batch_df, 7)

        payload = publish.call_args.kwargs["payload"]
        self.assertEqual(payload["batch_id"], 7)
        self.assertEqual(payload["benchmark_phase"], "measure")
        self.assertEqual(payload["run_tag"], "run-1")
        self.assertEqual(payload["load_profile"], "rf:full")
        self.assertEqual(payload["model_name"], "rf")
        self.assertEqual(payload["rows"], 10)
        self.assertNotIn("detection", payload)
        self.assertNotIn("precision", payload)
        self.assertNotIn("f1", payload)
        self.assertNotIn("avg_prediction_score", payload)
        self.assertNotIn("attack_ratio", payload)
        self.assertEqual(payload["kafka"]["lag_records_total"], 5)
        self.assertNotIn("metric_warnings", payload)
        self.assertEqual(batch_df.cache_calls, 1)
        self.assertEqual(batch_df.unpersist_calls, 1)


if __name__ == "__main__":
    unittest.main()
