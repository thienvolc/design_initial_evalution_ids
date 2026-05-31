from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest import mock

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.runtime import assembly
from ids_platform.streaming.runtime.assembly import build_runtime_streams
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


def _config(*, pass_through: bool) -> RuntimeConfig:
    return RuntimeConfig(
        run=RuntimeRunConfig(run_tag="run-1", input_run_tag="trace-1"),
        spark=RuntimeSparkConfig(),
        kafka=RuntimeKafkaConfig(
            bootstrap_servers="kafka:29092",
            input_topic="ids.raw",
            starting_offsets="earliest",
            fail_on_data_loss=False,
            max_offsets_per_trigger=100,
        ),
        model=RuntimeModelConfig(
            name="random_forest",
            mode="pass_through" if pass_through else "model",
            artifact_path=None if pass_through else Path("artifacts/model.joblib"),
            threshold=None if pass_through else 0.7,
        ),
        features=RuntimeFeatureConfig(
            feature_set="full",
            columns=["duration", "bytes"],
            fill_values={"duration": 0.0, "bytes": 1.0},
        ),
        output=RuntimeOutputConfig(
            parquet_checkpoint=Path("checkpoints/parquet"),
            metrics_checkpoint=Path("checkpoints/metrics"),
            sentinel_checkpoint=Path("checkpoints/sentinel"),
            artifact_output=Path("predictions/run"),
        ),
        lifecycle=RuntimeLifecycleConfig(
            drop_late_events=True,
            watermark_delay_sec=5,
        ),
        metrics=RuntimeMetricsConfig(),
    )


class RuntimeAssemblyTests(unittest.TestCase):
    def _patch_common_pipeline(self):
        patches = [
            mock.patch.object(assembly, "build_raw_schema", return_value="raw_schema"),
            mock.patch.object(assembly, "build_parsed_stream", return_value="parsed"),
            mock.patch.object(assembly, "filter_input_run_tag", return_value="filtered"),
            mock.patch.object(assembly, "split_control_and_data_records", return_value=("data", "control")),
            mock.patch.object(assembly, "prepare_feature_columns", return_value="prepared"),
            mock.patch.object(assembly, "add_event_timing_columns", return_value="timed"),
            mock.patch.object(assembly, "add_source_latency_columns", return_value="source_latency"),
            mock.patch.object(assembly, "add_processing_latency_columns", return_value="scored"),
        ]
        return [patch.start() for patch in patches], patches

    def test_build_runtime_streams_uses_pass_through_scoring_path(self) -> None:
        _started, patches = self._patch_common_pipeline()
        try:
            with mock.patch.object(
                assembly,
                "add_passthrough_prediction_columns",
                return_value="pass_scored",
            ) as passthrough, mock.patch.object(assembly, "make_score_udf") as make_score:
                result = build_runtime_streams(
                    spark="spark",
                    config=_config(pass_through=True),
                    stream_started_epoch_ms=123,
                )

            self.assertEqual(result.scored_stream, "scored")
            self.assertEqual(result.control_stream, "control")
            passthrough.assert_called_once_with(
                "timed",
                model_name="pass_through",
                feature_set="full",
                run_tag="run-1",
            )
            make_score.assert_not_called()
        finally:
            for patch in reversed(patches):
                patch.stop()

    def test_build_runtime_streams_uses_model_scoring_path(self) -> None:
        _started, patches = self._patch_common_pipeline()
        try:
            with mock.patch.object(assembly, "make_score_udf", return_value="score_udf") as make_score, mock.patch.object(
                assembly,
                "add_prediction_columns",
                return_value="model_scored",
            ) as add_prediction:
                result = build_runtime_streams(
                    spark="spark",
                    config=_config(pass_through=False),
                    stream_started_epoch_ms=123,
                )

            self.assertEqual(result.scored_stream, "scored")
            make_score.assert_called_once_with(
                "artifacts\\model.joblib",
                ["duration", "bytes"],
                {"duration": 0.0, "bytes": 1.0},
            )
            add_prediction.assert_called_once_with(
                "timed",
                score_udf="score_udf",
                feature_columns=["duration", "bytes"],
                threshold=0.7,
                model_name="random_forest",
                feature_set="full",
                run_tag="run-1",
            )
        finally:
            for patch in reversed(patches):
                patch.stop()

if __name__ == "__main__":
    unittest.main()
