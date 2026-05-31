from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.runtime.pipeline import (
    CONTROL_COLUMNS,
    KAFKA_METADATA_COLUMNS,
    LATENCY_COLUMNS,
    RAW_BASE_FIELDS,
    TIMING_COLUMNS,
    build_raw_schema,
    prepare_feature_columns,
)


class RuntimePipelineContractTests(unittest.TestCase):
    def test_raw_base_fields_preserve_replay_control_label_contract(self) -> None:
        self.assertEqual(
            RAW_BASE_FIELDS,
            (
                ("flow_id", "string"),
                ("replay_run_tag", "string"),
                ("is_control_record", "integer"),
                ("control_type", "string"),
                ("event_time", "string"),
                ("timestamp", "string"),
                ("source_ingest_ts", "string"),
                ("source_ingest_epoch_ms", "double"),
                ("label_binary", "integer"),
                ("label", "string"),
            ),
        )

    def test_pipeline_column_groups_preserve_runtime_contract(self) -> None:
        self.assertEqual(
            KAFKA_METADATA_COLUMNS,
            (
                "kafka_key",
                "raw_json",
                "kafka_timestamp",
                "kafka_topic",
                "kafka_partition",
                "kafka_offset",
            ),
        )
        self.assertEqual(CONTROL_COLUMNS, ("is_control_record", "control_type"))
        self.assertEqual(
            TIMING_COLUMNS,
            (
                "event_time_ts",
                "ingest_time",
                "source_ingest_time_ts",
                "watermark_ref_time",
                "event_lateness_ms",
                "is_late_event",
            ),
        )
        self.assertEqual(
            LATENCY_COLUMNS,
            ("source_to_ingest_ms", "processing_ms", "end_to_end_ms"),
        )

    def test_build_raw_schema_preserves_base_field_order_and_appends_features(self) -> None:
        schema = build_raw_schema(["duration", "bytes"])

        self.assertEqual(
            [field.name for field in schema.fields],
            [name for name, _type_name in RAW_BASE_FIELDS] + ["duration", "bytes"],
        )

    def test_prepare_feature_columns_rejects_missing_features(self) -> None:
        class _FakeFrame:
            columns = ["duration"]

        with self.assertRaisesRegex(ValueError, "missing runtime feature columns: bytes"):
            prepare_feature_columns(
                _FakeFrame(),
                feature_columns=["duration", "bytes"],
                fill_values={},
            )


if __name__ == "__main__":
    unittest.main()
