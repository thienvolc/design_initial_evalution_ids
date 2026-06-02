from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.runtime.sinks import (
    PREDICTION_PARQUET_COLUMNS,
    build_input_sentinel_writer,
    build_metrics_writer,
    build_prediction_parquet_writer,
    group_runtime_queries,
    start_runtime_queries,
)


class _FakeQuery:
    pass


class _FakeWriter:
    def __init__(self, query: _FakeQuery | None = None) -> None:
        self.operations: list[tuple] = []
        self.query = query or _FakeQuery()

    def format(self, value: str):
        self.operations.append(("format", value))
        return self

    def option(self, key: str, value: str):
        self.operations.append(("option", key, value))
        return self

    def outputMode(self, value: str):
        self.operations.append(("outputMode", value))
        return self

    def queryName(self, value: str):
        self.operations.append(("queryName", value))
        return self

    def foreachBatch(self, callback):
        self.operations.append(("foreachBatch", callback))
        return self

    def trigger(self, **kwargs):
        self.operations.append(("trigger", kwargs))
        return self

    def start(self):
        self.operations.append(("start",))
        return self.query


class _FakeStream:
    def __init__(self) -> None:
        self.writer = _FakeWriter()
        self.selected_columns: tuple[str, ...] | None = None
        self.selected_child: _FakeStream | None = None
        self.filter_condition = None
        self.control_type = "input_sentinel"

    @property
    def writeStream(self):
        return self.writer

    def select(self, *columns: str):
        selected = _FakeStream()
        selected.selected_columns = columns
        self.selected_child = selected
        return selected

    def filter(self, condition):
        filtered = _FakeStream()
        filtered.filter_condition = condition
        self.filtered_child = filtered
        return filtered


class RuntimeSinksTests(unittest.TestCase):
    def test_build_prediction_parquet_writer_selects_output_schema(self) -> None:
        stream = _FakeStream()

        writer = build_prediction_parquet_writer(
            stream,
            output_path=Path("artifacts/run-1"),
            checkpoint_path=Path("checkpoints/parquet"),
            model_name="rf",
        )

        self.assertEqual(stream.selected_child.selected_columns, PREDICTION_PARQUET_COLUMNS)
        self.assertEqual(writer.operations[0], ("format", "parquet"))
        self.assertIn(("option", "path", "artifacts\\run-1"), writer.operations)
        self.assertIn(("option", "checkpointLocation", "checkpoints\\parquet"), writer.operations)
        self.assertIn(("outputMode", "append"), writer.operations)
        self.assertIn(("queryName", "ids_predictions_parquet_rf"), writer.operations)

    def test_build_input_sentinel_writer_uses_foreach_batch(self) -> None:
        stream = _FakeStream()
        callback = object()

        writer = build_input_sentinel_writer(
            stream,
            checkpoint_path=Path("checkpoints/sentinel"),
            model_name="rf",
            mark_input_sentinel=callback,
        )

        self.assertIn(("foreachBatch", callback), writer.operations)
        self.assertIn(("option", "checkpointLocation", "checkpoints\\sentinel"), writer.operations)
        self.assertIn(("queryName", "ids_input_sentinel_rf"), writer.operations)

    def test_build_metrics_writer_uses_foreach_batch_and_checkpoint(self) -> None:
        stream = _FakeStream()
        callback = object()

        writer = build_metrics_writer(
            stream,
            write_metrics_batch=callback,
            checkpoint_path=Path("checkpoints/metrics"),
            model_name="rf",
        )

        self.assertIs(writer, stream.writer)
        self.assertIn(("foreachBatch", callback), writer.operations)
        self.assertIn(("option", "checkpointLocation", "checkpoints\\metrics"), writer.operations)
        self.assertIn(("queryName", "ids_metrics_rf"), writer.operations)

    def test_start_runtime_queries_applies_trigger_and_returns_active_queries(self) -> None:
        parquet_writer = _FakeWriter()
        metrics_writer = _FakeWriter()
        sentinel_writer = _FakeWriter()

        result = start_runtime_queries(
            parquet_writer=parquet_writer,
            metrics_writer=metrics_writer,
            sentinel_writer=sentinel_writer,
            trigger_interval="1 second",
        )

        parquet_query, metrics_query, sentinel_query, active_queries = result
        self.assertEqual(active_queries, [parquet_query, metrics_query, sentinel_query])
        for writer in (parquet_writer, metrics_writer, sentinel_writer):
            self.assertIn(("trigger", {"processingTime": "1 second"}), writer.operations)
            self.assertIn(("start",), writer.operations)

    def test_prediction_parquet_columns_match_existing_contract(self) -> None:
        self.assertEqual(PREDICTION_PARQUET_COLUMNS[0], "flow_id")
        self.assertIn("prediction_label", PREDICTION_PARQUET_COLUMNS)
        self.assertEqual(PREDICTION_PARQUET_COLUMNS[-1], "label")

    def test_group_runtime_queries_without_sentinel(self) -> None:
        parquet_query = object()
        metrics_query = object()

        data_queries, all_queries = group_runtime_queries(
            parquet_query=parquet_query,
            metrics_query=metrics_query,
            sentinel_query=None,
        )

        self.assertEqual(
            data_queries,
            [
                (parquet_query, "predictions_parquet"),
                (metrics_query, "metrics"),
            ],
        )
        self.assertEqual(all_queries, data_queries)

    def test_group_runtime_queries_with_sentinel(self) -> None:
        parquet_query = object()
        metrics_query = object()
        sentinel_query = object()

        data_queries, all_queries = group_runtime_queries(
            parquet_query=parquet_query,
            metrics_query=metrics_query,
            sentinel_query=sentinel_query,
        )

        self.assertEqual(all_queries[:-1], data_queries)
        self.assertEqual(all_queries[-1], (sentinel_query, "input_sentinel"))


if __name__ == "__main__":
    unittest.main()
