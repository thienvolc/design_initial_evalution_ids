from __future__ import annotations

import json
import shutil
import time
from pathlib import Path

from ids_platform.streaming.runtime.query import safe_tag

ARTIFACT_COLUMNS = (
    "flow_id",
    "model_name",
    "feature_set",
    "run_tag",
    "benchmark_phase",
    "prediction_label",
    "prediction_score",
    "threshold_used",
    "kafka_partition",
    "kafka_offset",
    "event_time_ts",
    "ingest_time",
    "emit_time",
    "source_to_ingest_ms",
    "processing_ms",
    "end_to_end_ms",
    "event_lateness_ms",
    "is_late_event",
    "label_binary",
    "label",
)


def collect_prediction_response_artifact(
    *,
    bootstrap_servers: str,
    input_topic: str,
    prediction_topic: str,
    run_tag: str,
    artifact_output: Path,
    expected_phase_rows: dict[str, int],
    timeout_sec: int,
    idle_sec: int,
) -> dict:
    expected = {
        _normalise_phase(phase): max(int(rows), 0)
        for phase, rows in expected_phase_rows.items()
        if max(int(rows), 0) > 0
    }
    if not expected:
        return {"artifact_collect_status": "skipped", "artifact_collect_rows": 0}

    rows = _collect_prediction_rows(
        bootstrap_servers=bootstrap_servers,
        prediction_topic=prediction_topic,
        run_tag=run_tag,
        expected_phase_rows=expected,
        timeout_sec=timeout_sec,
        idle_sec=idle_sec,
    )
    labels = _collect_input_labels(
        bootstrap_servers=bootstrap_servers,
        input_topic=input_topic,
        run_tag=run_tag,
        required_offsets={
            (int(row["kafka_partition"]), int(row["kafka_offset"]))
            for row in rows
            if row.get("kafka_partition") is not None and row.get("kafka_offset") is not None
        },
        timeout_sec=timeout_sec,
        idle_sec=idle_sec,
    )

    for row in rows:
        label = labels.get((int(row["kafka_partition"]), int(row["kafka_offset"])), {})
        row["label_binary"] = label.get("label_binary")
        row["label"] = label.get("label")

    _write_artifact(rows, Path(artifact_output))
    actual_counts = _phase_counts(rows)
    missing = {
        phase: expected_count - int(actual_counts.get(phase, 0))
        for phase, expected_count in expected.items()
        if int(actual_counts.get(phase, 0)) != expected_count
    }
    if missing:
        raise RuntimeError(f"prediction artifact collection incomplete: {missing}")

    return {
        "artifact_collect_status": "ok",
        "artifact_collect_rows": len(rows),
        "artifact_collect_labeled_rows": sum(
            1 for row in rows if row.get("label_binary") is not None
        ),
    }


def _collect_prediction_rows(
    *,
    bootstrap_servers: str,
    prediction_topic: str,
    run_tag: str,
    expected_phase_rows: dict[str, int],
    timeout_sec: int,
    idle_sec: int,
) -> list[dict]:
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"prediction-artifact-{safe_tag(run_tag)}-{int(time.time() * 1000)}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    rows: list[dict] = []
    counts = {phase: 0 for phase in expected_phase_rows}
    deadline = time.time() + max(int(timeout_sec), 1)
    idle_deadline: float | None = None
    complete_deadline: float | None = None
    try:
        consumer.subscribe([prediction_topic])
        while time.time() < deadline and (
            idle_deadline is None or time.time() < idle_deadline
        ):
            if complete_deadline is not None and time.time() >= complete_deadline:
                break
            message = consumer.poll(0.5)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(str(message.error()))
            payload = _decode_json(message.value())
            if str(payload.get("run_tag") or "") != str(run_tag):
                continue
            phase = _normalise_phase(payload.get("benchmark_phase"))
            if phase not in expected_phase_rows:
                continue
            rows.append(_normalise_prediction_row(payload))
            counts[phase] = counts.get(phase, 0) + 1
            idle_deadline = time.time() + max(int(idle_sec), 1)
            if _counts_complete(counts, expected_phase_rows) and complete_deadline is None:
                complete_deadline = time.time() + min(max(float(idle_sec), 1.0), 2.0)
    finally:
        consumer.close()

    return rows


def _collect_input_labels(
    *,
    bootstrap_servers: str,
    input_topic: str,
    run_tag: str,
    required_offsets: set[tuple[int, int]],
    timeout_sec: int,
    idle_sec: int,
) -> dict[tuple[int, int], dict]:
    if not required_offsets:
        return {}

    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"prediction-labels-{safe_tag(run_tag)}-{int(time.time() * 1000)}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    labels: dict[tuple[int, int], dict] = {}
    deadline = time.time() + max(int(timeout_sec), 1)
    idle_deadline: float | None = None
    try:
        consumer.subscribe([input_topic])
        while time.time() < deadline and (
            idle_deadline is None or time.time() < idle_deadline
        ):
            if len(labels) >= len(required_offsets):
                break
            message = consumer.poll(0.5)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(str(message.error()))
            key = (int(message.partition()), int(message.offset()))
            if key not in required_offsets:
                continue
            payload = _decode_json(message.value())
            if str(payload.get("replay_run_tag") or "") != str(run_tag):
                continue
            if int(payload.get("is_control_record") or 0) != 0:
                continue
            labels[key] = {
                "label_binary": payload.get("label_binary"),
                "label": payload.get("label"),
            }
            idle_deadline = time.time() + max(int(idle_sec), 1)
    finally:
        consumer.close()

    return labels


def _write_artifact(rows: list[dict], artifact_output: Path) -> None:
    if artifact_output.exists():
        if artifact_output.is_dir():
            shutil.rmtree(artifact_output)
        else:
            artifact_output.unlink()
    artifact_output.parent.mkdir(parents=True, exist_ok=True)

    import pandas as pd

    frame = pd.DataFrame(rows)
    for column in ARTIFACT_COLUMNS:
        if column not in frame.columns:
            frame[column] = None
    frame = frame.loc[:, list(ARTIFACT_COLUMNS)]
    frame.to_parquet(artifact_output, index=False)


def _normalise_prediction_row(payload: dict) -> dict:
    row = dict(payload)
    for column in ("kafka_partition", "kafka_offset", "prediction_label", "is_late_event"):
        if row.get(column) is not None:
            row[column] = int(row[column])
    for column in (
        "prediction_score",
        "threshold_used",
        "source_to_ingest_ms",
        "processing_ms",
        "end_to_end_ms",
        "event_lateness_ms",
    ):
        if row.get(column) is not None:
            row[column] = float(row[column])
    row["benchmark_phase"] = _normalise_phase(row.get("benchmark_phase"))
    return row


def _decode_json(value) -> dict:
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    return json.loads(str(value))


def _normalise_phase(value) -> str:
    phase = str(value or "").strip().lower()
    if phase in {"warmup", "cold_start", "pre_fault", "post_fault"}:
        return phase
    return "measure"


def _phase_counts(rows: list[dict]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        phase = _normalise_phase(row.get("benchmark_phase"))
        counts[phase] = counts.get(phase, 0) + 1
    return counts


def _counts_complete(counts: dict[str, int], expected: dict[str, int]) -> bool:
    return all(int(counts.get(phase, 0)) >= int(count) for phase, count in expected.items())
