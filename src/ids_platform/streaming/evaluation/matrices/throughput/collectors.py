from __future__ import annotations

import csv
import json
import os
import statistics
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from ids_platform.common.paths import resolve_project_path


class ResourceSampler:
    def __init__(self, *, interval_sec: float = 1.0) -> None:
        self.interval_sec = max(float(interval_sec), 0.1)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._samples: list[dict[str, float]] = []
        self._status = "not_started"

    def start(self) -> "ResourceSampler":
        try:
            import psutil  # noqa: F401
        except Exception:
            self._status = "psutil_missing"
            return self

        self._status = "running"
        self._thread = threading.Thread(target=self._sample_loop, name="benchmark-resource-sampler", daemon=True)
        self._thread.start()
        return self

    def stop(self) -> dict:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=max(self.interval_sec * 2.0, 1.0))

        if not self._samples:
            return {
                "resource_status": self._status,
                "resource_samples": 0,
                "cpu_avg_pct": "",
                "cpu_max_pct": "",
                "mem_avg_mb": "",
                "mem_max_mb": "",
            }

        cpu_values = [sample["cpu_pct"] for sample in self._samples]
        mem_values = [sample["mem_mb"] for sample in self._samples]
        return {
            "resource_status": "ok",
            "resource_samples": len(self._samples),
            "cpu_avg_pct": statistics.fmean(cpu_values),
            "cpu_max_pct": max(cpu_values),
            "mem_avg_mb": statistics.fmean(mem_values),
            "mem_max_mb": max(mem_values),
        }

    def _sample_loop(self) -> None:
        import psutil

        root = psutil.Process(os.getpid())
        self._prime_cpu_percent(root)
        while not self._stop.wait(self.interval_sec):
            try:
                processes = [root, *root.children(recursive=True)]
                cpu_pct = 0.0
                mem_bytes = 0
                for process in processes:
                    try:
                        cpu_pct += float(process.cpu_percent(None))
                        mem_bytes += int(process.memory_info().rss)
                    except (psutil.Error, OSError):
                        continue
                self._samples.append(
                    {
                        "cpu_pct": cpu_pct,
                        "mem_mb": mem_bytes / (1024.0 * 1024.0),
                    }
                )
            except Exception:
                self._status = "sample_error"

    @staticmethod
    def _prime_cpu_percent(root) -> None:
        try:
            root.cpu_percent(None)
            for child in root.children(recursive=True):
                child.cpu_percent(None)
        except Exception:
            return


class KafkaLagSampler:
    def __init__(
        self,
        *,
        bootstrap_servers: str,
        topic: str,
        artifact_output: Path,
        run_tag: str,
        prediction_topic: str = "",
        interval_sec: float = 1.0,
        output_dir: str = "artifacts/streaming/kafka_lag_timeseries",
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.prediction_topic = str(prediction_topic or "").strip()
        self.artifact_output = Path(artifact_output)
        self.run_tag = run_tag
        self.interval_sec = max(float(interval_sec), 0.25)
        self.output_dir = output_dir
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._phase = "startup"
        self._thread: threading.Thread | None = None
        self._consumer = None
        self._prediction_consumer = None
        self._topic_partition_cls = None
        self._samples: list[dict] = []
        self._status = "not_started"
        self._control_tail_records = 0
        self._processed_offsets: dict[int, int] = {}

    def start(self) -> "KafkaLagSampler":
        try:
            from confluent_kafka import Consumer, TopicPartition
        except Exception:
            self._status = "kafka_client_missing"
            return self

        self._consumer = Consumer(
            {
                "bootstrap.servers": self.bootstrap_servers,
                "group.id": f"lag-sampler-{self.run_tag}-{int(time.time() * 1000)}",
                "enable.auto.commit": False,
            }
        )
        if self.prediction_topic:
            self._prediction_consumer = Consumer(
                {
                    "bootstrap.servers": self.bootstrap_servers,
                    "group.id": f"lag-response-{self.run_tag}-{int(time.time() * 1000)}",
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": False,
                }
            )
            self._prediction_consumer.subscribe([self.prediction_topic])
        self._topic_partition_cls = TopicPartition
        self._status = "running"
        self._thread = threading.Thread(target=self._sample_loop, name="benchmark-kafka-lag-sampler", daemon=True)
        self._thread.start()
        return self

    def set_phase(self, phase: str) -> None:
        with self._lock:
            self._phase = str(phase or "").strip().lower() or "unknown"

    def set_control_tail_records(self, count: int) -> None:
        with self._lock:
            self._control_tail_records = max(int(count), 0)

    def mark(self, event: str) -> None:
        self._sample(event=str(event or "").strip())

    def stop(self) -> dict:
        self._sample(event="sampler_stop")
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=max(self.interval_sec * 2.0, 1.0))
        if self._consumer is not None:
            self._consumer.close()
            self._consumer = None
        if self._prediction_consumer is not None:
            self._prediction_consumer.close()
            self._prediction_consumer = None

        output_path = self._write_timeseries()
        summary = self._summary()
        summary["kafka_lag_timeseries_path"] = str(output_path) if output_path is not None else ""
        return summary

    def _sample_loop(self) -> None:
        while not self._stop.wait(self.interval_sec):
            self._sample()

    def _sample(self, *, event: str = "") -> None:
        if self._consumer is None or self._topic_partition_cls is None:
            return

        with self._lock:
            phase = self._phase
            control_tail_records = self._control_tail_records

        ts_epoch_ms = int(time.time() * 1000)
        sample = {
            "ts_utc": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "ts_epoch_ms": ts_epoch_ms,
            "run_tag": self.run_tag,
            "topic": self.topic,
            "phase": phase,
            "event": event,
            "lag_records": "",
            "raw_lag_records": "",
            "control_tail_records": control_tail_records,
            "high_watermark_max": "",
            "processed_offset_max": "",
            "partition_count": "",
            "status": "ok",
        }

        try:
            partitions = self._topic_partitions()
            if not partitions:
                sample["status"] = "topic_partitions_missing"
                self._samples.append(sample)
                return

            processed_offsets, status = self._processed_offsets_with_status()
            if status != "ok":
                sample["status"] = status
                self._samples.append(sample)
                return

            raw_lag = 0
            high_watermark_max = 0
            processed_offset_max = -1
            for partition in partitions:
                low, high = self._consumer.get_watermark_offsets(
                    self._topic_partition_cls(self.topic, int(partition)),
                    timeout=5.0,
                    cached=False,
                )
                high_watermark_max = max(high_watermark_max, int(high))
                processed_offset = int(processed_offsets.get(int(partition), int(low) - 1))
                processed_offset_max = max(processed_offset_max, processed_offset)
                raw_lag += max(int(high) - (processed_offset + 1), 0)

            sample.update(
                {
                    "lag_records": max(int(raw_lag) - int(control_tail_records), 0),
                    "raw_lag_records": int(raw_lag),
                    "high_watermark_max": int(high_watermark_max),
                    "processed_offset_max": int(processed_offset_max),
                    "partition_count": len(partitions),
                    "status": status,
                }
            )
        except Exception as exc:
            sample["status"] = f"sample_error:{type(exc).__name__}"

        self._samples.append(sample)

    def _topic_partitions(self) -> list[int]:
        metadata = self._consumer.list_topics(topic=self.topic, timeout=5.0)
        topic_metadata = metadata.topics.get(self.topic)
        partitions = getattr(topic_metadata, "partitions", None) if topic_metadata is not None else None
        if not partitions:
            return []
        return sorted(int(partition) for partition in partitions.keys())

    def _processed_offsets_with_status(self) -> tuple[dict[int, int], str]:
        if self._prediction_consumer is None:
            return _artifact_max_offsets_with_status(self.artifact_output)

        while True:
            message = self._prediction_consumer.poll(0.0)
            if message is None:
                break
            if message.error():
                return dict(self._processed_offsets), f"prediction_read_error:{message.error()}"
            try:
                payload = json.loads(message.value().decode("utf-8"))
                if str(payload.get("run_tag") or "") != self.run_tag:
                    continue
                partition = int(payload["kafka_partition"])
                offset = int(payload["kafka_offset"])
                self._processed_offsets[partition] = max(
                    int(self._processed_offsets.get(partition, -1)),
                    offset,
                )
            except Exception as exc:
                return dict(self._processed_offsets), f"prediction_decode_error:{type(exc).__name__}"

        return dict(self._processed_offsets), "ok"

    def _write_timeseries(self) -> Path | None:
        if not self._samples:
            return None
        path = resolve_project_path(self.output_dir) / f"{_safe_file_tag(self.run_tag)}.csv"
        path.parent.mkdir(parents=True, exist_ok=True)
        fieldnames = [
            "ts_utc",
            "ts_epoch_ms",
            "run_tag",
            "topic",
            "phase",
            "event",
            "lag_records",
            "raw_lag_records",
            "control_tail_records",
            "high_watermark_max",
            "processed_offset_max",
            "partition_count",
            "status",
        ]
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self._samples)
        return path

    def _summary(self) -> dict:
        if not self._samples:
            return {
                "kafka_lag_sampler_status": self._status,
                "kafka_lag_samples": 0,
            }

        measure_samples = [
            sample
            for sample in self._samples
            if str(sample.get("phase") or "").startswith("measure")
            and str(sample.get("status") or "") == "ok"
            and _sample_lag(sample) is not None
        ]
        replay_end = next(
            (sample for sample in self._samples if sample.get("event") == "measure_replay_end"),
            None,
        )
        peak_sample = max(measure_samples, key=lambda sample: _sample_lag(sample) or 0) if measure_samples else None
        replay_end_lag = _sample_lag(replay_end) if replay_end is not None else None
        clear_sec = _lag_clear_seconds(measure_samples, replay_end)
        last_lag = _sample_lag(measure_samples[-1]) if measure_samples else None

        return {
            "kafka_lag_sampler_status": "ok",
            "kafka_lag_samples": len(self._samples),
            "kafka_lag_peak_records": _sample_lag(peak_sample) if peak_sample is not None else "",
            "kafka_lag_peak_phase": peak_sample.get("phase", "") if peak_sample is not None else "",
            "kafka_lag_at_replay_end_records": replay_end_lag if replay_end_lag is not None else "",
            "kafka_lag_clear_sec": clear_sec if clear_sec is not None else "",
            "kafka_lag_area_records_sec": _lag_area_records_sec(measure_samples),
            "kafka_lag_end_after_drain_records": last_lag if last_lag is not None else "",
        }


def summarize_kafka_offset_lag(
    *,
    bootstrap_servers: str,
    topic: str,
    artifact_output: Path,
    phase: str = "measure",
    control_tail_records: int = 0,
) -> dict:
    offsets = _artifact_max_offsets(Path(artifact_output), phase=phase)
    if not offsets:
        return {
            "kafka_lag_status": "artifact_offsets_missing",
            "kafka_lag_records_end": "",
            "kafka_lag_partitions": 0,
        }

    try:
        from confluent_kafka import Consumer, TopicPartition
    except Exception:
        return {
            "kafka_lag_status": "kafka_client_missing",
            "kafka_lag_records_end": "",
            "kafka_lag_partitions": len(offsets),
        }

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"lag-sampler-{int(time.time() * 1000)}",
            "enable.auto.commit": False,
        }
    )
    try:
        total_lag = 0
        high_watermarks: dict[int, int] = {}
        for partition, max_offset in offsets.items():
            low, high = consumer.get_watermark_offsets(
                TopicPartition(topic, int(partition)),
                timeout=10.0,
                cached=False,
            )
            high_watermarks[int(partition)] = int(high)
            total_lag += max((int(high) - 1) - int(max_offset), 0)
    finally:
        consumer.close()

    raw_lag = int(total_lag)
    data_lag = max(raw_lag - max(int(control_tail_records), 0), 0)
    return {
        "kafka_lag_status": "ok",
        "kafka_lag_records_end": data_lag,
        "kafka_lag_records_raw_end": raw_lag,
        "kafka_control_tail_records": max(int(control_tail_records), 0),
        "kafka_lag_partitions": len(offsets),
        "kafka_high_watermark_max": max(high_watermarks.values()) if high_watermarks else "",
    }


def _artifact_max_offsets(artifact_output: Path, *, phase: str) -> dict[int, int]:
    if not artifact_output.exists():
        return {}
    if artifact_output.is_dir() and not any(artifact_output.glob("*.parquet")):
        return {}

    import pandas as pd

    frame = pd.read_parquet(
        artifact_output,
        columns=["benchmark_phase", "kafka_partition", "kafka_offset"],
    )
    if phase and "benchmark_phase" in frame.columns:
        expected_phase = str(phase).strip().lower()
        if expected_phase:
            phases = frame["benchmark_phase"].fillna("measure").astype(str).str.lower()
            frame = frame[phases == expected_phase]
    if frame.empty:
        return {}

    frame = frame.dropna(subset=["kafka_partition", "kafka_offset"])
    if frame.empty:
        return {}

    partitions = pd.to_numeric(frame["kafka_partition"], errors="coerce")
    offsets = pd.to_numeric(frame["kafka_offset"], errors="coerce")
    clean = frame.assign(kafka_partition=partitions, kafka_offset=offsets).dropna(
        subset=["kafka_partition", "kafka_offset"]
    )
    if clean.empty:
        return {}

    grouped = clean.groupby("kafka_partition")["kafka_offset"].max()
    return {int(partition): int(offset) for partition, offset in grouped.items()}


def _artifact_max_offsets_with_status(artifact_output: Path) -> tuple[dict[int, int], str]:
    try:
        return _artifact_max_offsets(Path(artifact_output), phase=""), "ok"
    except Exception as exc:
        return {}, f"artifact_read_error:{type(exc).__name__}"


def _sample_lag(sample: dict | None) -> int | None:
    if not sample:
        return None
    try:
        value = sample.get("lag_records")
        if value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _sample_ts(sample: dict | None) -> float | None:
    if not sample:
        return None
    try:
        value = sample.get("ts_epoch_ms")
        if value == "":
            return None
        return float(value) / 1000.0
    except (TypeError, ValueError):
        return None


def _lag_clear_seconds(measure_samples: list[dict], replay_end: dict | None) -> float | None:
    replay_end_ts = _sample_ts(replay_end)
    replay_end_lag = _sample_lag(replay_end)
    if replay_end_ts is None or replay_end_lag is None:
        return None
    if replay_end_lag <= 0:
        return 0.0
    for sample in measure_samples:
        sample_ts = _sample_ts(sample)
        sample_lag = _sample_lag(sample)
        if sample_ts is None or sample_lag is None or sample_ts < replay_end_ts:
            continue
        if sample_lag <= 0:
            return float(sample_ts - replay_end_ts)
    return None


def _lag_area_records_sec(samples: list[dict]) -> float | str:
    if len(samples) < 2:
        return ""
    area = 0.0
    previous = samples[0]
    for sample in samples[1:]:
        previous_ts = _sample_ts(previous)
        sample_ts = _sample_ts(sample)
        previous_lag = _sample_lag(previous)
        if previous_ts is not None and sample_ts is not None and previous_lag is not None:
            area += max(sample_ts - previous_ts, 0.0) * float(previous_lag)
        previous = sample
    return area


def _safe_file_tag(value: str) -> str:
    safe = "".join(char if char.isalnum() or char in "._-" else "_" for char in str(value).strip())
    return safe or "unknown_run"
