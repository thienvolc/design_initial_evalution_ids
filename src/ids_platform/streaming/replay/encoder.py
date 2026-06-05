from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

from ids_platform.streaming.replay.serializer import replay_record_to_json


@dataclass(frozen=True, slots=True)
class EncodedReplayRecord:
    key: str
    value: str


@dataclass(frozen=True, slots=True)
class ReplayRecordEncoder:
    run_tag: str
    phase: str = "measure"

    def encode_record(self, row: Mapping[str, Any], *, row_index: int) -> EncodedReplayRecord:
        ingest_time = time.time()
        record = dict(row)
        record["source_ingest_ts"] = datetime.fromtimestamp(
            ingest_time,
            tz=timezone.utc,
        ).isoformat()
        record["source_ingest_epoch_ms"] = int(ingest_time * 1000)
        record["replay_run_tag"] = self.run_tag
        record["replay_row_index"] = int(row_index)
        record["benchmark_phase"] = self.phase
        key = str(record.get("flow_id") or f"{self.run_tag}__row_{row_index}")
        return EncodedReplayRecord(key=key, value=replay_record_to_json(record))

    def encode_input_sentinel(self) -> EncodedReplayRecord:
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()
        epoch_ms = int(now.timestamp() * 1000)
        run_tag = self.run_tag
        sentinel_record = {
            "flow_id": f"{run_tag}__input_sentinel",
            "replay_run_tag": run_tag,
            "benchmark_phase": "control",
            "event_time": now_iso,
            "timestamp": now_iso,
            "source_ingest_ts": now_iso,
            "source_ingest_epoch_ms": epoch_ms,
            "label_binary": None,
            "label": None,
            "is_control_record": 1,
            "control_type": "input_sentinel",
        }
        return EncodedReplayRecord(
            key=str(sentinel_record["flow_id"]),
            value=replay_record_to_json(sentinel_record),
        )

