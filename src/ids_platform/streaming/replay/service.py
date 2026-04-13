from __future__ import annotations

import json
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd
import pyarrow.dataset as ds


def resolve_trace_column(schema_names: Iterable[str], preferred: str) -> str:
    names = list(schema_names)
    candidates = [preferred, "event_time", "timestamp", "Timestamp"]
    for column_name in candidates:
        if column_name and column_name in names:
            return column_name
    return ""


def load_trace_batches(
    parquet_path: Path,
    *,
    columns: list[str],
    batch_size: int,
    trace_order_column: str,
    force_sort_input: bool,
) -> tuple[list, str]:
    dataset = ds.dataset(parquet_path, format="parquet")
    available_columns = set(dataset.schema.names)
    readable_columns = [column_name for column_name in columns if column_name in available_columns]

    if not readable_columns:
        return [], ""

    table = dataset.to_table(columns=readable_columns)
    trace_column = resolve_trace_column(table.column_names, trace_order_column)
    if trace_column and force_sort_input:
        table = table.sort_by([(trace_column, "ascending")])

    return list(table.to_batches(max_chunksize=batch_size)), trace_column


def iter_replay_batches(record_batches: list, max_rows: int):
    if not record_batches:
        return

    if not max_rows or max_rows <= 0:
        for batch in record_batches:
            yield batch.to_pandas()
        return

    remaining = int(max_rows)
    for batch in record_batches:
        row_count = int(batch.num_rows)
        if row_count <= 0:
            continue

        if row_count > remaining:
            frame = batch.slice(0, remaining).to_pandas()
        else:
            frame = batch.to_pandas()

        yield frame
        remaining -= len(frame)
        if remaining <= 0:
            break


def to_json_value(row: pd.Series) -> str:
    payload: dict[str, object] = {}
    for key, value in row.items():
        if pd.isna(value):
            payload[str(key)] = None
        elif isinstance(value, (pd.Timestamp, datetime)):
            payload[str(key)] = pd.Timestamp(value).isoformat()
        elif hasattr(value, "item"):
            payload[str(key)] = value.item()
        else:
            payload[str(key)] = value
    return json.dumps(payload, ensure_ascii=False)


def expected_elapsed_for_rows(sent_rows: int, schedule: list[tuple[float, float]]) -> float:
    if sent_rows <= 0:
        return 0.0

    remaining = float(sent_rows)
    elapsed = 0.0
    for rows_per_second, duration_seconds in schedule:
        capacity = rows_per_second * duration_seconds
        take = min(remaining, capacity)
        elapsed += take / rows_per_second
        remaining -= take
        if remaining <= 0:
            return elapsed

    if remaining > 0 and schedule:
        elapsed += remaining / schedule[-1][0]
    return elapsed


def apply_reorder(frame: pd.DataFrame, *, window_size: int, seed_base: int) -> pd.DataFrame:
    if window_size <= 1 or frame.empty:
        return frame

    shuffled_chunks: list[pd.DataFrame] = []
    for start in range(0, len(frame), window_size):
        window = frame.iloc[start : start + window_size]
        shuffled_chunks.append(window.sample(frac=1, random_state=seed_base + start))
    return pd.concat(shuffled_chunks, axis=0).reset_index(drop=True)


def apply_lateness(frame: pd.DataFrame, *, ratio: float, max_sec: float, rng: random.Random) -> pd.DataFrame:
    if frame.empty or ratio <= 0 or max_sec <= 0:
        return frame
    if "event_time" not in frame.columns:
        return frame

    event_times = pd.to_datetime(frame["event_time"], errors="coerce", utc=True)
    valid_indices = [int(index) for index in frame.index if not pd.isna(event_times.loc[index])]
    if not valid_indices:
        return frame

    late_count = max(int(len(valid_indices) * ratio), 0)
    late_count = min(late_count, len(valid_indices))
    if late_count <= 0:
        return frame

    selected_indices = rng.sample(valid_indices, late_count)
    for index in selected_indices:
        shift_seconds = rng.uniform(0.001, max_sec)
        event_times.loc[index] = event_times.loc[index] - pd.to_timedelta(shift_seconds, unit="s")

    updated = frame.copy()
    updated["event_time"] = event_times
    return updated


def ensure_event_time_column(chunk: pd.DataFrame) -> pd.DataFrame:
    if "event_time" in chunk.columns:
        return chunk

    updated = chunk.copy()
    if "timestamp" in updated.columns:
        updated["event_time"] = updated["timestamp"]
    elif "Timestamp" in updated.columns:
        updated["event_time"] = updated["Timestamp"]
    else:
        updated["event_time"] = datetime.now(timezone.utc).isoformat()
    return updated


def normalize_flow_id(record: pd.Series, *, row_index: int) -> pd.Series:
    flow_id_value = record["flow_id"] if "flow_id" in record.index else None
    if isinstance(flow_id_value, pd.Series):
        flow_id_value = flow_id_value.iloc[0] if not flow_id_value.empty else None
    if flow_id_value is None or bool(pd.isna(flow_id_value)):
        record["flow_id"] = f"flow-{row_index}"
    return record
