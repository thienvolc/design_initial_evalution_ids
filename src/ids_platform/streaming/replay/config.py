from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds

from ids_platform.common.paths import resolve_project_path


@dataclass(frozen=True)
class RateStep:
    rows_per_sec: float
    duration_sec: float


@dataclass(frozen=True, slots=True)
class ReplayRatePlan:
    rows_per_sec: float
    schedule: tuple[RateStep, ...]

    def total_seconds(self) -> float:
        return sum(step.duration_sec for step in self.schedule)

    def expected_elapsed_for_rows(self, sent_rows: int) -> float:
        if sent_rows <= 0:
            return 0.0

        if self.schedule:
            return self._expected_elapsed_by_schedule(sent_rows)

        if self.rows_per_sec == 0:
            return 0.0

        return sent_rows / self.rows_per_sec

    def is_throttled(self) -> bool:
        return bool(self.schedule) or self.rows_per_sec > 0

    def _expected_elapsed_by_schedule(self, sent_rows: int) -> float:
        remaining_rows = float(sent_rows)
        elapsed_seconds = 0.0
        last_rows_per_sec = 0.0

        for step in self.schedule:
            if step.rows_per_sec == 0:
                elapsed_seconds += step.duration_sec
                continue

            last_rows_per_sec = step.rows_per_sec

            step_capacity = step.rows_per_sec * step.duration_sec
            rows_sent = min(remaining_rows, step_capacity)

            elapsed_seconds += rows_sent / step.rows_per_sec
            remaining_rows -= rows_sent

            if remaining_rows <= 0:
                return elapsed_seconds

        if last_rows_per_sec == 0:
            return elapsed_seconds

        return elapsed_seconds + remaining_rows / last_rows_per_sec


@dataclass(frozen=True)
class ReplaySource:
    table: pa.Table
    batch_size: int


@dataclass(frozen=True)
class ReplayRuntimeConfig:
    run_tag: str
    bootstrap_servers: str
    topic: str


@dataclass(frozen=True)
class ReplayTimingConfig:
    random_seed: int
    trace_order_column: str
    reorder_window_size: int
    late_event_ratio: float
    late_event_max_sec: float


@dataclass(frozen=True)
class ReplayConfig:
    source: ReplaySource
    runtime: ReplayRuntimeConfig
    rate: ReplayRatePlan
    timing: ReplayTimingConfig
    phase: str = "measure"


@dataclass(frozen=True, slots=True)
class ReplaySourceFactory:
    dataset_path: Path = resolve_project_path("data/gold/splits/test.parquet")
    batch_size: int = 5_000
    row_limit: int | None = None

    def create(self) -> ReplaySource:
        dataset = ds.dataset(self.dataset_path, format="parquet")

        table = dataset.to_table(columns=dataset.schema.names)
        if "flow_id" not in table.column_names:
            table = table.append_column(
                "flow_id",
                pa.array((f"flow-{index}" for index in range(table.num_rows))),
            )
        if "event_time" not in table.column_names:
            table = table.append_column("event_time", table["timestamp"])

        if self.row_limit is not None and int(self.row_limit) >= 0:
            table = table.slice(0, int(self.row_limit))

        return ReplaySource(table=table, batch_size=self.batch_size)
