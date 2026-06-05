from __future__ import annotations

import math
import time
from dataclasses import dataclass

from ids_platform.streaming.replay.config import ReplayRatePlan


@dataclass(frozen=True, slots=True)
class ReplayTick:
    index: int
    elapsed_sec: float
    deadline: float
    target_rows: int
    due_rows: int


class ReplayScheduler:
    def __init__(self, *, rate: ReplayRatePlan, total_rows: int, started_at: float) -> None:
        self.rate = rate
        self.total_rows = max(int(total_rows), 0)
        self.started_at = float(started_at)
        self.tick_seconds = max(float(rate.emit_interval_sec), 0.001)

    def tick(self, *, tick_index: int, sent_rows: int) -> ReplayTick:
        elapsed_sec = max(int(tick_index), 1) * self.tick_seconds
        target_rows = min(
            self.total_rows,
            int(math.floor(self.rate.expected_rows_for_elapsed(elapsed_sec))),
        )
        return ReplayTick(
            index=max(int(tick_index), 1),
            elapsed_sec=elapsed_sec,
            deadline=self.started_at + elapsed_sec,
            target_rows=target_rows,
            due_rows=max(target_rows - max(int(sent_rows), 0), 0),
        )

    @staticmethod
    def lag_ms(tick: ReplayTick, *, now: float | None = None) -> float:
        observed_at = time.perf_counter() if now is None else float(now)
        return max((observed_at - tick.deadline) * 1000.0, 0.0)

    @staticmethod
    def sleep_until(tick: ReplayTick) -> None:
        sleep_seconds = tick.deadline - time.perf_counter()
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

