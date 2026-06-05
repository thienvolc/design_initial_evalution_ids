from __future__ import annotations

import random

import pandas as pd

from ids_platform.streaming.replay.config import ReplayTimingConfig


class ReplayPerturbation:
    def __init__(self, config: ReplayTimingConfig) -> None:
        self.config = config

    def enabled(self) -> bool:
        return self.config.reorder_window_size > 1 or (
            self.config.late_event_ratio > 0 and self.config.late_event_max_sec > 0
        )

    def apply(self, frame: pd.DataFrame, *, chunk_index: int, rng: random.Random) -> pd.DataFrame:
        if self.config.reorder_window_size > 1:
            frame = reorder_by_window(
                frame,
                window_size=self.config.reorder_window_size,
                seed=self.config.random_seed + chunk_index,
            )

        if self.config.late_event_ratio > 0 and self.config.late_event_max_sec > 0:
            frame = make_events_late(
                frame,
                ratio=self.config.late_event_ratio,
                max_delay_sec=self.config.late_event_max_sec,
                rng=rng,
            )

        return frame


def reorder_by_window(frame: pd.DataFrame, *, window_size: int, seed: int) -> pd.DataFrame:
    if frame.empty or window_size <= 1:
        return frame

    chunks = [
        frame.iloc[start: start + window_size].sample(
            frac=1,
            random_state=seed + start
        )
        for start in range(0, len(frame), window_size)
    ]

    return pd.concat(chunks, ignore_index=True)


def make_events_late(
        frame: pd.DataFrame,
        *,
        ratio: float,
        max_delay_sec: float,
        rng: random.Random
) -> pd.DataFrame:
    if frame.empty or "event_time" not in frame.columns:
        return frame

    event_times = pd.to_datetime(frame["event_time"], errors="coerce", utc=True)
    valid_indices = event_times.dropna().index.tolist()

    if not valid_indices:
        return frame

    late_count = int(len(valid_indices) * ratio)
    if late_count <= 0:
        return frame

    updated = frame.copy()
    for index in rng.sample(valid_indices, k=min(late_count, len(valid_indices))):
        shift_seconds = rng.uniform(0.001, max_delay_sec)
        event_times.loc[index] -= pd.to_timedelta(shift_seconds, unit="s")

    updated["event_time"] = event_times
    return updated
