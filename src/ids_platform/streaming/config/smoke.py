from __future__ import annotations

from ids_platform.streaming.config.common import DEFAULT_REPLAY_RUNTIME_CONFIG, DEFAULT_TIMING_CONFIG, NO_THROTTLE_RATE
from ids_platform.streaming.replay.config import RateStep, ReplayConfig, ReplayRatePlan, ReplaySourceFactory


SMOKE_GATE_REPLAY_CONFIG = ReplayConfig(
    source=ReplaySourceFactory(batch_size=2_000).create(),
    runtime=DEFAULT_REPLAY_RUNTIME_CONFIG,
    rate=NO_THROTTLE_RATE,
    timing=DEFAULT_TIMING_CONFIG,
)

SMOKE_GATE_WARMUP_REPLAY_CONFIG = ReplayConfig(
    source=ReplaySourceFactory(batch_size=2_000).create(),
    runtime=DEFAULT_REPLAY_RUNTIME_CONFIG,
    rate=ReplayRatePlan(
        rows_per_sec=0.0,
        schedule=(RateStep(rows_per_sec=1_000, duration_sec=15),),
    ),
    timing=DEFAULT_TIMING_CONFIG,
)
