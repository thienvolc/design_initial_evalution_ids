from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class LayerCFaultMatrixOptions:
    config: str
    model: str
    feature_set: str
    scenarios: tuple[str, ...]
    warmup_rows: int
    warmup_rows_per_sec: float
    warmup_rate_schedule: str
    fault_delay_sec: int
    post_fault_rows: int
    post_fault_rows_per_sec: float
    batch_size: int
    trace_input_parquet: str
    trace_order_column: str
    slowdown_rows_per_sec: float
    producer_restart_pause_sec: int
    replay_retries: int
    replay_retry_wait_sec: int
    stream_run_seconds: int
    startup_wait_sec: int
    metrics_timeout_sec: int
    execution_mode: str
    python_executable: str
    bootstrap_servers: str
    summary_csv: str


@dataclass(frozen=True)
class LayerCFaultInjectionResult:
    ok: bool
    stream_process: object | None


@dataclass(frozen=True)
class LayerCPreRecoveryResult:
    ok: bool
    stream_process: object | None
    fault_time: datetime | None
    fault_epoch_ms: int


@dataclass(frozen=True)
class LayerCKafkaReadinessResult:
    host_ready: bool
    docker_ready: bool
    host_state: str
    docker_state: str


@dataclass(frozen=True)
class LayerCShutdownResult:
    graceful_shutdown_complete: bool
    stream_process: object | None
