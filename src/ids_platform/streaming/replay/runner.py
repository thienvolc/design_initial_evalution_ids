from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Iterator, Mapping

from ids_platform.streaming.replay.config import ReplayConfig, ReplayRuntimeConfig
from ids_platform.streaming.replay.encoder import ReplayRecordEncoder
from ids_platform.streaming.replay.perturbation import ReplayPerturbation
from ids_platform.streaming.replay.publisher import ReplayPublisher, ReplayPublisherStats
from ids_platform.streaming.replay.rate_limiter import sleep_for_rate_limit
from ids_platform.streaming.replay.scheduler import ReplayScheduler

_REPLAY_STABILITY_MAX_GAP_MS = 1_000.0


@dataclass(frozen=True, slots=True)
class ReplayRunSummary:
    sent_rows: int
    target_rows: int
    target_rps: float | None
    actual_elapsed_sec: float
    actual_rps: float | None
    emit_gap_p50_ms: float | None
    emit_gap_p95_ms: float | None
    emit_gap_p99_ms: float | None
    emit_gap_max_ms: float | None
    tick_lag_p95_ms: float | None
    tick_lag_max_ms: float | None
    publisher: ReplayPublisherStats
    status: str
    failure_reason: str

    def as_dict(self, *, prefix: str = "replay_") -> dict:
        return {
            f"{prefix}status": self.status,
            f"{prefix}failure_reason": self.failure_reason,
            f"{prefix}sent_rows": self.sent_rows,
            f"{prefix}target_rows": self.target_rows,
            f"{prefix}target_rps": self.target_rps,
            f"{prefix}actual_elapsed_sec": self.actual_elapsed_sec,
            f"{prefix}actual_rps": self.actual_rps,
            f"{prefix}emit_gap_p50_ms": self.emit_gap_p50_ms,
            f"{prefix}emit_gap_p95_ms": self.emit_gap_p95_ms,
            f"{prefix}emit_gap_p99_ms": self.emit_gap_p99_ms,
            f"{prefix}emit_gap_max_ms": self.emit_gap_max_ms,
            f"{prefix}tick_lag_p95_ms": self.tick_lag_p95_ms,
            f"{prefix}tick_lag_max_ms": self.tick_lag_max_ms,
            f"{prefix}producer_buffer_stall_count": self.publisher.buffer_stall_count,
            f"{prefix}producer_buffer_stall_sec": self.publisher.buffer_stall_sec,
            f"{prefix}producer_flush_sec": self.publisher.flush_sec,
        }


class _ReplayEmitMonitor:
    def __init__(self, *, target_rows: int, target_rps: float | None, started_at: float) -> None:
        self.target_rows = max(int(target_rows), 0)
        self.target_rps = target_rps
        self.started_at = float(started_at)
        self.sent_rows = 0
        self._last_emit_at: float | None = None
        self._emit_gaps_ms: list[float] = []
        self._tick_lags_ms: list[float] = []

    def mark_emit(self) -> None:
        emitted_at = time.perf_counter()
        if self._last_emit_at is not None:
            self._emit_gaps_ms.append((emitted_at - self._last_emit_at) * 1000.0)
        self._last_emit_at = emitted_at
        self.sent_rows += 1

    def mark_tick_lag(self, lag_ms: float) -> None:
        self._tick_lags_ms.append(max(float(lag_ms), 0.0))

    def summary(self, *, finished_at: float, publisher: ReplayPublisherStats) -> ReplayRunSummary:
        actual_elapsed_sec = max(float(finished_at) - self.started_at, 0.0)
        actual_rps = self.sent_rows / actual_elapsed_sec if actual_elapsed_sec > 0 else None
        emit_gap_p50_ms = _percentile(self._emit_gaps_ms, 0.50)
        emit_gap_p95_ms = _percentile(self._emit_gaps_ms, 0.95)
        emit_gap_p99_ms = _percentile(self._emit_gaps_ms, 0.99)
        emit_gap_max_ms = max(self._emit_gaps_ms) if self._emit_gaps_ms else None
        tick_lag_p95_ms = _percentile(self._tick_lags_ms, 0.95)
        tick_lag_max_ms = max(self._tick_lags_ms) if self._tick_lags_ms else None
        failures = _load_generator_failures(
            target_rps=self.target_rps,
            actual_rps=actual_rps,
            emit_gap_max_ms=emit_gap_max_ms,
            tick_lag_max_ms=tick_lag_max_ms,
            buffer_stall_count=publisher.buffer_stall_count,
        )
        return ReplayRunSummary(
            sent_rows=int(self.sent_rows),
            target_rows=int(self.target_rows),
            target_rps=self.target_rps,
            actual_elapsed_sec=actual_elapsed_sec,
            actual_rps=actual_rps,
            emit_gap_p50_ms=emit_gap_p50_ms,
            emit_gap_p95_ms=emit_gap_p95_ms,
            emit_gap_p99_ms=emit_gap_p99_ms,
            emit_gap_max_ms=emit_gap_max_ms,
            tick_lag_p95_ms=tick_lag_p95_ms,
            tick_lag_max_ms=tick_lag_max_ms,
            publisher=publisher,
            status="unstable" if failures else "ok",
            failure_reason=";".join(failures),
        )


def run_replay_job(config: ReplayConfig) -> int:
    return run_replay_job_with_summary(config).sent_rows


def run_replay_job_with_summary(config: ReplayConfig) -> ReplayRunSummary:
    source = config.source.materialize() if hasattr(config.source, "materialize") else config.source
    publisher = ReplayPublisher(config.runtime)
    encoder = ReplayRecordEncoder(config.runtime.run_tag, phase=config.phase)
    perturbation = ReplayPerturbation(config.timing)
    rng = random.Random(int(config.timing.random_seed))
    started_at = time.perf_counter()
    monitor = _ReplayEmitMonitor(
        target_rows=source.table.num_rows,
        target_rps=_target_rps(config, total_rows=source.table.num_rows),
        started_at=started_at,
    )

    if config.rate.is_throttled() and config.rate.uses_burst_pacing():
        _publish_burst_chunks(
            source=source,
            rate=config.rate,
            perturbation=perturbation,
            rng=rng,
            started_at=started_at,
            publisher=publisher,
            encoder=encoder,
            monitor=monitor,
        )
    else:
        rows = _iter_replay_rows(source=source, perturbation=perturbation, rng=rng)
        if config.rate.is_throttled():
            _publish_ticked_rows(
                rows=rows,
                total_rows=source.table.num_rows,
                config=config,
                started_at=started_at,
                publisher=publisher,
                encoder=encoder,
                monitor=monitor,
            )
        else:
            _publish_unthrottled_rows(
                rows=rows,
                publisher=publisher,
                encoder=encoder,
                monitor=monitor,
            )

    publisher.flush_or_raise("publishing completion")
    return monitor.summary(finished_at=time.perf_counter(), publisher=publisher.stats())


def _publish_burst_chunks(
    *,
    source,
    rate,
    perturbation: ReplayPerturbation,
    rng: random.Random,
    started_at: float,
    publisher: ReplayPublisher,
    encoder: ReplayRecordEncoder,
    monitor: _ReplayEmitMonitor,
) -> None:
    sent_rows = 0
    for chunk_index, rows in enumerate(_iter_replay_chunks(source=source, perturbation=perturbation, rng=rng), start=1):
        for offset, row in enumerate(rows):
            encoded = encoder.encode_record(row, row_index=sent_rows + offset)
            publisher.publish(encoded)
            monitor.mark_emit()

        publisher.flush_or_raise("publishing attempt", chunk_index=chunk_index)
        sent_rows += len(rows)
        sleep_for_rate_limit(rate=rate, sent_rows=sent_rows, started=started_at)


def _iter_replay_chunks(
    *,
    source,
    perturbation: ReplayPerturbation,
    rng: random.Random,
) -> Iterator[list[Mapping]]:
    for chunk_index, batch in enumerate(source.table.to_batches(source.batch_size), start=1):
        if perturbation.enabled():
            frame = batch.to_pandas()
            frame = perturbation.apply(frame, chunk_index=chunk_index, rng=rng)
            yield frame.to_dict(orient="records")
        else:
            yield batch.to_pylist()


def _iter_replay_rows(*, source, perturbation: ReplayPerturbation, rng: random.Random) -> Iterator[Mapping]:
    for rows in _iter_replay_chunks(source=source, perturbation=perturbation, rng=rng):
        yield from rows


def _publish_unthrottled_rows(
    *,
    rows,
    publisher: ReplayPublisher,
    encoder: ReplayRecordEncoder,
    monitor: _ReplayEmitMonitor,
) -> None:
    for row in rows:
        encoded = encoder.encode_record(row, row_index=monitor.sent_rows)
        publisher.publish(encoded)
        monitor.mark_emit()
        publisher.poll(0)


def _publish_ticked_rows(
    *,
    rows,
    total_rows: int,
    config: ReplayConfig,
    started_at: float,
    publisher: ReplayPublisher,
    encoder: ReplayRecordEncoder,
    monitor: _ReplayEmitMonitor,
) -> None:
    scheduler = ReplayScheduler(rate=config.rate, total_rows=total_rows, started_at=started_at)
    tick_index = 1

    while monitor.sent_rows < total_rows:
        tick = scheduler.tick(tick_index=tick_index, sent_rows=monitor.sent_rows)

        for _ in range(tick.due_rows):
            try:
                row = next(rows)
            except StopIteration:
                return
            encoded = encoder.encode_record(row, row_index=monitor.sent_rows)
            publisher.publish(encoded)
            monitor.mark_emit()

        publisher.poll(0)
        monitor.mark_tick_lag(scheduler.lag_ms(tick))
        if monitor.sent_rows >= total_rows:
            break

        scheduler.sleep_until(tick)
        tick_index += 1


def publish_input_sentinel(runtime: ReplayRuntimeConfig) -> None:
    publisher = ReplayPublisher(runtime)
    encoder = ReplayRecordEncoder(runtime.run_tag)
    publisher.publish_sentinel(encoder.encode_input_sentinel())


def _target_rps(config: ReplayConfig, *, total_rows: int) -> float | None:
    if not config.rate.is_throttled():
        return None
    expected_elapsed = config.rate.expected_elapsed_for_rows(total_rows)
    if expected_elapsed <= 0:
        return None
    return float(total_rows) / expected_elapsed


def _percentile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = min(max(round((len(ordered) - 1) * float(quantile)), 0), len(ordered) - 1)
    return float(ordered[index])


def _load_generator_failures(
    *,
    target_rps: float | None,
    actual_rps: float | None,
    emit_gap_max_ms: float | None,
    tick_lag_max_ms: float | None,
    buffer_stall_count: int,
) -> list[str]:
    failures: list[str] = []
    if target_rps is not None:
        if actual_rps is None or actual_rps < target_rps * 0.95:
            failures.append("replay_actual_rps_below_target")
    if emit_gap_max_ms is not None and emit_gap_max_ms > _REPLAY_STABILITY_MAX_GAP_MS:
        failures.append("replay_emit_gap_spike")
    if tick_lag_max_ms is not None and tick_lag_max_ms > _REPLAY_STABILITY_MAX_GAP_MS:
        failures.append("replay_tick_lag_spike")
    if buffer_stall_count > 0:
        failures.append("replay_producer_buffer_stall")
    return failures
