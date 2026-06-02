# ADR-0001: Layer C Checkpoint/Sentinel Recovery Boundary

- Status: Accepted
- Date: 2026-04-24
- Updated: 2026-06-01
- Scope: Layer C smoke and recovery flow

## Context

Layer C evaluates whether the streaming flow can continue after controlled interruption. Earlier versions mixed host orchestration, YAML profiles, stop files, runtime logs, and dashboard-style observability. That made the scenario hard to reason about and easy to break.

The active system now uses Python config objects and keeps only two runtime coordination mechanisms:

- Kafka input sentinel for normal end-of-run shutdown
- Spark checkpoints for restart/recovery

## Decision

Layer C remains an official matrix, but the default script is a smoke gate. The smoke gate uses 1000 replay rows per segment, 500-row batches, and a 500 rps schedule to detect startup, replay, sentinel, metrics, parquet, and timeout failures.

Paper-scale recovery runs must be selected explicitly from Python config builders. They are not the default runbook path.

## Active Contract

- Runtime starts before replay and reads only records for the current `run_tag`.
- Replay emits data records, then the orchestration publishes an input sentinel.
- Runtime stops after observing the sentinel.
- Restart scenarios reuse checkpoints by setting runtime reset behavior appropriately.
- Evaluation reads operational `ids.metrics` plus prediction parquet and writes summary CSVs.

## Removed From Active Layer C

- YAML profile execution
- stop-file lifecycle
- log-marker readiness as the source of truth
- legacy fault orchestration packages
- dashboard/exporter-based evidence

## Remaining Risk

Spark can still print noisy shutdown warnings when stopping multiple streaming queries. Those warnings are not accepted as benchmark evidence; summary status, metrics rows, parquet output, and process exit remain the active acceptance signals.
