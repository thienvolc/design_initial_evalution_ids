# Streaming Refactor Proposal

## Goal

Refactor `scripts/streaming/` and `src/ids_platform/streaming/` so the project is easier to understand and safer to evolve.

The main idea is to separate three paths clearly:

- `official`
  - the paper-facing and daily evaluation path
- `benchmark`
  - secondary benchmark and comparison experiments
- `observability`
  - live telemetry and debugging sidecars

This proposal is about structure and ownership.

It does not change methodology.

## What Must Not Change

- SUT vs evaluation boundary must stay intact
  - SUT consumes replayed Kafka traffic and emits predictions plus `ids.metrics`
  - matrices and reports remain the official evaluation system
- `run_tag` / `input_run_tag` isolation must stay intact
- Layer C host-orchestrated behavior must stay intact unless explicitly redesigned
- summary CSVs and report artifacts remain the official source of truth
- Prometheus/Grafana remain observability only

## Current Pain

- `scripts/streaming/` mixes:
  - official entrypoints
  - benchmark entrypoints
  - observability utilities
- `src/ids_platform/streaming/` mixes:
  - SUT runtime
  - replay
  - evaluation matrices
  - profile/full-run orchestration
  - benchmark path
  - observability
- the current structure is still workable, but the intent of each file is not obvious enough for a large cleanup

## Proposed Target Shape

### Scripts

```text
scripts/
  streaming/
    official/
      run_structured_streaming.py
      replay_parquet_to_kafka.py
      run_layer_a_matrix.py
      run_layer_b_matrix.py
      run_layer_c_matrix.py
      run_watermark_matrix.py
      run_load_quality_matrix.py
      run_online_profile.py
      run_online_full_evaluation.py
      build_online_report.py
      build_timeseries_plots.py
      read_metrics_for_run.py
    benchmark/
      run_benchmark_matrix.py
      run_pandas_udf_benchmark.py
    observability/
      export_prometheus_summary.py
    README.md
```

### Src

```text
src/ids_platform/streaming/
  core/
    config.py
    artifacts.py
  sut/
    runtime/
      structured_streaming_job.py
      pipeline.py
      scoring.py
      query.py
      control.py
    replay/
      runner.py
      config.py
      service.py
  evaluation/
    matrices/
      common.py
      layer_a_matrix.py
      layer_b_matrix.py
      layer_c_fault_matrix.py
      watermark_matrix.py
      load_quality_matrix.py
    orchestration/
      profile_runner.py
      profile_service.py
      full_evaluation.py
      fault_matrix.py
    reporting/
      report_builder.py
      metrics_reader.py
  benchmark/
    benchmark_contract.py
    orchestration/
      benchmark_runner.py
    runtime/
      pandas_udf_job.py
      benchmark.py
  observability/
    prometheus_exporter.py
    system.py
```

## Why This Shape

### Official

This path contains what the project uses to produce official evidence:

- replay
- SUT runtime
- Layer A/B/C
- watermark
- load-quality
- profile execution
- full evaluation
- report build

A new developer should be able to follow only this path and understand how the paper-facing system works.

### Benchmark

This path remains valid, but it should stop visually competing with the official flow.

It is useful for:

- pandas UDF comparison
- benchmark plan execution
- extra performance or fairness comparisons

But it is not the main path for daily evaluation or final reporting.

### Observability

This path is important operationally, but not part of the official evaluation contract.

It should be easy to find and easy to exclude mentally when someone is studying the evaluation logic.

## Proposed Ownership Model

### `sut/`

Owns only:

- reading Kafka input
- parsing payloads
- preparing features
- scoring
- writing predictions
- publishing debug telemetry
- controlled stop behavior

It must not own:

- official summary CSV creation
- profile gate logic
- final report semantics

### `evaluation/`

Owns only:

- experiment matrices
- orchestration around replay + SUT
- profile and full-run execution
- summary aggregation
- official report building

It must not silently become part of the SUT.

### `benchmark/`

Owns only:

- non-primary benchmark contracts
- alternate runtime comparison paths
- benchmark-specific summary logic

### `observability/`

Owns only:

- Prometheus exporter
- live system probes and related helpers

It must not become the official evaluation source.

## Recommended Migration Order

### Phase A: Namespace Clarification Without Logic Change

Do first:

- create new folders
- move files with import updates only
- keep script behavior identical
- keep CLI flags identical

This phase should produce mostly path changes, not logic changes.

### Phase B: Separate Official vs Benchmark Imports

Do next:

- move `benchmark_contract.py`
- move `benchmark_runner.py`
- move `pandas_udf_job.py`
- remove benchmark terminology from official-path docs where it creates confusion

### Phase C: Separate Observability From Evaluation

Do next:

- move `metrics/prometheus_exporter.py` and `metrics/system.py` to `observability/`
- update docs and imports
- keep `ids.metrics` handling in the SUT, but keep Prometheus export logic outside evaluation

### Phase D: Optional Internal Cleanup

Only after the move is stable:

- reduce overlap between `evaluation/matrices/common.py` and `evaluation/orchestration/fault_matrix.py`
- consider splitting Layer C helpers from generic fault helpers
- consider reducing coupling between reporting helpers and raw metrics readers

## Recommended Script Policy After Refactor

- `scripts/streaming/official/`
  - thin wrappers only
- `scripts/streaming/benchmark/`
  - thin wrappers only
- `scripts/streaming/observability/`
  - thin wrappers only

All decision-heavy logic should live in `src/ids_platform/streaming/...`.

## What To Avoid

- Do not merge benchmark and official evaluation into one generic “runner” abstraction too early
  - it will hide differences that currently matter
- Do not move Layer C into a generic fault framework if that removes its hybrid host/docker intent
- Do not let observability helpers become a dependency for official summary generation
- Do not change artifact paths and folder structure in the same commit as deep logic refactors

## Suggested First Concrete Move Set

If we want a low-risk first refactor batch, start with:

1. Move benchmark files into benchmark namespace
   - `scripts/streaming/run_benchmark_matrix.py`
   - `scripts/streaming/run_pandas_udf_benchmark.py`
   - `src/ids_platform/streaming/benchmark_contract.py`
   - `src/ids_platform/streaming/orchestration/benchmark_runner.py`
   - `src/ids_platform/streaming/runtime/pandas_udf_job.py`
   - `src/ids_platform/streaming/runtime/benchmark.py`

2. Move observability files into observability namespace
   - `src/ids_platform/streaming/metrics/prometheus_exporter.py`
   - `src/ids_platform/streaming/metrics/system.py`
   - `scripts/streaming/export_prometheus_summary.py`

3. Leave official path in place for one intermediate step
   - only update imports and docs
   - no behavior changes

This gives the largest clarity gain with the least risk to the main evaluation pipeline.

## Decision

Recommended direction:

- adopt the `official / benchmark / observability` split
- treat it as a namespace refactor first, not a behavior rewrite
- keep Layer C correctness constraints explicit throughout the migration

This is the cleanest structure that matches the repo as it exists today.
