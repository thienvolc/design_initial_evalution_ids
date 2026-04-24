# Refactor Inventory

## Purpose

This file is a working classification for the large refactor.

It is not a delete list.

Use it to answer:

- what is canonical now
- what is historical but still worth keeping
- what is redundant noise that should not drive design decisions

## Classification Rules

- `canonical`
  - current execution path, current architecture truth, or current operator guidance
- `legacy`
  - historical reference, old runbook, or note that may still contain context but is not the current source of truth
- `redundant`
  - generated output, cache, duplicated artifact, or low-value file that should not survive a cleanup unless explicitly needed

## Docs

### Canonical

- `docs/README.md`
  - canonical doc index
- `docs/architecture.md`
  - canonical high-level system shape
- `docs/component_guide.md`
  - canonical codebase navigation map
- `docs/glossary.md`
  - canonical terminology
- `docs/evaluation_methodology.md`
  - canonical SUT vs evaluation boundary
- `docs/execution_runbook.md`
  - canonical execution guidance
- `docs/adr/ADR-0001-layer-c-stabilization.md`
  - canonical rationale for Layer C behavior
- `scripts/streaming/README.md`
  - canonical operator-facing streaming entrypoint guide

### Legacy

- `docs/01_overview.md`
- `docs/02_feature.md`
- `docs/03_programming.md`
- `docs/04_submission_checklist.md`
- `docs/05_final_operational_summary.md`
- `docs/06_phase4_runbook.md`
- `docs/07_test_machine_run_guide.md`
- `docs/scale_up_plan.md`
  - these may still contain useful historical intent, but they are not the current execution truth
- `docs/paper_ref/`
- `docs/papers/`
- `docs/references/`
  - research/reference material, not runtime authority

### Redundant

- `docs/error.txt`
- `docs/feedback.txt`
- `docs/offline_training.note.txt`
- `docs/log.png`
- `docs/Design and Initial Operational Evaluation of a Streaming Network Intrusion Detection Pipeline using Kafka and Spark.txt`
  - keep only if they are actively cited in writing workflow; otherwise they are repo-noise, not system docs

## Scripts

### Canonical

- `scripts/streaming/run_structured_streaming.py`
- `scripts/streaming/replay_parquet_to_kafka.py`
- `scripts/streaming/read_metrics_for_run.py`
- `scripts/streaming/run_layer_a_matrix.py`
- `scripts/streaming/run_layer_b_matrix.py`
- `scripts/streaming/run_layer_c_matrix.py`
- `scripts/streaming/run_watermark_matrix.py`
- `scripts/streaming/run_load_quality_matrix.py`
- `scripts/streaming/run_online_profile.py`
- `scripts/streaming/run_online_full_evaluation.py`
- `scripts/streaming/build_online_report.py`
- `scripts/streaming/export_prometheus_summary.py`
  - these are on the current execution/reporting path or are documented as current operational entrypoints
- `scripts/runbooks/daily_light_regression.ps1`
- `scripts/runbooks/daily_regression.ps1`
- `scripts/runbooks/post_daily_light_regression.ps1`
  - current operational wrappers around canonical entrypoints
- `scripts/offline/run_offline_pipeline.py`
  - canonical offline pipeline launcher
- `scripts/offline/verify_inference.py`
  - useful current validation utility tied to offline artifact correctness

### Active Secondary

- `scripts/streaming/run_benchmark_matrix.py`
- `scripts/streaming/run_pandas_udf_benchmark.py`
- `scripts/offline/_common.py`
  - keep these in the main tree
  - `run_benchmark_matrix.py` and `run_pandas_udf_benchmark.py` are still valid code paths with config, backend implementation, and tests
  - they are secondary because they support benchmark/ablation work, not the main daily evaluation or paper-ready reporting path
  - `_common.py` is still needed by the remaining offline scripts

### Archived Historical

- `scripts/offline/ablation_gbt.py`
- `scripts/offline/diagnose_model.py`
- `scripts/offline/feature_sweep.py`
- `scripts/offline/label_count.py`
- `scripts/offline/rebuild_gradient_boosting.py`
  - analysis or one-off rebuild helpers that should stay in `_archive`, not in the main execution tree

### Redundant

- `scripts/offline/diagnose_output.txt`
- `scripts/offline/feature_sweep_output.txt`
- `scripts/offline/label_report.txt`
- `scripts/offline/shell.txt`
- `scripts/offline/verify_output.txt`
  - generated or note-style outputs; they should not live as first-class repo logic
- `scripts/**/__pycache__/`
  - generated cache only

## Src

### Canonical Core

- `src/ids_platform/common/`
  - shared config, path, subprocess utilities used by current flows
- `src/ids_platform/offline/`
  - current offline pipeline implementation
- `src/ids_platform/streaming/runtime/`
  - SUT runtime and benchmark runtime implementation
- `src/ids_platform/streaming/replay/`
  - replay contract and replay execution
- `src/ids_platform/streaming/matrices/`
  - Layer A/B/C, watermark, and load-quality experiment logic
- `src/ids_platform/streaming/orchestration/profile_runner.py`
- `src/ids_platform/streaming/orchestration/profile_service.py`
- `src/ids_platform/streaming/orchestration/full_evaluation.py`
- `src/ids_platform/streaming/orchestration/fault_matrix.py`
- `src/ids_platform/streaming/orchestration/benchmark_runner.py`
  - current orchestration layer for profiles, full runs, Layer C control, and benchmark path
- `src/ids_platform/streaming/reporting/`
  - official summary/report aggregation
- `src/ids_platform/streaming/metrics/`
  - observability sidecar implementation still used by current docs and scripts
- `src/ids_platform/streaming/artifacts.py`
- `src/ids_platform/streaming/config.py`
- `src/ids_platform/streaming/benchmark_contract.py`
  - current shared streaming contracts

### Secondary But Valid

- `src/ids_platform/streaming/runtime/pandas_udf_job.py`
  - valid benchmark path, but not the main online SUT path used in current paper-facing evaluation
- `src/ids_platform/streaming/runtime/benchmark.py`
  - benchmark-related helper, not core streaming evaluation path

### Redundant

- `src/**/__pycache__/`
  - generated cache only

## Refactor Notes

- Do not refactor by folder name alone.
  - some `docs/` files are canonical and some are clearly historical
  - some `scripts/` files are production entrypoints and some are saved outputs
- The benchmark path is still valid code.
  - do not archive `run_benchmark_matrix.py` or `run_pandas_udf_benchmark.py` unless the benchmark contract itself is being retired
- For `scripts/streaming/`, prefer keeping thin entrypoints and moving logic decisions into `src/`
- For `scripts/offline/`, the main cleanup opportunity is separating:
  - official pipeline entrypoints
  - investigation helpers
  - saved outputs that should leave the repo
- For `src/ids_platform/streaming/`, the main cleanup opportunity is reducing overlap between:
  - runtime
  - matrices
  - orchestration
  - reporting
  without breaking the SUT vs evaluation boundary

## Safe First Cleanup Targets

- all `__pycache__/` directories
- `.txt` outputs under `scripts/offline/`
- low-signal loose files under `docs/` such as `error.txt`, `feedback.txt`, `log.png`

These are the least risky removals because they are not on the current execution path.
