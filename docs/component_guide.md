# Component Guide

Use this file to find the right module before changing code.

## Main Script Entry Points

- `scripts/offline/run_offline_pipeline.py`
  - launches offline preprocessing, training, and evaluation
- `scripts/streaming/official/run_capacity_calibration.py`
  - runs the capacity calibration smoke gate by default
- `scripts/streaming/official/run_model_feature_tradeoff.py`
  - runs model and feature-set smoke gate by default
- `scripts/streaming/official/run_fault_recovery.py`
  - runs checkpoint/restart recovery smoke gate by default
- `scripts/streaming/official/run_overload_degradation.py`
  - runs quality-under-load smoke gate by default
- `scripts/streaming/official/build_timeseries_plots.py`
  - renders plots from already-produced metrics time series CSV files

The streaming scripts are config-driven. Defaults are smoke gates; use the main config builder functions when preparing paper-scale runs.

## Paper & Analysis Scripts

- `scripts/paper/build_paper_plots.py`
  - orchestrates report plots
- `scripts/paper/build_latency_cdf_plot.py`
  - builds empirical CDF plots from metrics time series CSV files

## Offline Modules

- `src/ids_platform/offline/pipeline.py`
  - top-level offline pipeline orchestration
- `src/ids_platform/offline/training.py`
  - model training logic
- `src/ids_platform/offline/evaluation.py`
  - offline evaluation metrics and artifacts

## Streaming Config

- `src/ids_platform/streaming/config/common.py`
  - shared benchmark config dataclasses and builders
- `src/ids_platform/streaming/config/runtime.py`
  - runtime artifact paths, model thresholds, and `RuntimeConfig` builder
- `src/ids_platform/streaming/config/calibration.py`
  - capacity calibration configs and operating-point run plans
- `src/ids_platform/streaming/config/model_feature_tradeoff.py`
  - model/feature tradeoff configs
- `src/ids_platform/streaming/config/fault_recovery.py`
  - checkpoint/restart recovery configs
- `src/ids_platform/streaming/config/overload_degradation.py`
  - overload degradation configs

## Streaming Runtime

- `src/ids_platform/streaming/runtime/structured_streaming_job.py`
  - main streaming runtime orchestration
- `src/ids_platform/streaming/runtime/pipeline.py`
  - stream parsing and feature preparation
- `src/ids_platform/streaming/runtime/scoring.py`
  - model scoring UDFs and prediction columns
- `src/ids_platform/streaming/runtime/metrics.py`
  - operational metrics payloads
- `src/ids_platform/streaming/runtime/system_metrics.py`
  - local process, executor memory, and Kafka lag probes used by runtime metrics

## Replay

- `src/ids_platform/streaming/replay/config.py`
  - replay config, source factory, and rate plan
- `src/ids_platform/streaming/replay/runner.py`
  - replay execution and input sentinel publishing
- `src/ids_platform/streaming/replay/publisher.py`
  - Kafka publish boundary

## Evaluation

- `src/ids_platform/streaming/evaluation/matrices/`
  - official matrix modules
- `src/ids_platform/streaming/evaluation/matrices/throughput/benchmark_run.py`
  - shared stream-first benchmark executor
- `src/ids_platform/streaming/evaluation/quality.py`
  - post-run quality summary from parquet prediction artifacts

Legacy YAML profile runners, old benchmark scripts, external dashboard assets, and `streaming.core` were removed from the active system.
