# Component Guide

Use this file to find the right module before changing code.

## Main Script Entry Points

- `scripts/offline/run_offline_pipeline.py`
  - launches offline preprocessing, training, and evaluation
- `scripts/streaming/official/run_layer_a_matrix.py`
  - runs the capacity/runtime-knob matrix
- `scripts/streaming/official/run_layer_b_matrix.py`
  - runs model and feature-set comparison
- `scripts/streaming/official/run_layer_c_matrix.py`
  - runs checkpoint/restart recovery scenarios
- `scripts/streaming/official/run_watermark_matrix.py`
  - runs late-event and watermark scenarios
- `scripts/streaming/official/run_load_quality_matrix.py`
  - runs quality-under-load scenarios
- `scripts/streaming/official/build_timeseries_plots.py`
  - renders plots from already-produced metrics time series CSV files

The streaming scripts are config-driven. Edit config objects in `src/ids_platform/streaming/config/` instead of adding CLI/YAML parsing.

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
- `src/ids_platform/streaming/config/capacity.py`
  - Layer A capacity configs
- `src/ids_platform/streaming/config/layer_b.py`
  - Layer B model/feature configs
- `src/ids_platform/streaming/config/layer_c.py`
  - Layer C recovery configs
- `src/ids_platform/streaming/config/watermark.py`
  - watermark configs
- `src/ids_platform/streaming/config/load_quality.py`
  - quality-under-load configs

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

Legacy YAML profile runners, benchmark scripts, Prometheus exporter, Grafana assets, and `streaming.core` were removed from the active system.
