# Component Guide

Use this file to find the right module before changing code.

## Main Script Entry Points

- `scripts/offline/run_offline_pipeline.py`
  - launches the offline preprocessing, training, and evaluation path
- `scripts/runbooks/offline_train_model.ps1`
  - host-side runbook for selective offline retraining by feature set and model list
- `scripts/streaming/official/run_structured_streaming.py`
  - launches the streaming SUT
- `scripts/streaming/official/replay_parquet_to_kafka.py`
  - replays parquet records into Kafka
- `scripts/streaming/official/run_streaming_profile.py`
  - launches a named profile from YAML
- `scripts/streaming/official/build_streaming_report.py`
  - aggregates matrix outputs into the final official report

## Active Secondary Entry Points

- `scripts/streaming/official/build_timeseries_plots.py`
  - renders plots from already-produced metrics timeseries CSV files
- `scripts/streaming/benchmark/run_benchmark_matrix.py`
  - launches benchmark scenarios outside the main official evaluation flow
- `scripts/streaming/benchmark/run_pandas_udf_benchmark.py`
  - launches the pandas UDF benchmark runtime
- `scripts/streaming/observability/export_prometheus_summary.py`
  - exposes live runtime telemetry for Prometheus

## Offline Modules

- `src/ids_platform/offline/pipeline.py`
  - top-level offline pipeline orchestration
- `src/ids_platform/offline/training.py`
  - model training logic
- `src/ids_platform/offline/evaluation.py`
  - offline evaluation metrics and artifacts

## Streaming Runtime

- `src/ids_platform/streaming/core/config.py`
  - shared streaming config loading and bootstrap resolution
- `src/ids_platform/streaming/core/artifacts.py`
  - shared artifact path and threshold helpers
- `src/ids_platform/streaming/runtime/structured_streaming_job.py`
  - main streaming runtime orchestration for the SUT
- `src/ids_platform/streaming/runtime/pipeline.py`
  - stream parsing and feature preparation
- `src/ids_platform/streaming/runtime/scoring.py`
  - model scoring UDFs and prediction column assembly
- `src/ids_platform/streaming/runtime/query.py`
  - query trigger and naming helpers

## Replay

- `src/ids_platform/streaming/replay/runner.py`
  - replay execution logic and Kafka publishing
- `src/ids_platform/streaming/replay/config.py`
  - replay/runtime estimation helpers

## Evaluation Namespace

- `src/ids_platform/streaming/evaluation/matrices/`
  - canonical import namespace for official matrix modules
- `src/ids_platform/streaming/evaluation/orchestration/`
  - canonical import namespace for official orchestration modules
- `src/ids_platform/streaming/evaluation/reporting/`
  - canonical import namespace for official reporting modules

## Experiment Matrices

- `src/ids_platform/streaming/evaluation/matrices/layer_a_matrix.py`
  - canonical implementation module for system knob experiments
- `src/ids_platform/streaming/evaluation/matrices/layer_b_matrix.py`
  - canonical implementation module for model and feature-set experiments
- `src/ids_platform/streaming/evaluation/matrices/layer_c/`
  - canonical Layer C implementation package for fault orchestration, replay, recovery, shutdown, and runtime validation
- `src/ids_platform/streaming/evaluation/matrices/layer_c_fault_matrix.py`
  - compatibility entrypoint that preserves historic Layer C imports and delegates to the `layer_c/` package
- `src/ids_platform/streaming/evaluation/matrices/watermark_matrix.py`
  - canonical implementation module for watermark experiments
- `src/ids_platform/streaming/evaluation/matrices/load_quality_matrix.py`
  - canonical implementation module for quality-under-load experiments
- `src/ids_platform/streaming/evaluation/matrices/common.py`
  - canonical shared helpers for process readiness, metrics polling, and common utilities

## Orchestration

- `src/ids_platform/streaming/evaluation/orchestration/profile_runner.py`
  - canonical implementation module for profile command resolution
- `src/ids_platform/streaming/evaluation/orchestration/profile_service.py`
  - canonical implementation module for profile loading and execution
- `src/ids_platform/streaming/benchmark/orchestration/runner.py`
  - benchmark matrix orchestration and summary writing
- `src/ids_platform/streaming/evaluation/orchestration/fault_matrix.py`
  - canonical implementation module for Layer C fault process management

## Reporting

- `src/ids_platform/streaming/evaluation/reporting/report_builder.py`
  - canonical implementation module that builds official report JSON and markdown from summary CSVs
- `src/ids_platform/streaming/evaluation/reporting/metrics_reader.py`
  - canonical implementation module that reads debug telemetry from Kafka for reporting helpers

## Observability

- `src/ids_platform/streaming/observability/prometheus_exporter.py`
  - Kafka-to-Prometheus live telemetry exporter
- `ops/observability/prometheus.yml`
  - Prometheus scrape configuration
- `ops/observability/grafana/...`
  - Grafana dashboards for live runtime monitoring
