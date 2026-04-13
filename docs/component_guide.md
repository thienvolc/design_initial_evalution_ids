# Component Guide

Use this file to find the right module before changing code.

## Main Script Entry Points

- `scripts/offline/run_offline_pipeline.py`
  - launches the offline preprocessing, training, and evaluation path
- `scripts/streaming/run_structured_streaming.py`
  - launches the streaming SUT
- `scripts/streaming/replay_parquet_to_kafka.py`
  - replays parquet records into Kafka
- `scripts/streaming/run_online_profile.py`
  - launches a named profile from YAML
- `scripts/streaming/run_online_full_evaluation.py`
  - orchestrates the end-to-end online evaluation campaign
- `scripts/streaming/build_online_report.py`
  - aggregates matrix outputs into the final official report
- `scripts/streaming/export_prometheus_summary.py`
  - exposes live runtime telemetry for Prometheus

## Offline Modules

- `src/ids_platform/offline/pipeline.py`
  - top-level offline pipeline orchestration
- `src/ids_platform/offline/training.py`
  - model training logic
- `src/ids_platform/offline/evaluation.py`
  - offline evaluation metrics and artifacts

## Streaming Runtime

- `src/ids_platform/streaming/runtime/structured_streaming_job.py`
  - main streaming runtime orchestration for the SUT
- `src/ids_platform/streaming/runtime/pipeline.py`
  - stream parsing and feature preparation
- `src/ids_platform/streaming/runtime/scoring.py`
  - model scoring UDFs and prediction column assembly
- `src/ids_platform/streaming/runtime/query.py`
  - query trigger and naming helpers
- `src/ids_platform/streaming/artifacts.py`
  - artifact path resolution for models, manifests, and outputs

## Replay

- `src/ids_platform/streaming/replay/runner.py`
  - replay execution logic and Kafka publishing
- `src/ids_platform/streaming/replay/config.py`
  - replay/runtime estimation helpers

## Experiment Matrices

- `src/ids_platform/streaming/matrices/layer_a_matrix.py`
  - system knob experiments
- `src/ids_platform/streaming/matrices/layer_b_matrix.py`
  - model and feature-set experiments
- `src/ids_platform/streaming/matrices/layer_c_fault_matrix.py`
  - fault and recovery experiments
- `src/ids_platform/streaming/matrices/watermark_matrix.py`
  - watermark experiments
- `src/ids_platform/streaming/matrices/load_quality_matrix.py`
  - quality-under-load experiments
- `src/ids_platform/streaming/matrices/common.py`
  - shared matrix helpers for process readiness, metrics polling, and common utilities

## Orchestration

- `src/ids_platform/streaming/orchestration/profile_runner.py`
  - resolves and validates profile commands
- `src/ids_platform/streaming/orchestration/profile_service.py`
  - profile loading and execution service helpers
- `src/ids_platform/streaming/orchestration/full_evaluation.py`
  - sequencing for full evaluation runs
- `src/ids_platform/streaming/orchestration/benchmark_runner.py`
  - benchmark matrix orchestration and summary writing
- `src/ids_platform/streaming/orchestration/fault_matrix.py`
  - process management for fault scenarios

## Reporting and UI

- `src/ids_platform/streaming/reporting/report_builder.py`
  - builds official report JSON and markdown from summary CSVs
- `src/ids_platform/streaming/reporting/metrics_reader.py`
  - reads debug telemetry from Kafka for reporting helpers
- `apps/streaming_dashboard.py`
  - official post-run evaluation dashboard

## Observability

- `src/ids_platform/streaming/metrics/prometheus_exporter.py`
  - Kafka-to-Prometheus live telemetry exporter
- `ops/observability/prometheus.yml`
  - Prometheus scrape configuration
- `ops/observability/grafana/...`
  - Grafana dashboards for live runtime monitoring
