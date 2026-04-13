# Architecture

## Project Shape

This project is a research-oriented Python codebase with two major execution paths:

- offline pipeline
  - preprocess data, train models, evaluate offline, and persist model artifacts
- streaming pipeline
  - replay parquet traffic into Kafka, run Spark Structured Streaming scoring, collect scenario summaries, and build final evaluation reports

It is a single-repo system with script entrypoints, shared library modules under `src/ids_platform`, Docker-based infrastructure, and file-based experiment artifacts.

## Major Runtime Components

- offline training
  - creates model artifacts and feature metadata used later by streaming
- replay service
  - reads parquet traffic and publishes records to Kafka
- SUT runtime
  - Spark Structured Streaming job that consumes Kafka, scores records, and emits predictions plus debug telemetry
- evaluation matrices
  - orchestrate repeatable experiments across Layer A, Layer B, Layer C, watermark, and load-quality scenarios
- reporting
  - aggregates summary CSVs into the official report JSON and markdown
- evaluation UI
  - Streamlit dashboard for completed experiment results
- observability
  - Prometheus exporter, Prometheus, and Grafana for live runtime telemetry

## Boundary: SUT vs Evaluation System

Use this split consistently:

- SUT
  - `run_structured_streaming.py`
  - input: replayed Kafka traffic
  - output: predictions parquet/Kafka plus debug telemetry on `ids.metrics`
- evaluation system
  - matrix runners aggregating `ids.metrics` into summary CSVs
  - consolidated report JSON/markdown
  - Streamlit post-run dashboard

Prometheus and Grafana are not part of the official evaluation pipeline. They support live monitoring and debugging only.

## Data Flow

```text
offline parquet/dataset
  -> offline pipeline
  -> model artifacts + feature manifest + thresholds

test parquet
  -> replay_parquet_to_kafka.py
  -> Kafka input topic
  -> run_structured_streaming.py (SUT)
  -> prediction parquet / prediction topic
  -> ids.metrics debug telemetry topic

matrix runners
  -> orchestrate replay + SUT runs
  -> collect ids.metrics-derived summaries
  -> write summary CSVs

build_online_report.py
  -> read summary CSVs
  -> write report JSON + markdown

Streamlit
  -> read summary CSVs and consolidated reports
  -> present official evaluation results

Prometheus exporter
  -> read ids.metrics
  -> expose live runtime telemetry to Prometheus/Grafana
```

## Design Patterns in Use

- thin script entrypoints
  - `scripts/` provides CLI launchers, `src/` holds implementation
- artifact-driven experimentation
  - experiments communicate largely through Kafka topics, summary CSVs, and report files
- orchestration by subprocess
  - matrix and profile runners launch lower-level scripts rather than embedding all logic in one process
- separation by responsibility
  - runtime, replay, matrices, orchestration, reporting, metrics, and UI are split into separate modules
