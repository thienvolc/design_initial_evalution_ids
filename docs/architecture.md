# Architecture

## Project Shape

This project has two active execution paths:

- offline pipeline
  - preprocess data, train models, evaluate offline, and persist model artifacts
- streaming pipeline
  - replay parquet traffic into Kafka, run Spark Structured Streaming scoring, collect matrix summaries, and build report artifacts

The codebase uses Python modules under `src/ids_platform`, thin script entrypoints, Docker infrastructure, and file-based experiment artifacts.

## Major Runtime Components

- offline training
  - creates model artifacts and feature metadata used by streaming
- replay
  - reads parquet traffic and publishes records plus an input sentinel to Kafka
- SUT runtime
  - consumes Kafka records, scores them, writes prediction parquet, and publishes operational `ids.metrics`
- evaluation matrices
  - orchestrate capacity calibration, Layer B, Layer C, watermark, and load-quality runs
- report artifacts
  - summary CSVs, metrics time series, prediction parquet, and plots

## Boundary: SUT vs Evaluation System

Use this split consistently:

- SUT
  - input: replayed Kafka traffic
  - outputs: prediction parquet and operational `ids.metrics`
- evaluation system
  - orchestrates replay/runtime
  - reads `ids.metrics` and prediction parquet
  - writes matrix summary CSVs and plots

Default matrix scripts are smoke gates. They prove the flow starts, replays, stops on sentinel, emits metrics, and writes summary rows. They are not paper-scale benchmark evidence.

## Data Flow

```text
offline parquet/dataset
  -> offline pipeline
  -> model artifacts + feature manifest + thresholds

test parquet
  -> replay config/source factory
  -> Kafka input topic
  -> runtime SUT
  -> prediction parquet
  -> ids.metrics operational topic

matrix runners
  -> orchestrate replay + runtime
  -> collect metrics + summarize parquet quality
  -> write summary CSVs and metrics time series
```

## Design Rules

- Keep script entrypoints thin and config-driven.
- Keep smoke gates separate from paper-scale benchmark configs.
- Treat summary CSVs, metrics time series, and prediction parquet as the active source of truth.
- Do not reintroduce YAML profile parsing, old benchmark scripts, or dashboard-driven evidence.
