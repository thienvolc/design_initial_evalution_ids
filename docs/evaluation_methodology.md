# Evaluation Methodology

## Chosen Model

This repo currently uses a metrics-driven evaluation boundary between the system under test and the evaluation system.

- keep in-SUT metrics for debugging and live observability
- treat matrix summary CSVs and consolidated reports as the authoritative experiment outputs for now

This repo currently uses a metrics-driven evaluation path. A fully separate external evaluator is not part of the active workflow.

## System Under Test

The SUT is the Spark Structured Streaming runtime:

- input
  - replayed Kafka traffic
- processing
  - parsing, feature preparation, model scoring, prediction emission
- output
  - prediction parquet
  - prediction Kafka output
  - debug telemetry on `ids.metrics`

## Official Evaluation System

The authoritative evaluation path currently uses matrix runners that read SUT-emitted metrics:

- Layer A, Layer B, Layer C, watermark, and load-quality summary CSVs
- consolidated report JSON and markdown
- Streamlit dashboard that reads those artifacts

These are the outputs that should be cited in reports, conclusions, and comparisons.

## Artifact Contract

- prediction artifacts
  - raw SUT outputs
- matrix summary CSVs
  - official per-scenario evaluation outputs currently derived from `ids.metrics`
- consolidated report JSON/markdown
  - official aggregated evaluation outputs
- Prometheus and Grafana
  - live runtime monitoring only

## Reproducible Flow

Use this sequence for repeatable evaluation:

1. replay traffic into Kafka
2. run the SUT streaming scorer
3. collect per-scenario summary CSVs
4. build the consolidated report
5. review official results in Streamlit

## What Not To Do

Avoid treating these as the final benchmark source:

- Grafana panels
- Prometheus timeseries
- raw `ids.metrics` topic data by itself, outside the matrix summary pipeline

These are useful for debugging, run health, and operational inspection. Current official claims should be made from the matrix summary CSVs and consolidated reports, not from ad hoc inspection of the live telemetry stream.
