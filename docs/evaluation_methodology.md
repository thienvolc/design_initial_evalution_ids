# Evaluation Methodology

## Chosen Boundary

The active workflow separates runtime execution from evaluation:

- runtime emits operational metrics and prediction parquet
- evaluation matrices turn those artifacts into summary CSVs and plots
- paper conclusions should cite matrix artifacts, not ad hoc runtime inspection

## System Under Test

The SUT is the Spark Structured Streaming runtime:

- input: replayed Kafka traffic
- processing: parsing, feature preparation, model scoring
- output: prediction parquet and operational `ids.metrics`

Runtime no longer publishes a Kafka prediction sink. Classification quality is computed after the run from prediction parquet.

## Official Evaluation System

The authoritative evaluation path is:

- capacity calibration, Layer B, Layer C, watermark, and load-quality summary CSVs
- per-run metrics time series
- prediction parquet quality summaries
- report plots derived from those files

Default matrix scripts are smoke gates. They check startup, replay, sentinel stop, metrics publication, parquet quality summary, and CSV status. Larger paper-scale runs must explicitly select the main Python config builders.

## Artifact Contract

- prediction parquet
  - source of truth for post-run classification quality
- `ids.metrics`
  - operational runtime payloads only
- matrix summary CSVs
  - official per-scenario evaluation outputs
- metrics time series
  - official operational traces for plots and diagnosis

## What Not To Do

Avoid treating these as final benchmark evidence:

- smoke gate outputs by themselves
- raw `ids.metrics` messages outside the matrix summary pipeline
- manual dashboard screenshots or informal process logs
