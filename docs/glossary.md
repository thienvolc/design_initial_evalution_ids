# Glossary

## SUT

System under test. In this project, the SUT is the Spark Structured Streaming scorer. It consumes replayed Kafka traffic, writes prediction parquet, and publishes operational metrics.

## Evaluation System

The code that turns runtime artifacts into official results: matrix summaries, metrics time series, prediction-quality summaries, and plots.

## Smoke Gate

A short health check for the streaming flow. Current smoke gates use 1000 rows, batches of 500, and a replay schedule of 500 rps for 2 seconds.

Smoke gates detect broken startup, replay, sentinel stop, metrics publication, parquet output, or summary CSV generation. They are not paper-scale benchmark evidence.

## Official Evaluation Output

Artifacts that should be treated as the source of truth for experiment conclusions:

- matrix summary CSVs
- metrics time series CSVs
- prediction parquet quality summaries
- report plots derived from those files

## Capacity Calibration

Operating-point evaluation for the streaming SUT. Typical focus:

- target replay RPS
- throughput and rows processed
- p50/p95 latency
- batch wall time
- Kafka lag

## Model-Feature Tradeoff

Model and feature-set evaluation in the streaming context. Typical focus:

- model choice
- feature-set choice
- quality/operation tradeoff under the streaming runtime

## Fault Recovery

Fault and recovery evaluation. The active recovery mechanism is checkpoint-based restart plus input sentinel coordination.

## Overload Degradation

Experiment set for runtime degradation when replay pressure exceeds sustainable processing capacity.

## `run_tag`

Identifier attached to a run's output artifacts and metrics.

## `input_run_tag`

Identifier used by the stream to filter which replayed Kafka records belong to the current run. In matrix runs, `run_tag` and `input_run_tag` should match.

## Metrics Topic

Kafka topic for operational runtime payloads. In this project, `ids.metrics` supports matrix summaries and operational diagnosis; classification quality comes from prediction parquet.
