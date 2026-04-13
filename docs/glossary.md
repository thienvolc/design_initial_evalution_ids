# Glossary

## SUT

System under test.

In this project, the SUT is the Spark Structured Streaming scorer started by `run_structured_streaming.py`. It consumes replayed Kafka traffic and emits predictions plus debug telemetry.

## Evaluation System

The part of the project that turns experiment runs into official results.

In this repo, that means matrix summaries, consolidated reports, and the Streamlit evaluation dashboard.

## Debug Telemetry

Supporting runtime observability emitted by the SUT, primarily through `ids.metrics` and surfaced through Prometheus/Grafana.

Useful for debugging and live monitoring, but not the official benchmark source.

## Official Evaluation Output

Artifacts that should be treated as the source of truth for experiment conclusions:

- matrix summary CSVs
- consolidated report JSON
- consolidated report markdown

## Layer A

System-level streaming knob evaluation.

Typical focus:
- `max_offsets_per_trigger`
- `shuffle_partitions`
- trigger interval
- throughput and latency tradeoffs

## Layer B

Model and feature-set evaluation in the streaming context.

Typical focus:
- model choice
- feature-set choice
- inference latency
- quality under the streaming runtime

## Layer C

Fault and recovery evaluation.

Typical focus:
- restart or fault scenarios
- recovery time
- post-fault throughput and latency

## Watermark Matrix

Experiment set for late-event handling and watermark delay behavior.

## Load-Quality Matrix

Experiment set for detection quality under different input pressure/load profiles.

## `run_tag`

Identifier attached to a run's output artifacts and telemetry.

Used to distinguish one experiment run from another.

## `input_run_tag`

Identifier used by the stream to filter which replayed Kafka records should be consumed for the current run.

In most experiments, `run_tag` and `input_run_tag` should match.

## `available-now`

Structured Streaming mode that processes data currently available in the source and then stops.

Useful when replay has already populated the topic.

## `run-seconds`

Explicit wall-clock duration for keeping the stream alive during trace-style runs.

Useful when the stream starts first and replay happens afterward.

## Metrics Topic

The Kafka topic used for SUT-emitted runtime telemetry.

In this project, it supports debugging and observability, not the final evaluation report.
