# Phase 3: Streaming Core and Replay

## Goal

Build the online execution path:

- replay parquet rows into Kafka
- run the Spark Structured Streaming scorer
- emit predictions and runtime telemetry for later evaluation

## Status

- completed

## Inputs

- replay source parquet:
  - resolved from `paths.input_parquet` in streaming config
  - in `configs/streaming/online.yaml`, default is `data/gold/splits/test.parquet`
- feature manifest used by replay and scoring:
  - resolved from config and `feature_set`
  - default full-feature path in `online.yaml` is `artifacts/offline/preprocessing/feature_manifest.json`
- streaming runtime config:
  - `configs/streaming/online.yaml`
  - or other streaming config passed by CLI
- trained model artifact and threshold metadata resolved from config + `feature_set`

## Tasks

### Task 1: Parquet To Kafka Replay

- Status: done
- Goal: publish offline parquet rows into the Kafka raw topic with controlled pace
- What was done:
  - added replay CLI and runner
  - loaded replay rows from parquet using feature manifest columns
  - supported `rows_per_sec` and `rate_schedule`
  - added optional reorder / late-event injection controls
  - wrote `replay_run_tag` into payloads
  - emitted `input_sentinel` control record when requested
- Inputs:
  - parquet split resolved from config or CLI override
  - Kafka input topic from streaming config
- Outputs:
  - records published to Kafka input topic, typically `ids.raw.flows`
  - replay log lines on stdout such as `[replay] event=start/progress/done`
- Main code:
  - `src/ids_platform/streaming/replay/runner.py`
  - `scripts/streaming/replay_parquet_to_kafka.py`

### Task 2: Structured Streaming Scoring Runtime

- Status: done
- Goal: consume Kafka input, score rows with offline-trained model artifacts, and emit prediction outputs
- What was done:
  - added Kafka source reading and JSON parsing
  - filtered input by `input_run_tag`
  - prepared feature columns from manifest + fill values
  - applied model scoring UDF and threshold logic
  - wrote prediction results to Kafka and parquet sinks
- Inputs:
  - Kafka input topic, typically `ids.raw.flows`
  - `input_run_tag` to isolate one replay run when needed
  - model artifact resolved from config + `feature_set`
  - threshold resolved from model config or `valid_metrics.csv`
- Outputs:
  - Kafka output topic, typically `ids.predictions.binary`
  - prediction parquet under:
    - `artifacts/streaming/predictions/<run_tag or model_feature_suffix>/`
  - checkpoints under:
    - `artifacts/streaming/checkpoints/<run_tag or model_feature_suffix>/`
- Main code:
  - `src/ids_platform/streaming/runtime/structured_streaming_job.py`
  - `src/ids_platform/streaming/runtime/pipeline.py`
  - `src/ids_platform/streaming/runtime/scoring.py`
  - `scripts/streaming/run_structured_streaming.py`

### Task 3: Runtime Metrics And Control Semantics

- Status: done
- Goal: make the streaming job observable and controllable enough for later matrix orchestration
- What was done:
  - emitted runtime events on stdout with `[stream] ...`
  - published batch metrics to Kafka metrics topic
  - added `run_tag` to outputs and metrics
  - separated control records from data records
  - introduced `input_sentinel` handling for controlled stop behavior
- Inputs:
  - scored micro-batches from the SUT
  - control records from replay when sentinel is enabled
- Outputs:
  - Kafka metrics topic, typically `ids.metrics`
  - runtime log lines such as `job_start`, `batch_metrics`, `job_stop`
- Main code:
  - `src/ids_platform/streaming/runtime/structured_streaming_job.py`
  - `src/ids_platform/streaming/runtime/pipeline.py`

## What Must Stay True

- `run_tag` and `input_run_tag` isolation are core correctness rules
- replay writes data into Kafka, but does not perform evaluation
- SUT emits predictions and debug telemetry, but does not build official summary CSVs
- matrix/report logic belongs to later phases, not inside the core streaming runtime

## Why This Phase Matters

This phase created the core online contract of the project. If replay payloads, `run_tag` semantics, sink outputs, or metrics emission change carelessly, later matrices may still run but measure the wrong thing.

## Main Code

- `src/ids_platform/streaming/replay/runner.py`
- `src/ids_platform/streaming/runtime/structured_streaming_job.py`
- `src/ids_platform/streaming/runtime/pipeline.py`
- `src/ids_platform/streaming/runtime/scoring.py`
- `scripts/streaming/replay_parquet_to_kafka.py`
- `scripts/streaming/run_structured_streaming.py`
