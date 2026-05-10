# Streaming Scripts

This directory contains thin CLI entrypoints for the streaming part of the project.

Implementation code lives under:
- `src/ids_platform/streaming/runtime`
- `src/ids_platform/streaming/replay`
- `src/ids_platform/streaming/evaluation`
- `src/ids_platform/streaming/benchmark`
- `src/ids_platform/streaming/observability`

## Boundary

Use these roles consistently:

- `run_structured_streaming.py`
  - runs the system under test (SUT)
  - reads replayed Kafka traffic
  - emits predictions and debug telemetry
- matrix summaries
  - produce the official evaluation outputs
  - current official summary path is derived from `ids.metrics`
- `observability/export_prometheus_summary.py`, Prometheus, and Grafana
  - provide live runtime observability only
  - they are not the authoritative benchmark source

## Execution Model

Use Docker as the default execution path.

Why:
- streaming configs use container hostnames such as `kafka:29092`
- Spark, Kafka, and the Python runtime are expected to share the same Docker network
- Docker is the compatible and documented path for this repo

Default pattern:

```powershell
docker compose up -d zookeeper kafka ids-dev
docker compose exec -T ids-dev python <script> <args>
```

Layer C is the one deliberate exception in local orchestration:
- the Layer C matrix runner stays on the host so it can inject Docker faults such as Kafka restarts
- the SUT, replay, and metrics readers launched by Layer C still run in Docker with `execution_mode=docker`
- the implementation now lives under `src/ids_platform/streaming/evaluation/matrices/layer_c/`
- `src/ids_platform/streaming/evaluation/matrices/layer_c_fault_matrix.py` remains a compatibility entrypoint for existing imports/tests

Do not run Layer C on Windows host Spark directly. That path is unsupported because Spark checkpoint/file handling on Windows causes Hadoop native I/O failures.

## Canonical Evaluation Flow

The official evaluation flow is:

1. Replay traffic into Kafka.
2. Run the SUT streaming scorer.
3. Collect matrix summary CSVs.


Prometheus and Grafana are outside this flow. They support live telemetry only.

## Main Entrypoints

- `run_structured_streaming.py`
  - start the Structured Streaming scorer
- `replay_parquet_to_kafka.py`
  - replay parquet rows into the Kafka input topic
- `benchmark/run_pandas_udf_benchmark.py`
  - run the Spark pandas UDF benchmark path
- `run_streaming_profile.py`
  - execute a named experiment profile
- `run_layer_a_matrix.py`
  - evaluate system-level streaming knobs
- `run_layer_b_matrix.py`
  - evaluate model and feature-set combinations
- `run_layer_c_matrix.py`
  - evaluate fault and recovery scenarios
- `run_watermark_matrix.py`
  - evaluate watermark and late-event behavior
- `run_load_quality_matrix.py`
  - evaluate detection quality under load
- `benchmark/run_benchmark_matrix.py`
  - run benchmark matrix scenarios

- `observability/export_prometheus_summary.py`
  - expose live runtime telemetry to Prometheus

## Common Commands

Start shared services:

```powershell
docker compose up -d zookeeper kafka ids-dev
```

Run the SUT manually:

```powershell
docker compose exec -T ids-dev python scripts/streaming/official/run_structured_streaming.py --config configs/streaming/streaming.yaml --model logistic_regression --feature-set full --run-tag smoke_test --input-run-tag smoke_test --run-seconds 180
```

Replay test traffic:

```powershell
docker compose exec -T ids-dev python scripts/streaming/official/replay_parquet_to_kafka.py --config configs/streaming/streaming.yaml --run-tag smoke_test
```

Run a profile:

```powershell
docker compose exec -T ids-dev python scripts/streaming/official/run_streaming_profile.py --profile-config experiments/streaming/profiles/local_profiles.yaml --profile smoke_gate
```

Run the Layer C profile from the host:

```powershell
python scripts/streaming/official/run_streaming_profile.py --profile-config experiments/streaming/profiles/local_profiles.yaml --profile layer_c_local_light
```

Run Layer C directly from the host with Docker-backed execution:

```powershell
python scripts/streaming/official/run_layer_c_matrix.py --config configs/streaming/streaming.yaml --execution-mode docker --scenarios producer_restart
```

Run a matrix directly:

```powershell
docker compose exec -T ids-dev python scripts/streaming/official/run_layer_a_matrix.py --config configs/streaming/streaming.yaml
```

Expose live runtime telemetry:

```powershell
docker compose exec -T ids-dev python scripts/streaming/observability/export_prometheus_summary.py --config configs/streaming/streaming.yaml --port 9108
```

## Profile Files

- `experiments/streaming/profiles/local_profiles.yaml`
  - workstation-oriented profile set
- `experiments/streaming/profiles/streaming_profiles.yaml`
  - larger test-machine and benchmark profile set

## Editing Guidance

Keep scripts in this directory thin.

Make implementation changes in `src/ids_platform/streaming`:
- runtime behavior: `runtime/`
- replay behavior: `replay/`
- shared config/artifact helpers: `core/`
- scenario logic: `evaluation/matrices/`

- orchestration and profiles: `evaluation/orchestration/`
- benchmark path: `benchmark/`
- telemetry exporter: `observability/`

Compatibility note:
- most active streaming shim namespaces were removed during refactor
- Layer C still keeps `evaluation/matrices/layer_c_fault_matrix.py` as an intentional compatibility facade until callers/tests are migrated
