# Streaming Scripts

This directory contains thin CLI entrypoints for the streaming part of the project.

Implementation code lives under:
- `src/ids_platform/streaming/runtime`
- `src/ids_platform/streaming/replay`
- `src/ids_platform/streaming/matrices`
- `src/ids_platform/streaming/reporting`
- `src/ids_platform/streaming/orchestration`
- `src/ids_platform/streaming/metrics`

## Boundary

Use these roles consistently:

- `run_structured_streaming.py`
  - runs the system under test (SUT)
  - reads replayed Kafka traffic
  - emits predictions and debug telemetry
- matrix summaries and `build_online_report.py`
  - produce the official evaluation outputs
  - current official summary path is derived from `ids.metrics`
- `apps/streaming_dashboard.py`
  - presents official post-run evaluation results
- `export_prometheus_summary.py`, Prometheus, and Grafana
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

Use host execution only when a profile explicitly requires `runtime: host` or when you are debugging intentionally outside Docker.

## Canonical Evaluation Flow

The official evaluation flow is:

1. Replay traffic into Kafka.
2. Run the SUT streaming scorer.
3. Collect matrix summary CSVs.
4. Build the consolidated report.
5. Review official results in Streamlit.

Prometheus and Grafana are outside this flow. They support live telemetry only.

## Main Entrypoints

- `run_structured_streaming.py`
  - start the Structured Streaming scorer
- `replay_parquet_to_kafka.py`
  - replay parquet rows into the Kafka input topic
- `run_pandas_udf_benchmark.py`
  - run the Spark pandas UDF benchmark path
- `run_online_profile.py`
  - execute a named experiment profile
- `run_online_full_evaluation.py`
  - run the consolidated Layer A/B/C evaluation flow
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
- `run_benchmark_matrix.py`
  - run benchmark matrix scenarios
- `build_online_report.py`
  - aggregate summaries into the final report JSON/markdown
- `read_metrics_for_run.py`
  - inspect debug telemetry for a specific run
- `export_prometheus_summary.py`
  - expose live runtime telemetry to Prometheus

## Common Commands

Start shared services:

```powershell
docker compose up -d zookeeper kafka ids-dev
```

Run the SUT manually:

```powershell
docker compose exec -T ids-dev python scripts/streaming/run_structured_streaming.py --config configs/streaming/online.yaml --model logistic_regression --feature-set full --run-tag smoke_test --input-run-tag smoke_test --run-seconds 180
```

Replay test traffic:

```powershell
docker compose exec -T ids-dev python scripts/streaming/replay_parquet_to_kafka.py --config configs/streaming/online.yaml --run-tag smoke_test
```

Run a profile:

```powershell
docker compose exec -T ids-dev python scripts/streaming/run_online_profile.py --profile-config experiments/streaming/profiles/local_profiles.yaml --profile smoke_gate
```

Run a matrix directly:

```powershell
docker compose exec -T ids-dev python scripts/streaming/run_layer_a_matrix.py --config configs/streaming/online.yaml
```

Build the official report:

```powershell
docker compose exec -T ids-dev python scripts/streaming/build_online_report.py --layer-a artifacts/streaming/online/layer_a_summary.csv --layer-b artifacts/streaming/online/layer_b_summary.csv --layer-c artifacts/streaming/online/layer_c_summary.csv
```

Expose live runtime telemetry:

```powershell
docker compose exec -T ids-dev python scripts/streaming/export_prometheus_summary.py --config configs/streaming/online.yaml --port 9108
```

## Profile Files

- `experiments/streaming/profiles/local_profiles.yaml`
  - workstation-oriented profile set
- `experiments/streaming/profiles/online_profiles.yaml`
  - larger test-machine and scale-up profile set

## Editing Guidance

Keep scripts in this directory thin.

Make implementation changes in `src/ids_platform/streaming`:
- runtime behavior: `runtime/`
- replay behavior: `replay/`
- scenario logic: `matrices/`
- report generation: `reporting/`
- orchestration and profiles: `orchestration/`
- telemetry exporter: `metrics/`
