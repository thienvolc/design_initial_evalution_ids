# Execution Runbook

## Default Rule

Use Docker-first execution for streaming work.

Start shared services first:

```powershell
docker compose up -d zookeeper kafka ids-dev
```

Then run streaming scripts through the `ids-dev` container unless a profile explicitly requires host runtime.

## Canonical Evaluation Order

Official evaluation follows this order:

1. replay traffic into Kafka
2. run the SUT streaming scorer
3. collect matrix summary CSVs
4. build the consolidated report
5. review official results in Streamlit

Prometheus and Grafana are optional sidecar observability only.

## Manual Streaming Run

Terminal 1, run the SUT:

```powershell
docker compose exec -T ids-dev python scripts/streaming/run_structured_streaming.py --config configs/streaming/online.yaml --model logistic_regression --feature-set full --run-tag smoke_test --input-run-tag smoke_test --run-seconds 180
```

Terminal 2, replay data:

```powershell
docker compose exec -T ids-dev python scripts/streaming/replay_parquet_to_kafka.py --config configs/streaming/online.yaml --run-tag smoke_test
```

Use `--available-now` only when the input topic already contains the data you want the stream to process.

## Profile Run

Run a named profile from the local workstation profile set:

```powershell
docker compose exec -T ids-dev python scripts/streaming/run_online_profile.py --profile-config experiments/streaming/profiles/local_profiles.yaml --profile smoke_gate
```

Profile execution wraps the lower-level matrix or benchmark script for that scenario.

## Matrix Run

Run a single matrix directly when you want explicit control:

```powershell
docker compose exec -T ids-dev python scripts/streaming/run_layer_a_matrix.py --config configs/streaming/online.yaml
docker compose exec -T ids-dev python scripts/streaming/run_layer_b_matrix.py --config configs/streaming/online.yaml
python scripts/streaming/run_layer_c_matrix.py --config configs/streaming/online.yaml
```

Layer C may run on the host depending on the profile/runtime setup.

## Build the Official Report

Use the matrix summaries to produce the final report:

```powershell
docker compose exec -T ids-dev python scripts/streaming/build_online_report.py --layer-a artifacts/streaming/online/layer_a_summary.csv --layer-b artifacts/streaming/online/layer_b_summary.csv --layer-c artifacts/streaming/online/layer_c_summary.csv
```

The report JSON and markdown are part of the official evaluation outputs.

## View Official Results

Run Streamlit on the host:

```powershell
python -m streamlit run apps/streaming_dashboard.py
```

The dashboard reads summary CSVs and consolidated report artifacts. It does not depend on Prometheus data.

## Live Telemetry Only

If you want live runtime telemetry during experiments:

```powershell
docker compose exec -T ids-dev python scripts/streaming/export_prometheus_summary.py --config configs/streaming/online.yaml --port 9108
docker compose -f ops/observability/docker-compose.observability.yaml up -d
```

Use this for run health and debugging. Do not use it as the official benchmark source.
