# Execution Runbook

## Default Rule

Use host PowerShell for offline training.

## Offline Training

Use these runbooks as command notebooks:

```powershell
.\scripts\runbooks\offline_train_model.ps1
.\scripts\runbooks\offline_train_reduced.ps1
.\scripts\runbooks\offline_train_full.ps1
```

Each file contains ready-made commands.
Uncomment exactly one command block, then run the file.

Direct examples:

```powershell
.\.venv\Scripts\python.exe scripts/offline/run_offline_pipeline.py --phases 3 4 --feature-set reduced --models logistic_regression
.\.venv\Scripts\python.exe scripts/offline/run_offline_pipeline.py --phases 3 4 --feature-set reduced --models logistic_regression gradient_boosting
.\.venv\Scripts\python.exe scripts/offline/run_offline_pipeline.py --phases 3 --feature-set full --models random_forest
```

Recommended practical order before the final report:

1. run reduced once with all three models in one pass
2. review `artifacts/offline/evaluation/test_summary_reduced.csv`
3. run full shortlist with `random_forest` and `gradient_boosting`
4. add `logistic_regression` full only if you need a full 3x2 comparison table

Important artifact rule:

- filtered offline runs rewrite the feature-set summary files such as `valid_metrics_reduced.csv` and `test_summary_reduced.csv`
- if you want one coherent comparison table for a feature set, run all relevant models for that feature set in the same command

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


Prometheus and Grafana are optional sidecar observability only.

## Manual Streaming Run

Terminal 1, run the SUT:

```powershell
docker compose exec -T ids-dev python scripts/streaming/official/run_structured_streaming.py --config configs/streaming/streaming.yaml --model logistic_regression --feature-set full --run-tag smoke_test --input-run-tag smoke_test --run-seconds 180
```

Terminal 2, replay data:

```powershell
docker compose exec -T ids-dev python scripts/streaming/official/replay_parquet_to_kafka.py --config configs/streaming/streaming.yaml --run-tag smoke_test
```

Use `--available-now` only when the input topic already contains the data you want the stream to process.

## Profile Run

Run a named profile from the local workstation profile set:

```powershell
docker compose exec -T ids-dev python scripts/streaming/official/run_streaming_profile.py --profile-config experiments/streaming/profiles/local_profiles.yaml --profile smoke_gate
```

Profile execution wraps the lower-level matrix or benchmark script for that scenario.

Runbook style:

```powershell
.\scripts\runbooks\daily_light_regression.ps1
.\scripts\runbooks\daily_regression.ps1
.\scripts\runbooks\post_daily_light_regression.ps1
```

Each runbook is now a command notebook.
Uncomment the commands you want, then run the file.

Recommended report path after offline retraining:

1. run `smoke_gate`
2. run `layer_a_500k`
3. run `layer_b_500k`
4. run `watermark_500k`
5. run `layer_c_700k_fault`
6. run `stress_main_testx4`

## Matrix Run

Run a single matrix directly when you want explicit control:

```powershell
docker compose exec -T ids-dev python scripts/streaming/official/run_layer_a_matrix.py --config configs/streaming/streaming.yaml
docker compose exec -T ids-dev python scripts/streaming/official/run_layer_b_matrix.py --config configs/streaming/streaming.yaml
python scripts/streaming/official/run_layer_c_matrix.py --config configs/streaming/streaming.yaml
```

Layer C is the intentional host-orchestrated exception:
- run the Layer C matrix from the host when the scenario needs Docker fault injection
- keep `--execution-mode docker` for the SUT and replay subprocesses that Layer C launches
- the canonical implementation is split under `src/ids_platform/streaming/evaluation/matrices/layer_c/`
- `src/ids_platform/streaming/evaluation/matrices/layer_c_fault_matrix.py` is kept only as the compatibility entrypoint

Practical configuration notes:

- keep `logistic_regression + full` as the default streaming benchmark pair unless retrained full artifacts clearly show a better model with acceptable latency and operational stability
- treat reduced models mainly as comparison baselines unless retraining materially improves their false-positive behavior
- review `artifacts/streaming/evaluation/layer_c_summary_700k_fault.csv` before final report claims, because Kafka restart remains the slowest resilience path



## Live Telemetry Only

If you want live runtime telemetry during experiments:

```powershell
docker compose exec -T ids-dev python scripts/streaming/observability/export_prometheus_summary.py --config configs/streaming/streaming.yaml --port 9108
docker compose -f ops/observability/docker-compose.observability.yaml up -d
```

Use this for run health and debugging. Do not use it as the official benchmark source.
