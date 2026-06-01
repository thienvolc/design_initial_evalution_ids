# Execution Runbook

## Default Rule

Use host PowerShell for offline training and Docker for streaming work.

## Offline Training

Use these runbooks as command notebooks:

```powershell
.\scripts\runbooks\offline_train_model.ps1
.\scripts\runbooks\offline_train_reduced.ps1
.\scripts\runbooks\offline_train_full.ps1
```

Direct examples:

```powershell
.\.venv\Scripts\python.exe scripts/offline/run_offline_pipeline.py --phases 3 4 --feature-set reduced --models logistic_regression
.\.venv\Scripts\python.exe scripts/offline/run_offline_pipeline.py --phases 3 --feature-set full --models random_forest
```

## Streaming Flow

Streaming uses Python config modules, not YAML profiles or CLI matrices.

Active flow:

1. Edit/select config in `src/ids_platform/streaming/config/`.
2. Start Docker services.
3. Run an official matrix script.
4. Read summary CSVs, metrics time series, parquet predictions, and plots from `artifacts/streaming/`.

Start services:

```powershell
docker compose up -d zookeeper kafka ids-dev
```

Run matrices:

```powershell
docker compose exec -T ids-dev python scripts/streaming/official/run_layer_a_matrix.py
docker compose exec -T ids-dev python scripts/streaming/official/run_layer_b_matrix.py
docker compose exec -T ids-dev python scripts/streaming/official/run_watermark_matrix.py
docker compose exec -T ids-dev python scripts/streaming/official/run_layer_c_matrix.py
docker compose exec -T ids-dev python scripts/streaming/official/run_load_quality_matrix.py
```

Build plots after matrix runs:

```powershell
.\.venv\Scripts\python.exe scripts/streaming/official/build_timeseries_plots.py `
  --inputs artifacts/streaming/metrics_timeseries `
  --output-dir artifacts/streaming/plots/final
```

## Active Artifacts

- Summary CSVs: `artifacts/streaming/evaluation/`
- Metrics time series: `artifacts/streaming/metrics_timeseries/`
- Prediction parquet artifacts: `artifacts/streaming/predictions/`
- Plots: `artifacts/streaming/plots/`

Prometheus/Grafana and YAML profile execution were removed from the active runbook because they were not part of the authoritative benchmark path.
