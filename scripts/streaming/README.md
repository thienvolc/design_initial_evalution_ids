# Streaming Scripts

Streaming now uses Python config modules as the single source of truth.
The default official matrix scripts run smoke gates only.

The active paper flow is:

1. Build a config in `src/ids_platform/streaming/config/`.
2. Run a matrix script from `scripts/streaming/official/`.
3. Replay writes Kafka input records.
4. Runtime writes prediction parquet artifacts and operational `ids.metrics`.
5. Evaluation merges operational metrics with parquet quality summaries into CSV outputs.

YAML profile runners, old benchmark scripts, and external dashboard assets were removed because they were not authoritative inputs for the report.

## Active Entrypoints

- `official/run_capacity_calibration.py`
- `official/run_layer_b_matrix.py`
- `official/run_layer_c_matrix.py`
- `official/run_watermark_matrix.py`
- `official/run_load_quality_matrix.py`
- `official/build_timeseries_plots.py`

`official/run_structured_streaming.py` and `official/replay_parquet_to_kafka.py` remain thin debug entrypoints. Normal runs should go through the matrix scripts so sentinel, metrics collection, and quality summarization stay coordinated.

## Docker Path

Use Docker for streaming runs:

```powershell
docker compose up -d zookeeper kafka ids-dev
docker compose exec -T ids-dev python scripts/streaming/official/run_capacity_calibration.py
```

The matrix scripts are config-driven and intentionally accept no CLI flags. Defaults are smoke gates with 1000 rows at 500 rps for timeout and sentinel checks. Use the `build_*_main_config()` functions in `src/ids_platform/streaming/config/` when preparing larger paper runs.

Run `official/run_capacity_calibration.py` as the capacity section's operating-point benchmark. It is also the source for selecting stable, degraded, and overload RPS values before broader streaming runs.

## Report Artifacts

- Summary CSVs: `artifacts/streaming/evaluation/`
- Per-run metrics time series: `artifacts/streaming/metrics_timeseries/`
- Prediction parquet artifacts: `artifacts/streaming/predictions/`
- Plots: `artifacts/streaming/plots/`
