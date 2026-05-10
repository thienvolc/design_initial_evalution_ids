$ErrorActionPreference = "Stop"

$PY = ".\.venv\Scripts\python.exe"
$timeseriesDir = "artifacts/streaming/metrics_timeseries"

Write-Host "Streaming report runbook"
Write-Host "Uncomment the command set you want, then run this file."
Write-Host "Recommended final flow: build optional plots."

# Optional timeseries plots
# & $PY scripts/streaming/official/build_timeseries_plots.py `
#   --inputs $timeseriesDir `
#   --output-dir artifacts/streaming/plots/final

