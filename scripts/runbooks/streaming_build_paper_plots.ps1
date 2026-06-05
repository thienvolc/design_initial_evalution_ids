$ErrorActionPreference = "Stop"

$PY = ".\.venv\Scripts\python.exe"

Write-Host "Streaming paper plot runbook"
Write-Host "Builds latency/throughput time series and paper plots from existing streaming artifacts."

& $PY scripts/paper/build_streaming_experiment_outputs.py

Write-Host "Completed paper plot generation."
Write-Host "Plots: paper/figures/plots"
