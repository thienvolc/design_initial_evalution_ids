$ErrorActionPreference = "Stop"

$PY = ".\.venv\Scripts\python.exe"

Write-Host "Offline Spark reduced runbook"
Write-Host "Runs RF-17 only for the current model-feature tradeoff experiment."

& $PY scripts/offline/run_offline_pipeline.py --phases 3 --feature-set reduced --models random_forest
