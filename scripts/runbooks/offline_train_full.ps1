$ErrorActionPreference = "Stop"

$PY = ".\.venv\Scripts\python.exe"

Write-Host "Offline Spark full runbook"
Write-Host "Runs phase 3 only: train, threshold calibration, test evaluation, and Spark artifact export."

& $PY scripts/offline/run_offline_pipeline.py --phases 3 --feature-set full --models logistic_regression gradient_boosting random_forest
