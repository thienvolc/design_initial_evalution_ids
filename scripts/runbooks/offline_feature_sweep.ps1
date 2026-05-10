$ErrorActionPreference = "Stop"

$PY = ".\.venv\Scripts\python.exe"
$RunTag = "feature_sweep_" + (Get-Date -Format "yyyyMMdd_HHmmss")

Write-Host "Offline reduced-feature sweep"
Write-Host "This runbook writes versioned CSV artifacts and does not overwrite the base files by default."
Write-Host "run_tag=$RunTag"

& $PY scripts/offline/feature_sweep.py `
  --run-tag $RunTag `
  --models logistic_regression random_forest gradient_boosting `
  --k-values 10 17 25 40 78

if ($LASTEXITCODE -ne 0) {
  exit $LASTEXITCODE
}

Write-Host "Completed offline feature sweep."
