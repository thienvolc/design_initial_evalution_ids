$ErrorActionPreference = "Stop"

$PY = ".\.venv\Scripts\python.exe"
$RunTag = "ablation_" + (Get-Date -Format "yyyyMMdd_HHmmss")

Write-Host "Offline reduced-feature ablation"
Write-Host "This runbook writes versioned CSV artifacts and does not overwrite the base files by default."
Write-Host "run_tag=$RunTag"

& $PY scripts/offline/ablation_gbt.py `
  --run-tag $RunTag `
  --model gradient_boosting

if ($LASTEXITCODE -ne 0) {
  exit $LASTEXITCODE
}

Write-Host "Completed offline reduced-feature ablation."
