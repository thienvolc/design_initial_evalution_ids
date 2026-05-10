$ErrorActionPreference = "Stop"

$PY = ".\.venv\Scripts\python.exe"

Write-Host "Build repeated-run summaries for Layer A, Layer B, Layer C, watermark, and load-quality"

& $PY scripts/streaming/official/build_repeated_run_stats.py `
  --output-dir artifacts/streaming/evaluation/repeated_run_stats

Write-Host "Completed repeated-run summary build."
