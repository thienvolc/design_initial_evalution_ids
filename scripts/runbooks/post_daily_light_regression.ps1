$ErrorActionPreference = "Stop"

$PY = ".\.venv\Scripts\python.exe"

Write-Host "Streaming report runbook"
Write-Host "Uncomment the command set you want, then run this file."
Write-Host "Recommended final flow: build optional plots."

# Optional paper plots from summary CSVs
# & $PY scripts/paper/build_paper_plots.py
