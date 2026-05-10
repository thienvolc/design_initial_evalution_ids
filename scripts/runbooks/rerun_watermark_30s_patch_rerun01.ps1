$ErrorActionPreference = "Stop"

function Assert-FileDoesNotExist {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    if (Test-Path -LiteralPath $Path) {
        throw "Refusing to overwrite existing file: $Path"
    }
}

Write-Host "Patch rerun for the missing watermark 30s case in rerun01"
Write-Host "This script reruns only watermark_delay_sec=30 with the same full-schedule settings."
Write-Host "It does not overwrite the existing 0s / 10s artifacts."
Write-Host "The output summary is a temporary patch CSV that will later be merged into rerun01."

$summaryPatch = "artifacts/streaming/evaluation/watermark_summary_fullschedule_rerun01_patch_30s.csv"
$traceMaxRows = 2100000

Assert-FileDoesNotExist -Path $summaryPatch

docker compose up -d zookeeper kafka ids-dev | Out-Host

& docker compose exec -T ids-dev python scripts/streaming/official/run_watermark_matrix.py `
  --config configs/streaming/streaming.yaml `
  --model random_forest `
  --feature-set full `
  --trace-input-parquet data/gold/splits/test.parquet `
  --trace-order-column event_time `
  --trace-rate-schedule "5000:180,10000:120" `
  --trace-stream-run-seconds 900 `
  --trace-startup-wait-sec 8 `
  --warmup-rows 50000 `
  --warmup-rate-schedule "1000:50" `
  --warmup-stream-run-seconds 0 `
  --watermark-delays 30 `
  --drop-late-events `
  --max-rows $traceMaxRows `
  --batch-size 5000 `
  --metrics-timeout-sec 2400 `
  --summary-csv $summaryPatch

Write-Host "Completed watermark 30s patch rerun."
Write-Host "Patch summary written to: $summaryPatch"
