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

Write-Host "Clean rerun watermark matrix: run 1, run 2, run 3"
Write-Host "This script uses the fixed watermark flow:"
Write-Host "stream first, starting_offsets=latest, replay second, stop on input sentinel."
Write-Host "This version keeps the current watermark schedule and increases max_rows so the full schedule is replayed."
Write-Host "All outputs are written to new summary CSV files, so old incorrect artifacts are preserved but not overwritten."

$traceMaxRows = 2100000
# $summaryRun01 = "artifacts/streaming/evaluation/watermark_summary_fullschedule_rerun01.csv"
$summaryRun02 = "artifacts/streaming/evaluation/watermark_summary_fullschedule_rerun02.csv"
$summaryRun03 = "artifacts/streaming/evaluation/watermark_summary_fullschedule_rerun03.csv"

# Assert-FileDoesNotExist -Path $summaryRun01
Assert-FileDoesNotExist -Path $summaryRun02
Assert-FileDoesNotExist -Path $summaryRun03

# Watermark - run 1
# & docker compose exec -T ids-dev python scripts/streaming/official/run_watermark_matrix.py `
#   --config configs/streaming/streaming.yaml `
#   --model random_forest `
#   --feature-set full `
#   --trace-input-parquet data/gold/splits/test.parquet `
#   --trace-order-column event_time `
#   --trace-rate-schedule "5000:180,10000:120" `
#   --trace-stream-run-seconds 900 `
#   --trace-startup-wait-sec 8 `
#   --warmup-rows 50000 `
#   --warmup-rate-schedule "1000:50" `
#   --warmup-stream-run-seconds 0 `
#   --watermark-delays 0 10 30 `
#   --drop-late-events `
#   --max-rows $traceMaxRows `
#   --batch-size 5000 `
#   --metrics-timeout-sec 2400 `
#   --summary-csv $summaryRun01

# Write-Host "Sleep 120 seconds before next run..."
# Start-Sleep -Seconds 120

# Watermark - run 2
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
  --watermark-delays 0 10 30 `
  --drop-late-events `
  --max-rows $traceMaxRows `
  --batch-size 5000 `
  --metrics-timeout-sec 2400 `
  --summary-csv $summaryRun02

Start-Sleep -Seconds 10
& docker restart ids-kafka
Write-Host "Sleep 120 seconds before next run..."
Start-Sleep -Seconds 120

# Watermark - run 3
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
  --watermark-delays 0 10 30 `
  --drop-late-events `
  --max-rows $traceMaxRows `
  --batch-size 5000 `
  --metrics-timeout-sec 2400 `
  --summary-csv $summaryRun03

Write-Host "Completed clean watermark rerun 1, run 2, and run 3."
