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

function Restart-ContainerIfExists {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    $exists = docker ps -a --format "{{.Names}}" | Where-Object { $_ -eq $Name }
    if ($exists) {
        & docker restart $Name | Out-Host
    } else {
        Write-Host "Skip restart for missing container: $Name"
    }
}

Write-Host "Clean rerun load-quality matrix: run 1, run 2, run 3"
Write-Host "This script uses the fixed load-quality flow:"
Write-Host "stream first, starting_offsets=latest, replay second, stop on input sentinel."
Write-Host "The orchestration now waits for graceful stop markers such as input_sentinel_seen,"
Write-Host "reason=input_sentinel, and job_stop before collecting the final summary."
Write-Host "This version keeps the current step-load schedule and increases trace-max-rows so the full schedule is replayed."
Write-Host "Expect each run to take substantially longer than the old 150k-row capped run."
Write-Host "All outputs are written to new summary CSV files, so existing official artifacts are preserved."

$traceMaxRows = 5496572
$summaryRun01 = "artifacts/streaming/evaluation/load_quality_summary_stress_fullschedule_rerun01.csv"
$summaryRun02 = "artifacts/streaming/evaluation/load_quality_summary_stress_fullschedule_rerun02.csv"
$summaryRun03 = "artifacts/streaming/evaluation/load_quality_summary_stress_fullschedule_rerun03.csv"

$kafkaVolume = "design_initial_evalution_ids_kafka-data"

Assert-FileDoesNotExist -Path $summaryRun01
Assert-FileDoesNotExist -Path $summaryRun02
Assert-FileDoesNotExist -Path $summaryRun03

# docker compose up -d zookeeper kafka ids-dev | Out-Host

# Load-quality - run 1
& docker compose exec -T ids-dev python scripts/streaming/official/run_load_quality_matrix.py `
  --config configs/streaming/streaming.yaml `
  --model random_forest `
  --feature-set full `
  --trace-profile-name trace_step_rate_main `
  --trace-stream-run-seconds 5400 `
  --warmup-rows 120000 `
  --warmup-rate-schedule "5000:24" `
  --trace-rate-schedule "5000:180,10000:180,20000:139.8286" `
  --trace-max-rows $traceMaxRows `
  --batch-size 10000 `
  --metrics-timeout-sec 6000 `
  --summary-csv $summaryRun01

Start-Sleep -Seconds 10
Write-Host "Stopping stack..."
docker compose down | Out-Host

Write-Host "Removing Kafka data volume: $kafkaVolume"
docker volume rm $kafkaVolume | Out-Host

Write-Host "Starting clean stack..."
docker compose up -d zookeeper kafka ids-dev | Out-Host

Write-Host "Waiting 180 seconds for Kafka to become stable..."
Start-Sleep -Seconds 180

# Load-quality - run 2
& docker compose exec -T ids-dev python scripts/streaming/official/run_load_quality_matrix.py `
  --config configs/streaming/streaming.yaml `
  --model random_forest `
  --feature-set full `
  --trace-profile-name trace_step_rate_main `
  --trace-stream-run-seconds 5400 `
  --warmup-rows 120000 `
  --warmup-rate-schedule "5000:24" `
  --trace-rate-schedule "5000:180,10000:180,20000:139.8286" `
  --trace-max-rows $traceMaxRows `
  --batch-size 10000 `
  --metrics-timeout-sec 6000 `
  --summary-csv $summaryRun02

Start-Sleep -Seconds 10
Write-Host "Stopping stack..."
docker compose down | Out-Host

Write-Host "Removing Kafka data volume: $kafkaVolume"
docker volume rm $kafkaVolume | Out-Host

Write-Host "Starting clean stack..."
docker compose up -d zookeeper kafka ids-dev | Out-Host

Write-Host "Waiting 120 seconds for Kafka to become stable..."
Start-Sleep -Seconds 180


# Load-quality - run 3
& docker compose exec -T ids-dev python scripts/streaming/official/run_load_quality_matrix.py `
  --config configs/streaming/streaming.yaml `
  --model random_forest `
  --feature-set full `
  --trace-profile-name trace_step_rate_main `
  --trace-stream-run-seconds 5400 `
  --warmup-rows 120000 `
  --warmup-rate-schedule "5000:24" `
  --trace-rate-schedule "5000:180,10000:180,20000:139.8286" `
  --trace-max-rows $traceMaxRows `
  --batch-size 10000 `
  --metrics-timeout-sec 6000 `
  --summary-csv $summaryRun03

Write-Host "Completed clean load-quality rerun 1, run 2, and run 3."
