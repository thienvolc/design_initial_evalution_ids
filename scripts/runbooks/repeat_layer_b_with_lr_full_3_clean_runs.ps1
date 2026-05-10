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

Write-Host "Clean rerun Layer B with logistic_regression:full baseline: run 1, run 2, run 3"
Write-Host "This script keeps the current Layer B benchmark setup and adds the missing logistic_regression:full pair."
Write-Host "All outputs are written to new summary CSV files, so existing Layer B artifacts are preserved."

$summaryRun01 = "artifacts/streaming/evaluation/layer_b_summary_500k_with_lr_full_rerun01.csv"
$summaryRun02 = "artifacts/streaming/evaluation/layer_b_summary_500k_with_lr_full_rerun02.csv"
$summaryRun03 = "artifacts/streaming/evaluation/layer_b_summary_500k_with_lr_full_rerun03.csv"

$kafkaVolume = "design_initial_evalution_ids_kafka-data"

Assert-FileDoesNotExist -Path $summaryRun01
# Assert-FileDoesNotExist -Path $summaryRun02
# Assert-FileDoesNotExist -Path $summaryRun03

Write-Host "Stopping stack..."
docker compose down | Out-Host

Write-Host "Removing Kafka data volume: $kafkaVolume"
docker volume rm $kafkaVolume | Out-Host

Write-Host "Starting clean stack..."
docker compose up -d zookeeper kafka ids-dev | Out-Host

Write-Host "Waiting 120 seconds for Kafka to become stable..."
Start-Sleep -Seconds 180


# Layer B - run 1
& docker compose exec -T ids-dev python scripts/streaming/official/run_layer_b_matrix.py `
  --config configs/streaming/streaming.yaml `
  --trace-input-parquet data/gold/splits/test.parquet `
  --trace-order-column event_time `
  --trace-rate-schedule "5000:180,10000:120" `
  --trace-stream-run-seconds 420 `
  --warmup-rows 50000 `
  --warmup-rate-schedule "1000:50" `
  --model-feature-pairs "logistic_regression:full" "logistic_regression:reduced" "random_forest:reduced" "random_forest:full" "gradient_boosting:reduced" `
  --repeats 1 `
  --max-rows 125000 `
  --batch-size 5000 `
  --metrics-timeout-sec 1500 `
  --run-prefix layerB_lrfull_rerun01 `
  --summary-csv $summaryRun01


# Start-Sleep -Seconds 10
# Write-Host "Stopping stack..."
# docker compose down | Out-Host

# Write-Host "Removing Kafka data volume: $kafkaVolume"
# docker volume rm $kafkaVolume | Out-Host

# Write-Host "Starting clean stack..."
# docker compose up -d zookeeper kafka ids-dev | Out-Host

# Write-Host "Waiting 120 seconds for Kafka to become stable..."
# Start-Sleep -Seconds 180


# # Layer B - run 2
# & docker compose exec -T ids-dev python scripts/streaming/official/run_layer_b_matrix.py `
#   --config configs/streaming/streaming.yaml `
#   --trace-input-parquet data/gold/splits/test.parquet `
#   --trace-order-column event_time `
#   --trace-rate-schedule "5000:180,10000:120" `
#   --trace-stream-run-seconds 420 `
#   --warmup-rows 50000 `
#   --warmup-rate-schedule "1000:50" `
#   --model-feature-pairs "logistic_regression:full" "logistic_regression:reduced" "random_forest:reduced" "random_forest:full" "gradient_boosting:reduced" `
#   --repeats 1 `
#   --max-rows 125000 `
#   --batch-size 5000 `
#   --metrics-timeout-sec 1500 `
#   --run-prefix layerB_lrfull_rerun02 `
#   --summary-csv $summaryRun02

# Start-Sleep -Seconds 10
# Write-Host "Stopping stack..."
# docker compose down | Out-Host

# Write-Host "Removing Kafka data volume: $kafkaVolume"
# docker volume rm $kafkaVolume | Out-Host

# Write-Host "Starting clean stack..."
# docker compose up -d zookeeper kafka ids-dev | Out-Host

# Write-Host "Waiting 120 seconds for Kafka to become stable..."
# Start-Sleep -Seconds 180

# # Layer B - run 3
# & docker compose exec -T ids-dev python scripts/streaming/official/run_layer_b_matrix.py `
#   --config configs/streaming/streaming.yaml `
#   --trace-input-parquet data/gold/splits/test.parquet `
#   --trace-order-column event_time `
#   --trace-rate-schedule "5000:180,10000:120" `
#   --trace-stream-run-seconds 420 `
#   --warmup-rows 50000 `
#   --warmup-rate-schedule "1000:50" `
#   --model-feature-pairs "logistic_regression:full" "logistic_regression:reduced" "random_forest:reduced" "random_forest:full" "gradient_boosting:reduced" `
#   --repeats 1 `
#   --max-rows 125000 `
#   --batch-size 5000 `
#   --metrics-timeout-sec 1500 `
#   --run-prefix layerB_lrfull_rerun03 `
#   --summary-csv $summaryRun03

# Write-Host "Completed clean Layer B rerun 1, run 2, and run 3 with logistic_regression:full."

# Write-Host "Stopping stack..."
# docker compose down | Out-Host

# Write-Host "Removing Kafka data volume: $kafkaVolume"
# docker volume rm $kafkaVolume | Out-Host

# Write-Host "Starting clean stack..."
# docker compose up -d zookeeper kafka ids-dev | Out-Host

# Write-Host "Waiting 120 seconds for Kafka to become stable..."
# Start-Sleep -Seconds 180
