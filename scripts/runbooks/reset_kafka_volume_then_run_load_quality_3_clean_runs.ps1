$ErrorActionPreference = "Stop"

function Assert-VolumeExists {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    $exists = docker volume ls --format "{{.Name}}" | Where-Object { $_ -eq $Name }
    if (-not $exists) {
        throw "Kafka volume not found: $Name"
    }
}

$kafkaVolume = "design_initial_evalution_ids_kafka-data"

Write-Host "Reset Kafka volume, then run load-quality clean reruns."
Write-Host "This will DELETE all existing Kafka topic data in volume: $kafkaVolume"
Write-Host "Use this only when starting a fresh load-quality campaign."

Assert-VolumeExists -Name $kafkaVolume

Write-Host "Stopping stack..."
docker compose down | Out-Host

Write-Host "Removing Kafka data volume: $kafkaVolume"
docker volume rm $kafkaVolume | Out-Host

Write-Host "Starting clean stack..."
docker compose up -d zookeeper kafka ids-dev | Out-Host

Write-Host "Waiting 120 seconds for Kafka to become stable..."
Start-Sleep -Seconds 120

Write-Host "Starting load-quality 3 clean runs..."
& powershell -NoProfile -ExecutionPolicy Bypass -File "scripts/runbooks/repeat_load_quality_3_clean_runs.ps1"
