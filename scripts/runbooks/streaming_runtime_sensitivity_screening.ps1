$ErrorActionPreference = "Stop"

$TopicNamespace = "sensitivity_screen_" + (Get-Date -Format "yyyyMMddHHmmss")

Write-Host "Streaming runtime sensitivity screening runbook"
Write-Host "Runs fresh-process screening: 4 profiles x 2 RPS x pass-through/RF-Full."
Write-Host "Profiles:"
Write-Host "  latency        = 500ms trigger, 500 max offsets, shuffle 4"
Write-Host "  latency_high   = 500ms trigger, 1000 max offsets, shuffle 4"
Write-Host "  balanced       = 1s trigger, 1000 max offsets, shuffle 4"
Write-Host "  balanced_high  = 1s trigger, 2000 max offsets, shuffle 4"
Write-Host "RPS: 300, 500"
Write-Host "topic_namespace=$TopicNamespace"

docker compose up -d zookeeper kafka ids-dev
if ($LASTEXITCODE -ne 0) {
  exit $LASTEXITCODE
}

docker compose exec -T -e IDS_STREAMING_TOPIC_NAMESPACE=$TopicNamespace ids-dev python scripts/streaming/official/run_runtime_sensitivity_screening_fresh.py
if ($LASTEXITCODE -ne 0) {
  exit $LASTEXITCODE
}

Write-Host "Completed runtime sensitivity screening."
Write-Host "Summary:"
Write-Host "  artifacts/streaming/evaluation/capacity_runtime_sensitivity_screening.csv"
