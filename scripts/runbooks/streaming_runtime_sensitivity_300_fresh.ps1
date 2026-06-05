$ErrorActionPreference = "Stop"

$TopicNamespace = "sensitivity300_" + (Get-Date -Format "yyyyMMddHHmmss")

Write-Host "Streaming runtime sensitivity 300 rps fresh-process runbook"
Write-Host "Runs each profile/model combination in a separate Python process."
Write-Host "topic_namespace=$TopicNamespace"

docker compose up -d zookeeper kafka ids-dev
if ($LASTEXITCODE -ne 0) {
  exit $LASTEXITCODE
}

docker compose exec -T -e IDS_STREAMING_TOPIC_NAMESPACE=$TopicNamespace ids-dev python scripts/streaming/official/run_runtime_sensitivity_300_fresh.py
if ($LASTEXITCODE -ne 0) {
  exit $LASTEXITCODE
}

Write-Host "Completed runtime sensitivity 300 rps fresh-process run."
Write-Host "Summary:"
Write-Host "  artifacts/streaming/evaluation/capacity_runtime_sensitivity_300_fresh.csv"
