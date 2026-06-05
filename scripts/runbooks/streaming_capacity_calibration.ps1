$ErrorActionPreference = "Stop"

$TopicNamespace = "capacity_" + (Get-Date -Format "yyyyMMddHHmmss")

Write-Host "Streaming capacity calibration runbook"
Write-Host "Runs one-repeat 1s/1000-offset threshold validation."
Write-Host "Workloads:"
Write-Host "  pass-through: 500, 600, 750 rps"
Write-Host "  RF-Full:      300, 500, 600 rps"
Write-Host "topic_namespace=$TopicNamespace"

docker compose up -d zookeeper kafka ids-dev
if ($LASTEXITCODE -ne 0) {
  exit $LASTEXITCODE
}
docker compose exec -T -e IDS_STREAMING_TOPIC_NAMESPACE=$TopicNamespace ids-dev python scripts/streaming/official/run_capacity_calibration_benchmarks.py
if ($LASTEXITCODE -ne 0) {
  exit $LASTEXITCODE
}

Write-Host "Completed capacity calibration."
Write-Host "Summaries:"
Write-Host "  artifacts/streaming/evaluation/capacity_load_threshold.csv"
