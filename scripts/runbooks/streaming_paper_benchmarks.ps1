$ErrorActionPreference = "Stop"

$TopicNamespace = "paper_" + (Get-Date -Format "yyyyMMddHHmmss")

Write-Host "Streaming paper benchmark runbook"
Write-Host "Runs the four main config-driven experiments inside Docker."
Write-Host "topic_namespace=$TopicNamespace"

docker compose up -d zookeeper kafka ids-dev
if ($LASTEXITCODE -ne 0) {
  exit $LASTEXITCODE
}
docker compose exec -T -e IDS_STREAMING_TOPIC_NAMESPACE=$TopicNamespace ids-dev python scripts/streaming/official/run_paper_benchmarks.py
if ($LASTEXITCODE -ne 0) {
  exit $LASTEXITCODE
}

Write-Host "Completed streaming paper benchmarks."
Write-Host "Summary CSVs: artifacts/streaming/evaluation"
Write-Host "Kafka lag time series: artifacts/streaming/kafka_lag_timeseries"
Write-Host "Prediction parquet artifacts: artifacts/streaming/predictions"
