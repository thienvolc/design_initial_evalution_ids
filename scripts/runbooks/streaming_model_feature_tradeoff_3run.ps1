$ErrorActionPreference = "Stop"

$RunStamp = Get-Date -Format "yyyyMMddHHmmss"
$TopicNamespace = "feature_tradeoff_" + $RunStamp
$SummaryCsv = "artifacts/streaming/evaluation/model_feature_tradeoff_3run_$RunStamp.csv"

Write-Host "Streaming model-feature tradeoff runbook"
Write-Host "Runtime profile: 1s trigger, 1000 max offsets, shuffle 4, local[4]."
Write-Host "Workload: 500 rps, constant ticked replay."
Write-Host "Runs:"
Write-Host "  RF-Full x 3 repeats"
Write-Host "  RF-17   x 3 repeats"
Write-Host "topic_namespace=$TopicNamespace"
Write-Host "summary_csv=$SummaryCsv"

docker compose up -d zookeeper kafka ids-dev
if ($LASTEXITCODE -ne 0) {
  exit $LASTEXITCODE
}

docker compose exec -T `
  -e IDS_STREAMING_TOPIC_NAMESPACE=$TopicNamespace `
  -e IDS_STREAMING_RUN_STAMP=$RunStamp `
  -e IDS_STREAMING_SUMMARY_CSV=$SummaryCsv `
  ids-dev python scripts/streaming/official/run_model_feature_tradeoff_reproducible.py
if ($LASTEXITCODE -ne 0) {
  exit $LASTEXITCODE
}

Write-Host "Completed model-feature tradeoff."
Write-Host "Summary:"
Write-Host "  $SummaryCsv"
