$ErrorActionPreference = "Stop"

$RunStamp = Get-Date -Format "yyyyMMddHHmmss"
$TopicNamespace = "fault_recovery_" + $RunStamp
$SummaryCsv = "artifacts/streaming/evaluation/fault_recovery_3run_$RunStamp.csv"

Write-Host "Streaming fault recovery runbook"
Write-Host "Runtime profile: 1s trigger, 1000 max offsets, shuffle 4, local[4]."
Write-Host "Model: RF-Full"
Write-Host "Scenarios:"
Write-Host "  cold start @ 300 rps"
Write-Host "  Spark crash/restart from checkpoint @ 300 rps"
Write-Host "  Spark crash/restart from checkpoint @ 500 rps"
Write-Host "Repeats: 3"
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
  ids-dev python scripts/streaming/official/run_fault_recovery_reproducible.py
if ($LASTEXITCODE -ne 0) {
  exit $LASTEXITCODE
}

Write-Host "Completed fault recovery."
Write-Host "Summary:"
Write-Host "  $SummaryCsv"
