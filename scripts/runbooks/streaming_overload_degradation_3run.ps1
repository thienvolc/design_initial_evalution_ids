$ErrorActionPreference = "Stop"

$RunStamp = Get-Date -Format "yyyyMMddHHmmss"
$TopicNamespace = "overload_" + $RunStamp
$SummaryCsv = "artifacts/streaming/evaluation/overload_degradation_3run_$RunStamp.csv"

Write-Host "Streaming overload degradation runbook"
Write-Host "Runtime profile: 1s trigger, 1000 max offsets, shuffle 4, local[4]."
Write-Host "Model: RF-Full"
Write-Host "Replay mode: burst"
Write-Host "Profiles:"
Write-Host "  500 rps = operating point control"
Write-Host "  600 rps = pressure / near capacity"
Write-Host "  750 rps = overload candidate"
Write-Host "  900 rps = clear overload candidate"
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
  ids-dev python scripts/streaming/official/run_overload_degradation_reproducible.py
if ($LASTEXITCODE -ne 0) {
  exit $LASTEXITCODE
}

Write-Host "Completed overload degradation."
Write-Host "Summary:"
Write-Host "  $SummaryCsv"
