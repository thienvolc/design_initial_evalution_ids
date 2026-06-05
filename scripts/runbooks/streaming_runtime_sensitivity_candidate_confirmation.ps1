$ErrorActionPreference = "Stop"

$RunStamp = Get-Date -Format "yyyyMMddHHmmss"
$TopicNamespace = "sensitivity_candidate_" + $RunStamp
$RepeatCount = 3
if ($env:IDS_STREAMING_REPEAT_COUNT) {
  $RepeatCount = [int]$env:IDS_STREAMING_REPEAT_COUNT
}
$SummaryCsv = "artifacts/streaming/evaluation/capacity_runtime_sensitivity_candidate_confirmation_$RunStamp.csv"

Write-Host "Streaming runtime sensitivity candidate confirmation"
Write-Host "Runs fresh-process candidates: 2 profiles x pass-through/RF-Full x $RepeatCount repeat(s)."
Write-Host "Candidates:"
Write-Host "  A = 300 rps, 500ms trigger, 500 max offsets, shuffle 4"
Write-Host "  B = 500 rps, 1s trigger, 1000 max offsets, shuffle 4"
Write-Host "topic_namespace=$TopicNamespace"
Write-Host "summary_csv=$SummaryCsv"

docker compose up -d zookeeper kafka ids-dev
if ($LASTEXITCODE -ne 0) {
  exit $LASTEXITCODE
}

docker compose exec -T `
  -e IDS_STREAMING_TOPIC_NAMESPACE=$TopicNamespace `
  -e IDS_STREAMING_RUN_STAMP=$RunStamp `
  -e IDS_STREAMING_REPEAT_COUNT=$RepeatCount `
  -e IDS_STREAMING_SUMMARY_CSV=$SummaryCsv `
  ids-dev python scripts/streaming/official/run_runtime_sensitivity_candidate_confirmation_fresh.py
if ($LASTEXITCODE -ne 0) {
  exit $LASTEXITCODE
}

Write-Host "Completed runtime sensitivity candidate confirmation."
Write-Host "Summary:"
Write-Host "  $SummaryCsv"
