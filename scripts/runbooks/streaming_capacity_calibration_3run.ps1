$ErrorActionPreference = "Stop"

$RunStamp = Get-Date -Format "yyyyMMddHHmmss"
$TopicNamespace = "capacity3_" + $RunStamp
$RepeatCount = 3
if ($env:IDS_STREAMING_REPEAT_COUNT) {
  $RepeatCount = [int]$env:IDS_STREAMING_REPEAT_COUNT
}
$SummaryCsv = "artifacts/streaming/evaluation/capacity_load_threshold_3run_$RunStamp.csv"

Write-Host "Streaming capacity calibration reproducible runbook"
Write-Host "Runs capacity threshold with the selected runtime profile."
Write-Host "Runtime profile: 1s trigger, 1000 max offsets, shuffle 4, local[4]."
Write-Host "Workloads:"
Write-Host "  pass-through: 500, 600, 750 rps"
Write-Host "  RF-Full:      300, 500, 600 rps"
Write-Host "repeats=$RepeatCount"
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
  ids-dev python scripts/streaming/official/run_capacity_calibration_reproducible.py
if ($LASTEXITCODE -ne 0) {
  exit $LASTEXITCODE
}

Write-Host "Completed 3-run capacity calibration."
Write-Host "Summary:"
Write-Host "  $SummaryCsv"
