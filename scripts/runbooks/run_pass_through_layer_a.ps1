$ErrorActionPreference = "Stop"

Write-Host "Run Layer A pass-through streaming baseline"
Write-Host "This baseline keeps replay + Kafka + Spark Structured Streaming + artifact collection,"
Write-Host "and disables ML scoring to isolate streaming-system overhead."

& docker compose exec -T ids-dev python scripts/streaming/official/run_layer_a_matrix.py `
  --config configs/streaming/streaming.yaml `
  --model random_forest `
  --feature-set full `
  --baseline-mode pass_through `
  --trace-input-parquet data/gold/splits/test.parquet `
  --trace-order-column event_time `
  --trace-rate-schedule "5000:180,10000:120" `
  --trace-stream-run-seconds 420 `
  --warmup-rows 50000 `
  --warmup-rate-schedule "1000:50" `
  --profiles "A_low:500:4:10 seconds" "A_mid:2000:8:10 seconds" "A_high:8000:16:10 seconds" `
  --repeats 1 `
  --max-rows 166667 `
  --batch-size 5000 `
  --metrics-timeout-sec 1500 `
  --run-prefix layerA_passthrough `
  --summary-csv artifacts/streaming/evaluation/layer_a_pass_through_summary_500k.csv

Write-Host "Completed Layer A pass-through streaming baseline."
