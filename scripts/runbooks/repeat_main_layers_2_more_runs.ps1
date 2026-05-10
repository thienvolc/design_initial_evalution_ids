$ErrorActionPreference = "Stop"

$PY = ".\.venv\Scripts\python.exe"

Write-Host "Repeat main paper layers: add run 2 and run 3"
Write-Host "This script runs Layer A, Layer B, Layer C sequentially."
Write-Host "Existing official summaries are preserved because reruns write to new summary CSV files."

# Layer A - run 2
& docker compose exec -T ids-dev python scripts/streaming/official/run_layer_a_matrix.py `
  --config configs/streaming/streaming.yaml `
  --model random_forest `
  --feature-set full `
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
  --run-prefix layerA_scaleup_rerun02 `
  --summary-csv artifacts/streaming/evaluation/layer_a_summary_500k_rerun02.csv

Write-Host "Sleep 120 seconds before next run..."
Start-Sleep -Seconds 120

# Layer A - run 3
& docker compose exec -T ids-dev python scripts/streaming/official/run_layer_a_matrix.py `
  --config configs/streaming/streaming.yaml `
  --model random_forest `
  --feature-set full `
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
  --run-prefix layerA_scaleup_rerun03 `
  --summary-csv artifacts/streaming/evaluation/layer_a_summary_500k_rerun03.csv

Write-Host "Sleep 120 seconds before next run..."
Start-Sleep -Seconds 120

# Layer B - run 2
& docker compose exec -T ids-dev python scripts/streaming/official/run_layer_b_matrix.py `
  --config configs/streaming/streaming.yaml `
  --trace-input-parquet data/gold/splits/test.parquet `
  --trace-order-column event_time `
  --trace-rate-schedule "5000:180,10000:120" `
  --trace-stream-run-seconds 420 `
  --warmup-rows 50000 `
  --warmup-rate-schedule "1000:50" `
  --model-feature-pairs "logistic_regression:reduced" "random_forest:reduced" "random_forest:full" "gradient_boosting:reduced" `
  --repeats 1 `
  --max-rows 125000 `
  --batch-size 5000 `
  --metrics-timeout-sec 1500 `
  --run-prefix layerB_scaleup_rerun02 `
  --summary-csv artifacts/streaming/evaluation/layer_b_summary_500k_rerun02.csv

Write-Host "Sleep 120 seconds before next run..."
Start-Sleep -Seconds 120

# Layer B - run 3
& docker compose exec -T ids-dev python scripts/streaming/official/run_layer_b_matrix.py `
  --config configs/streaming/streaming.yaml `
  --trace-input-parquet data/gold/splits/test.parquet `
  --trace-order-column event_time `
  --trace-rate-schedule "5000:180,10000:120" `
  --trace-stream-run-seconds 420 `
  --warmup-rows 50000 `
  --warmup-rate-schedule "1000:50" `
  --model-feature-pairs "logistic_regression:reduced" "random_forest:reduced" "random_forest:full" "gradient_boosting:reduced" `
  --repeats 1 `
  --max-rows 125000 `
  --batch-size 5000 `
  --metrics-timeout-sec 1500 `
  --run-prefix layerB_scaleup_rerun03 `
  --summary-csv artifacts/streaming/evaluation/layer_b_summary_500k_rerun03.csv

Write-Host "Sleep 120 seconds before next run..."
Start-Sleep -Seconds 120

# Layer C - run 2
& $PY scripts/streaming/official/run_layer_c_matrix.py `
  --config configs/streaming/streaming.yaml `
  --execution-mode docker `
  --model random_forest `
  --feature-set full `
  --trace-input-parquet data/gold/splits/test.parquet `
  --trace-order-column event_time `
  --scenarios kafka_restart spark_process_restart producer_restart network_slowdown `
  --warmup-rows 120000 `
  --warmup-rate-schedule "5000:24" `
  --fault-delay-sec 45 `
  --post-fault-rows 180000 `
  --post-fault-rows-per-sec 10000 `
  --batch-size 5000 `
  --stream-run-seconds 1200 `
  --startup-wait-sec 90 `
  --metrics-timeout-sec 1200 `
  --replay-retries 8 `
  --replay-retry-wait-sec 5 `
  --summary-csv artifacts/streaming/evaluation/layer_c_summary_700k_fault_rerun02.csv

Write-Host "Sleep 120 seconds before next run..."
Start-Sleep -Seconds 120

# Layer C - run 3
& $PY scripts/streaming/official/run_layer_c_matrix.py `
  --config configs/streaming/streaming.yaml `
  --execution-mode docker `
  --model random_forest `
  --feature-set full `
  --trace-input-parquet data/gold/splits/test.parquet `
  --trace-order-column event_time `
  --scenarios kafka_restart spark_process_restart producer_restart network_slowdown `
  --warmup-rows 120000 `
  --warmup-rate-schedule "5000:24" `
  --fault-delay-sec 45 `
  --post-fault-rows 180000 `
  --post-fault-rows-per-sec 10000 `
  --batch-size 5000 `
  --stream-run-seconds 1200 `
  --startup-wait-sec 90 `
  --metrics-timeout-sec 1200 `
  --replay-retries 8 `
  --replay-retry-wait-sec 5 `
  --summary-csv artifacts/streaming/evaluation/layer_c_summary_700k_fault_rerun03.csv

Write-Host "Completed Layer A, Layer B, Layer C rerun 2 and rerun 3."
