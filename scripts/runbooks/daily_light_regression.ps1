$ErrorActionPreference = "Stop"

Write-Host "Streaming local smoke runbook"
Write-Host "Runs capacity calibration smoke plus the remaining config-driven smoke gates. These are health checks, not paper benchmark evidence."

docker compose up -d zookeeper kafka ids-dev

$scripts = @(
  "scripts/streaming/official/run_capacity_calibration.py",
  "scripts/streaming/official/run_model_feature_tradeoff.py",
  "scripts/streaming/official/run_watermark_matrix.py",
  "scripts/streaming/official/run_layer_c_matrix.py",
  "scripts/streaming/official/run_load_quality_matrix.py"
)

foreach ($script in $scripts) {
  Write-Host "Running $script"
  docker compose exec -T ids-dev python $script
}
