$ErrorActionPreference = "Stop"

$PY = ".\.venv\Scripts\python.exe"
$timeseriesDir = "artifacts/streaming/metrics_timeseries"

Write-Host "Streaming report runbook"
Write-Host "Uncomment the command set you want, then run this file."
Write-Host "Recommended final flow: build benchmark report -> optional plots."

# Local report build
# & $PY scripts/streaming/official/build_streaming_report.py `
#   --layer-a artifacts/streaming/evaluation/layer_a_summary_local_medium.csv `
#   --layer-b artifacts/streaming/evaluation/layer_b_summary_local_medium.csv `
#   --layer-c artifacts/streaming/evaluation/layer_c_summary_local_medium.csv `
#   --watermark-summary artifacts/streaming/evaluation/watermark_summary_local_medium.csv `
#   --load-quality-summary artifacts/streaming/evaluation/load_quality_summary_local_medium.csv `
#   --out-md artifacts/streaming/evaluation/streaming_evaluation_report_local.md `
#   --out-json artifacts/streaming/evaluation/streaming_evaluation_report_local.json

# Benchmark / final report build
# & $PY scripts/streaming/official/build_streaming_report.py `
#   --layer-a artifacts/streaming/evaluation/layer_a_summary_500k.csv `
#   --layer-b artifacts/streaming/evaluation/layer_b_summary_500k.csv `
#   --layer-c artifacts/streaming/evaluation/layer_c_summary_700k_fault.csv `
#   --watermark-summary artifacts/streaming/evaluation/watermark_summary_500k.csv `
#   --load-quality-summary artifacts/streaming/evaluation/load_quality_summary_stress_full_testx4.csv `
#   --out-md artifacts/streaming/evaluation/streaming_evaluation_report_final.md `
#   --out-json artifacts/streaming/evaluation/streaming_evaluation_report_final.json

# Optional timeseries plots
# & $PY scripts/streaming/official/build_timeseries_plots.py `
#   --inputs $timeseriesDir `
#   --output-dir artifacts/streaming/plots/final

