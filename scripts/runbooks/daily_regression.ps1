$ErrorActionPreference = "Stop"

$PY = ".\.venv\Scripts\python.exe"
$PROF = "experiments/streaming/profiles/streaming_profiles.yaml"
$timeseriesDir = "artifacts/streaming/metrics_timeseries"

Write-Host "Streaming full-pipeline runbook"
Write-Host "Uncomment exactly one block, then run this file."
Write-Host "Primary benchmark path now targets random_forest:full in the main profiles."
Write-Host "Recommended official order: smoke -> layer_a_500k -> layer_b_500k -> watermark_500k -> layer_c_700k_fault -> stress_main_testx4 -> build final report"

# 1. List available benchmark profiles
# & $PY scripts/streaming/official/run_streaming_profile.py --profile-config $PROF --list

# 2. Smoke gate before any heavy run
# & $PY scripts/streaming/official/run_streaming_profile.py --profile-config $PROF --profile smoke_gate --gate-only

# 3. Full benchmark path
# & $PY scripts/streaming/official/run_streaming_profile.py --profile-config $PROF --profile layer_a_500k --allow-heavy
# & $PY scripts/streaming/official/run_streaming_profile.py --profile-config $PROF --profile layer_b_500k --allow-heavy
# & $PY scripts/streaming/official/run_streaming_profile.py --profile-config $PROF --profile watermark_500k --allow-heavy
& $PY scripts/streaming/official/run_streaming_profile.py --profile-config $PROF --profile layer_c_700k_fault --allow-heavy
# & $PY scripts/streaming/official/run_streaming_profile.py --profile-config $PROF --profile stress_main_testx4 --allow-heavy

# 4. Build final report after the benchmark path completes
# & $PY scripts/streaming/official/build_streaming_report.py `
#   --layer-a artifacts/streaming/evaluation/layer_a_summary_500k.csv `
#   --layer-b artifacts/streaming/evaluation/layer_b_summary_500k.csv `
#   --layer-c artifacts/streaming/evaluation/layer_c_summary_700k_fault.csv `
#   --watermark-summary artifacts/streaming/evaluation/watermark_summary_500k.csv `
#   --load-quality-summary artifacts/streaming/evaluation/load_quality_summary_stress_full_testx4.csv `
#   --out-md artifacts/streaming/evaluation/streaming_evaluation_report_final.md `
#   --out-json artifacts/streaming/evaluation/streaming_evaluation_report_final.json

# 5. Optional timeseries plots after report build
# & $PY scripts/streaming/official/build_timeseries_plots.py `
#   --inputs $timeseriesDir `
#   --output-dir artifacts/streaming/plots/final

# Optional lighter / debug benchmark path
# & $PY scripts/streaming/official/run_streaming_profile.py --profile-config $PROF --profile layer_b_light --allow-heavy

# Optional ablations
# & $PY scripts/streaming/official/run_streaming_profile.py --profile-config $PROF --profile watermark_late_injection_ablation --allow-heavy
# & $PY scripts/streaming/official/run_streaming_profile.py --profile-config $PROF --profile stress_bursty_testx4 --allow-heavy
