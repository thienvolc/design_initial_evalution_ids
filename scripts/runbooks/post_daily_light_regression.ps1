$ErrorActionPreference = "Stop"

param(
    [switch]$OpenDashboard
)

$PY = ".\.venv\Scripts\python.exe"

$layerA = "artifacts/streaming/scale_up/layer_a_summary_local_medium.csv"
$layerB = "artifacts/streaming/scale_up/layer_b_summary_local_medium.csv"
$layerC = "artifacts/streaming/scale_up/layer_c_summary_local_medium.csv"
$watermark = "artifacts/streaming/scale_up/watermark_summary_local_medium.csv"
$loadQuality = "artifacts/streaming/scale_up/load_quality_summary_local_medium.csv"

$reportMd = "artifacts/streaming/scale_up/online_evaluation_report_daily_light.md"
$reportJson = "artifacts/streaming/scale_up/online_evaluation_report_daily_light.json"
$plotsDir = "artifacts/streaming/plots/daily_light"
$timeseriesDir = "artifacts/streaming/metrics_timeseries"

Write-Host "[1/3] Build consolidated report"
& $PY scripts/streaming/build_online_report.py `
  --layer-a $layerA `
  --layer-b $layerB `
  --layer-c $layerC `
  --watermark-summary $watermark `
  --load-quality-summary $loadQuality `
  --out-md $reportMd `
  --out-json $reportJson

Write-Host "[2/3] Build timeseries plots"
& $PY scripts/streaming/build_timeseries_plots.py `
  --inputs $timeseriesDir `
  --output-dir $plotsDir

Write-Host "[3/3] Summary"
Write-Host "Report markdown: $reportMd"
Write-Host "Report json: $reportJson"
Write-Host "Plots dir: $plotsDir"

if ($OpenDashboard) {
    Write-Host "Opening Streamlit dashboard..."
    & $PY -m streamlit run apps/streaming_dashboard.py
}
