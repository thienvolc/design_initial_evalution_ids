$ErrorActionPreference = "Stop"

$PY = ".\.venv\Scripts\python.exe"
$PROF = "experiments/streaming/profiles/local_profiles.yaml"

Write-Host "[1/6] smoke_gate"
# & $PY scripts/streaming/run_online_profile.py --profile-config $PROF --profile smoke_gate
& $PY scripts/streaming/run_online_profile.py --profile-config $PROF --profile smoke_gate --gate-only

Write-Host "[2/6] layer_a_local_light"
# & $PY scripts/streaming/run_online_profile.py --profile-config $PROF --profile layer_a_local_light
& $PY scripts/streaming/run_online_profile.py --profile-config $PROF --profile layer_a_local_light --gate-only
Write-Host "[3/6] layer_b_light"
& $PY scripts/streaming/run_online_profile.py --profile-config $PROF --profile layer_b_light
# & $PY scripts/streaming/run_online_profile.py --profile-config $PROF --profile layer_b_light --gate-only

Write-Host "[4/6] watermark_local_light"
& $PY scripts/streaming/run_online_profile.py --profile-config $PROF --profile watermark_local_light
# & $PY scripts/streaming/run_online_profile.py --profile-config $PROF --profile watermark_local_light --gate-only

Write-Host "[5/6] layer_c_local_light"
& $PY scripts/streaming/run_online_profile.py --profile-config $PROF --profile layer_c_local_light
# & $PY scripts/streaming/run_online_profile.py --profile-config $PROF --profile layer_c_local_light --gate-only

Write-Host "[6/6] stress_local_light"
& $PY scripts/streaming/run_online_profile.py --profile-config $PROF --profile stress_local_light
# & $PY scripts/streaming/run_online_profile.py --profile-config $PROF --profile stress_local_light --gate-only

Write-Host "Daily light regression complete."


