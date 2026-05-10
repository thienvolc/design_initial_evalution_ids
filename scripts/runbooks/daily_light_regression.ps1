$ErrorActionPreference = "Stop"

$PY = ".\.venv\Scripts\python.exe"
$PROF = "experiments/streaming/profiles/local_profiles.yaml"

Write-Host "Streaming local runbook"
Write-Host "Uncomment the commands you want, then run this file."

# 1. List available local profiles
# & $PY scripts/streaming/official/run_streaming_profile.py --profile-config $PROF --list

# 2. Smoke gate before anything longer
# & $PY scripts/streaming/official/run_streaming_profile.py --profile-config $PROF --profile smoke_gate --gate-only

# 3. Local main path: Layer A
# & $PY scripts/streaming/official/run_streaming_profile.py --profile-config $PROF --profile layer_a_local_medium

# 4. Local main path: Layer B
# & $PY scripts/streaming/official/run_streaming_profile.py --profile-config $PROF --profile layer_b_local_medium

# 5. Local main path: Watermark
# & $PY scripts/streaming/official/run_streaming_profile.py --profile-config $PROF --profile watermark_local_medium

# 6. Local main path: Layer C
# & $PY scripts/streaming/official/run_streaming_profile.py --profile-config $PROF --profile layer_c_local_medium

# 7. Local main path: Load quality
# & $PY scripts/streaming/official/run_streaming_profile.py --profile-config $PROF --profile stress_local_medium

# Optional local reruns / longer checks
# & $PY scripts/streaming/official/run_streaming_profile.py --profile-config $PROF --profile layer_b_local_extended --allow-heavy
# & $PY scripts/streaming/official/run_streaming_profile.py --profile-config $PROF --profile stress_local_extended --allow-heavy
