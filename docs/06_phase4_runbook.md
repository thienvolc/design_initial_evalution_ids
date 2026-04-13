> Legacy note: this runbook is outdated and still contains host-first examples and older profile names.
> Use [execution_runbook.md](execution_runbook.md) and [../scripts/streaming/README.md](../scripts/streaming/README.md) for the current Docker-first workflow.

# Phase 4 Runbook (Local-First)

## Purpose
- Make reruns reproducible with minimal manual steps.
- Separate local-light workflow and online-heavy workflow.

## Profiles
- Local default config: `experiments/streaming/profiles/local_profiles.yaml`
- Online real-test config: `experiments/streaming/profiles/online_profiles.yaml`

## Daily Light Regression
Run these profiles (no heavy run):
```powershell
$PY = ".\.venv\Scripts\python.exe"
$PROF = "experiments/streaming/profiles/local_profiles.yaml"

$PY scripts/streaming/run_online_profile.py --profile-config $PROF --profile smoke_gate
$PY scripts/streaming/run_online_profile.py --profile-config $PROF --profile layer_a_local_light --gate-only
$PY scripts/streaming/run_online_profile.py --profile-config $PROF --profile layer_b_light --gate-only
$PY scripts/streaming/run_online_profile.py --profile-config $PROF --profile watermark_local_light --gate-only
$PY scripts/streaming/run_online_profile.py --profile-config $PROF --profile layer_c_local_light --gate-only
$PY scripts/streaming/run_online_profile.py --profile-config $PROF --profile stress_local_light --gate-only
```

## Online Heavy Campaign (When resources are available)
```powershell
$PY = ".\.venv\Scripts\python.exe"
$PROF = "experiments/streaming/profiles/online_profiles.yaml"

$PY scripts/streaming/run_online_profile.py --profile-config $PROF --profile smoke_gate
$PY scripts/streaming/run_online_profile.py --profile-config $PROF --profile layer_a_500k --allow-heavy
$PY scripts/streaming/run_online_profile.py --profile-config $PROF --profile layer_b_500k --allow-heavy
$PY scripts/streaming/run_online_profile.py --profile-config $PROF --profile watermark_500k --allow-heavy
$PY scripts/streaming/run_online_profile.py --profile-config $PROF --profile layer_c_700k_fault --allow-heavy
$PY scripts/streaming/run_online_profile.py --profile-config $PROF --profile stress_main_testx4 --allow-heavy
```

## Observability Startup
```powershell
$PY = ".\.venv\Scripts\python.exe"
$PY scripts/streaming/export_prometheus_summary.py --port 9108 --refresh-sec 15
```

In separate terminal:
```powershell
docker compose -f ops/observability/docker-compose.observability.yaml up -d
```

## Shutdown
```powershell
docker compose -f ops/observability/docker-compose.observability.yaml down
```



