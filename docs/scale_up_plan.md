# Scale-Up Plan

## Goal
- Align execution with larger volume targets discussed earlier.
- Keep current final artifacts unchanged.
- Write all scale-up outputs to artifacts/streaming/scale_up.

## Data Source (what is actually loaded)
- Default replay source in current config: data/gold/splits/test.parquet
  - Defined in configs/streaming/online.yaml -> paths.input_parquet
- For scale-up and stress-full in this plan: data/gold/splits/test.parquet
  - Use configs/streaming/online_scale_up.yaml
  - Test split size from split_metadata.yaml: 1,374,143 rows (label 0: 998,793; label 1: 375,350)
  - Replay script now loops over the same test parquet automatically when max_rows exceeds source size
  - Stress full target in this revision: full test split looped x4 = 5,496,572 rows

## Current Baseline (already executed)
- Layer A total rows: 12,000.
- Layer B total rows: 9,000.
- Watermark total rows: 4,000.
- Failure test: time-based, 12 scenarios, recovery observed.
- Stress 5M-20M: not executed.

## Target Matrix
| Phase | Practical target in this plan | How total is computed |
|---|---:|---|
| Smoke | 50,000 | Separate gate run (quick validation) |
| Layer A (system) | 500,000 | 3 profiles x 166,667 (approx 500k) |
| Layer B (model) | 500,000 | 4 curated combos x 125,000 |
| Layer C (fault) | 700,000 | warmup 350,000 + post-fault 350,000 per scenario |
| Watermark | 500,000 + variants | 4 delays x 125,000 + reordered/lateness run |
| Stress main | Full test x 4 loops (5,496,572) | Trace-driven step-rate, summed max_rows |
| Stress secondary | Bursty | Base and burst steps on same trace corpus |

## Pre-flight
```powershell
$PY = ".\\.venv\\Scripts\\python.exe"
$CFG = "configs/streaming/online_scale_up.yaml"
$TRACE_FILE = "data/gold/splits/test.parquet"
$AB_RATE_PROFILE = "5000:180,10000:120"
$SMOKE_WARMUP_SCHEDULE = "1000:15"
$AB_WARMUP_ROWS = 50000
$AB_WARMUP_SCHEDULE = "1000:50"
$WATERMARK_WARMUP_ROWS = 50000
$WATERMARK_WARMUP_SCHEDULE = "1000:50"
$STRESS_WARMUP_ROWS = 300000
$STRESS_WARMUP_SCHEDULE = "5000:60"
New-Item -ItemType Directory -Force artifacts/streaming/scale_up | Out-Null

docker compose ps
docker compose exec -T ids-dev pkill -f run_structured_streaming.py ; true
```

## Profile-Based Execution (recommended)
Manage phase arguments in one place:
- Local profile config file (current machine): experiments/streaming/profiles/local_profiles.yaml
- Real-test profile config file: experiments/streaming/profiles/online_profiles.yaml
- Launcher: scripts/streaming/run_online_profile.py

```powershell
$PROFILE_CFG_LOCAL = "experiments/streaming/profiles/local_profiles.yaml"
$PROFILE_CFG_ONLINE = "experiments/streaming/profiles/online_profiles.yaml"

# list available profiles
$PY scripts/streaming/run_online_profile.py --profile-config $PROFILE_CFG_LOCAL --list
$PY scripts/streaming/run_online_profile.py --profile-config $PROFILE_CFG_LOCAL --list --list-light-only
$PY scripts/streaming/run_online_profile.py --profile-config $PROFILE_CFG_ONLINE --list --list-heavy-only

# inspect resolved command without running
$PY scripts/streaming/run_online_profile.py --profile-config $PROFILE_CFG_ONLINE --profile layer_a_500k --dry-run

# skip execution, only evaluate pass/fail from existing summary_csv
$PY scripts/streaming/run_online_profile.py --profile-config $PROFILE_CFG_LOCAL --profile smoke_gate --gate-only

# run phase by profile name
# Official report run: 1 Layer C profile + 1 Stress profile
$PY scripts/streaming/run_online_profile.py --profile-config $PROFILE_CFG_ONLINE --profile smoke_gate
$PY scripts/streaming/run_online_profile.py --profile-config $PROFILE_CFG_ONLINE --profile layer_b_light
$PY scripts/streaming/run_online_profile.py --profile-config $PROFILE_CFG_ONLINE --profile layer_a_500k --allow-heavy
$PY scripts/streaming/run_online_profile.py --profile-config $PROFILE_CFG_ONLINE --profile layer_b_500k --allow-heavy
$PY scripts/streaming/run_online_profile.py --profile-config $PROFILE_CFG_ONLINE --profile watermark_500k --allow-heavy
$PY scripts/streaming/run_online_profile.py --profile-config $PROFILE_CFG_ONLINE --profile layer_c_700k_fault --allow-heavy
$PY scripts/streaming/run_online_profile.py --profile-config $PROFILE_CFG_ONLINE --profile stress_main_testx4 --allow-heavy

# Optional ablation/sensitivity runs (not required for official report)
$PY scripts/streaming/run_online_profile.py --profile-config $PROFILE_CFG_ONLINE --profile watermark_late_injection_ablation --allow-heavy
$PY scripts/streaming/run_online_profile.py --profile-config $PROFILE_CFG_ONLINE --profile stress_bursty_testx4 --allow-heavy

# Local-light sequence (for weak machines, no 500k runs)
$PY scripts/streaming/run_online_profile.py --profile-config $PROFILE_CFG_LOCAL --profile smoke_gate
$PY scripts/streaming/run_online_profile.py --profile-config $PROFILE_CFG_LOCAL --profile layer_a_local_light
$PY scripts/streaming/run_online_profile.py --profile-config $PROFILE_CFG_LOCAL --profile layer_b_light
$PY scripts/streaming/run_online_profile.py --profile-config $PROFILE_CFG_LOCAL --profile watermark_local_light
$PY scripts/streaming/run_online_profile.py --profile-config $PROFILE_CFG_LOCAL --profile layer_c_local_light
$PY scripts/streaming/run_online_profile.py --profile-config $PROFILE_CFG_LOCAL --profile stress_local_light
```

Note:
- runtime=docker profiles run with docker compose exec ids-dev python ...
- runtime=host profiles run on host Python (needed for Layer C fault scripts)
- Heavy profiles are blocked by default; use --allow-heavy to run them.
- pass/fail gates from profile meta.pass_fail are evaluated after each successful run.
- Use --gate-only to evaluate pass/fail from existing artifacts without running workloads.
- Use --skip-gates only for local debugging when gate data is not yet stable.
- Recommended on low-resource machines: run only profiles with resource_class=light.
- Policy: only smoke_gate is enforced (required=true); benchmark profiles are advisory (required=false).

Loop behavior check:
- If max_rows <= 1,374,143: replay consumes one pass of test split.
- If max_rows > 1,374,143: replay continues with additional passes until max_rows is reached.

Fairness rule for Layer A and Layer B:
- Same source trace: $TRACE_FILE
- Same replay order key: event_time
- Same replay rate profile: $AB_RATE_PROFILE
- Same total rows: 500,000

Warmup policy in this revision:
- Smoke: 10-20 seconds (use schedule, no metric on warmup phase)
- Layer A/B: 30-60 seconds or 50k rows (set to 50k with low warmup schedule)
- Layer C: 30-60 seconds before fault (already controlled by warmup replay profile)
- Stress: 30-60 seconds at low rate before measurement phase
- Watermark: 50k-100k rows warmup before measured replay

## Stress Replay Method
- Trace-driven replay corpus: test/unseen split (data/gold/splits/test.parquet).
- Keep event order from trace and loop corpus when target rows exceed one pass.
- Main method for stress: step-rate replay (selected).
- Alternative method (optional): time-scaled replay.

Step-rate levels for main stress:
- 5k rows/s
- 10k rows/s
- 20k rows/s

Chosen step lengths (rows-based equivalent):
- step_5k: 900,000 rows (180s)
- step_10k: 1,800,000 rows (180s)
- step_20k: 2,796,572 rows (139.8286s)
- Total: 5,496,572 rows

## Phase 2: Streamlit MVP Dashboard
Goal:
- Read summary CSV/JSON artifacts without manual file opening.
- Filter by layer/profile/mode.
- Compare runs in <= 3 actions.

Scope boundary:
- Streamlit is a results portal (post-run analysis), not a real-time operations console.
- Keep it focused on:
  - run table
  - compare charts (p95 latency / throughput / F1 / FPR)
  - filters by layer/profile/model

Run locally:
```powershell
$PY -m streamlit run apps/streaming_dashboard.py
```

Expected behavior:
- Data source: artifacts/streaming/**/*summary*.csv and artifacts/streaming/**/*summary*.json.
- Profile metadata mapping from both experiment profile files:
  - experiments/streaming/profiles/local_profiles.yaml
  - experiments/streaming/profiles/online_profiles.yaml
- Tabs:
  - Data Explorer
  - Compare Runs
  - Quality Snapshot

## Phase 3: Local Runtime Observability (Prometheus/Grafana)
Goal:
- Expose runtime-like summary signals in Prometheus format.
- Use Grafana for a baseline operational dashboard.

Scope boundary:
- Grafana/Prometheus is for runtime observability while workloads are running.
- Streamlit and Grafana are complementary, not replacements for each other.

Exporter (local):
```powershell
$PY scripts/streaming/export_prometheus_summary.py --port 9108 --refresh-sec 15
```

Quick verify exporter without long-running process:
```powershell
$PY scripts/streaming/export_prometheus_summary.py --once
```

Prometheus config:
- ops/observability/prometheus.yml

Grafana dashboard template:
- ops/observability/grafana_streaming_dashboard.json

Suggested local stack:
1. Run exporter script locally.
2. Run Prometheus with ops/observability/prometheus.yml.
3. Import grafana_streaming_dashboard.json into Grafana (Prometheus datasource).

One-command Docker stack (Prometheus + Grafana):
```powershell
docker compose -f ops/observability/docker-compose.observability.yaml up -d
```

Access:
- Prometheus: http://127.0.0.1:9090
- Grafana: http://127.0.0.1:3000 (admin/admin)

Provisioning in this repo:
- Grafana datasource: ops/observability/grafana/provisioning/datasources/datasource.yaml
- Grafana dashboard provider: ops/observability/grafana/provisioning/dashboards/dashboards.yaml
- Dashboard JSON: ops/observability/grafana/dashboards/ids_streaming_dashboard.json

Stop stack:
```powershell
docker compose -f ops/observability/docker-compose.observability.yaml down
```

## Phase 4: Hardening & Reproducibility (Local)
Delivered baseline assets:
- Runbook: docs/06_phase4_runbook.md
- Daily regression script: scripts/runbooks/daily_light_regression.ps1

Run daily light regression:
```powershell
powershell -ExecutionPolicy Bypass -File scripts/runbooks/daily_light_regression.ps1
```

## Phase 0: Smoke Gate (50k)
Local gate lock on current machine:
- Required pass criteria for profile `smoke_gate`: rows_total >= 3000, fnr <= 0.40, fpr <= 0.20.

```powershell
docker compose exec ids-dev python scripts/streaming/run_layer_b_matrix.py `
  --config $CFG `
  --warmup-rows 15000 `
  --warmup-rate-schedule $SMOKE_WARMUP_SCHEDULE `
  --models logistic_regression random_forest `
  --feature-sets full `
  --repeats 1 `
  --max-rows 25000 `
  --batch-size 2000 `
  --metrics-timeout-sec 300 `
  --run-prefix smoke50k `
  --summary-csv artifacts/streaming/scale_up/layer_b_summary_smoke_50k.csv
```

## Phase 1: Layer A (500K)
```powershell
docker compose exec ids-dev python scripts/streaming/run_layer_a_matrix.py `
  --config $CFG `
  --model logistic_regression `
  --feature-set full `
  --trace-input-parquet $TRACE_FILE `
  --trace-order-column event_time `
  --trace-rate-schedule $AB_RATE_PROFILE `
  --trace-stream-run-seconds 420 `
  --warmup-rows $AB_WARMUP_ROWS `
  --warmup-rate-schedule $AB_WARMUP_SCHEDULE `
  --profiles "A_low:500:4:10 seconds" "A_mid:2000:8:10 seconds" "A_high:8000:16:10 seconds" `
  --repeats 1 `
  --max-rows 166667 `
  --batch-size 5000 `
  --metrics-timeout-sec 1500 `
  --run-prefix layerA_scaleup `
  --summary-csv artifacts/streaming/scale_up/layer_a_summary_500k.csv
```

## Phase 2: Layer B (500K, fairness with Layer A, curated minimal combos)
```powershell
docker compose exec ids-dev python scripts/streaming/run_layer_b_matrix.py `
  --config $CFG `
  --trace-input-parquet $TRACE_FILE `
  --trace-order-column event_time `
  --trace-rate-schedule $AB_RATE_PROFILE `
  --trace-stream-run-seconds 420 `
  --warmup-rows $AB_WARMUP_ROWS `
  --warmup-rate-schedule $AB_WARMUP_SCHEDULE `
  --model-feature-pairs logistic_regression:reduced random_forest:reduced random_forest:full gradient_boosting:reduced `
  --repeats 1 `
  --max-rows 125000 `
  --batch-size 5000 `
  --metrics-timeout-sec 1500 `
  --run-prefix layerB_scaleup `
  --summary-csv artifacts/streaming/scale_up/layer_b_summary_500k.csv
```

## Phase 3: Watermark (500K baseline)
```powershell
docker compose exec ids-dev python scripts/streaming/run_watermark_matrix.py `
  --config $CFG `
  --model logistic_regression `
  --feature-set full `
  --trace-input-parquet $TRACE_FILE `
  --trace-order-column event_time `
  --trace-rate-schedule $AB_RATE_PROFILE `
  --warmup-rows $WATERMARK_WARMUP_ROWS `
  --warmup-rate-schedule $WATERMARK_WARMUP_SCHEDULE `
  --watermark-delays 0 10 30 60 `
  --drop-late-events `
  --max-rows 125000 `
  --batch-size 5000 `
  --metrics-timeout-sec 1800 `
  --summary-csv artifacts/streaming/scale_up/watermark_summary_500k.csv
```

## Phase 3B: Watermark reordered/lateness variant
```powershell
docker compose exec ids-dev python scripts/streaming/run_watermark_matrix.py `
  --config $CFG `
  --model logistic_regression `
  --feature-set full `
  --trace-input-parquet $TRACE_FILE `
  --trace-order-column event_time `
  --trace-rate-schedule $AB_RATE_PROFILE `
  --warmup-rows $WATERMARK_WARMUP_ROWS `
  --warmup-rate-schedule $WATERMARK_WARMUP_SCHEDULE `
  --reorder-window-size 500 `
  --late-event-ratio 0.15 `
  --late-event-max-sec 8 `
  --watermark-delays 0 10 30 60 `
  --drop-late-events `
  --max-rows 125000 `
  --batch-size 5000 `
  --metrics-timeout-sec 1800 `
  --summary-csv artifacts/streaming/scale_up/watermark_summary_500k_reordered_late.csv
```

## Phase 4: Layer C Fault (700K, 3 loops, inject in loop2 middle window)
```powershell
& $PY scripts/streaming/run_layer_c_matrix.py `
  --config $CFG `
  --model logistic_regression `
  --feature-set full `
  --trace-input-parquet $TRACE_FILE `
  --trace-order-column event_time `
  --scenarios kafka_restart spark_process_restart producer_restart network_slowdown `
  --warmup-rows 350000 `
  --warmup-rate-schedule "5000:45,10000:30" `
  --fault-delay-sec 45 `
  --post-fault-rows 350000 `
  --post-fault-rows-per-sec 10000 `
  --batch-size 5000 `
  --stream-run-seconds 1200 `
  --startup-wait-sec 10 `
  --metrics-timeout-sec 1200 `
  --replay-retries 8 `
  --replay-retry-wait-sec 5 `
  --summary-csv artifacts/streaming/scale_up/layer_c_summary_700k_fault.csv
```

To enforce 3 loops reliability, repeat scenario cycle 3 times:
```powershell
& $PY scripts/streaming/run_layer_c_matrix.py `
  --config $CFG `
  --model logistic_regression `
  --feature-set full `
  --trace-input-parquet $TRACE_FILE `
  --trace-order-column event_time `
  --scenarios kafka_restart spark_process_restart producer_restart network_slowdown kafka_restart spark_process_restart producer_restart network_slowdown kafka_restart spark_process_restart producer_restart network_slowdown `
  --warmup-rows 350000 `
  --warmup-rate-schedule "5000:45,10000:30" `
  --fault-delay-sec 45 `
  --post-fault-rows 350000 `
  --post-fault-rows-per-sec 10000 `
  --batch-size 5000 `
  --stream-run-seconds 1200 `
  --startup-wait-sec 10 `
  --metrics-timeout-sec 1200 `
  --replay-retries 8 `
  --replay-retry-wait-sec 5 `
  --summary-csv artifacts/streaming/scale_up/layer_c_summary_700k_fault_3loops.csv
```

## Phase 5: Stress Main (Trace-driven Step-rate, Full test x4 = 5,496,572)
```powershell
docker compose exec ids-dev python scripts/streaming/run_load_quality_matrix.py `
  --config $CFG `
  --model logistic_regression `
  --feature-set full `
  --trace-profile-name trace_step_rate_main `
  --warmup-rows $STRESS_WARMUP_ROWS `
  --warmup-rate-schedule $STRESS_WARMUP_SCHEDULE `
  --trace-rate-schedule "5000:180,10000:180,20000:139.8286" `
  --trace-max-rows 5496572 `
  --batch-size 10000 `
  --metrics-timeout-sec 3600 `
  --summary-csv artifacts/streaming/scale_up/load_quality_summary_stress_full_testx4.csv
```

Expected replay loops in Phase 5:
- Total loops over test split: exactly 4 passes.

## Phase 6: Stress Secondary (Bursty Trace Replay)
```powershell
docker compose exec ids-dev python scripts/streaming/run_load_quality_matrix.py `
  --config $CFG `
  --model logistic_regression `
  --feature-set full `
  --trace-profile-name trace_bursty_secondary `
  --warmup-rows $STRESS_WARMUP_ROWS `
  --warmup-rate-schedule $STRESS_WARMUP_SCHEDULE `
  --trace-rate-schedule "10000:120,40000:20,10000:120,50000:20,10000:120" `
  --trace-max-rows 5496572 `
  --batch-size 10000 `
  --metrics-timeout-sec 3600 `
  --summary-csv artifacts/streaming/scale_up/load_quality_summary_bursty_trace.csv
```

Bursty metrics focus:
- lag peak
- time-to-drain
- p95/p99 end-to-end latency

## Phase 7: Fault Stress Near Sustainable Threshold
```powershell
& $PY scripts/streaming/run_layer_c_matrix.py `
  --config $CFG `
  --model logistic_regression `
  --feature-set full `
  --trace-input-parquet $TRACE_FILE `
  --trace-order-column event_time `
  --scenarios producer_restart spark_process_restart kafka_restart network_slowdown `
  --warmup-rows 80000 `
  --post-fault-rows 80000 `
  --batch-size 10000 `
  --slowdown-rows-per-sec 5000 `
  --producer-restart-pause-sec 5 `
  --stream-run-seconds 900 `
  --startup-wait-sec 12 `
  --metrics-timeout-sec 900 `
  --replay-retries 10 `
  --replay-retry-wait-sec 5 `
  --summary-csv artifacts/streaming/scale_up/layer_c_summary_fault_stress.csv
```

## Verification Commands
```powershell
(Import-Csv artifacts/streaming/scale_up/layer_a_summary_500k.csv | Measure-Object rows_total -Sum).Sum
(Import-Csv artifacts/streaming/scale_up/layer_b_summary_500k.csv | Measure-Object rows -Sum).Sum
(Import-Csv artifacts/streaming/scale_up/watermark_summary_500k.csv | Measure-Object rows -Sum).Sum
(Import-Csv artifacts/streaming/scale_up/watermark_summary_500k_reordered_late.csv | Measure-Object rows -Sum).Sum
(Import-Csv artifacts/streaming/scale_up/load_quality_summary_stress_full_testx4.csv | Measure-Object rows -Sum).Sum
(Import-Csv artifacts/streaming/scale_up/load_quality_summary_bursty_trace.csv | Measure-Object rows -Sum).Sum
(Import-Csv artifacts/streaming/scale_up/layer_c_summary_700k_fault.csv | Measure-Object rows_after_fault -Sum).Sum
(Import-Csv artifacts/streaming/scale_up/layer_c_summary_700k_fault_3loops.csv | Measure-Object rows_after_fault -Sum).Sum
(Import-Csv artifacts/streaming/scale_up/layer_c_summary_fault_stress.csv | Measure-Object rows_after_fault -Sum).Sum
```

## Consolidated Scale-Up Report
```powershell
$PY scripts/streaming/build_online_report.py `
  --layer-a artifacts/streaming/scale_up/layer_a_summary_500k.csv `
  --layer-b artifacts/streaming/scale_up/layer_b_summary_500k.csv `
  --layer-c artifacts/streaming/scale_up/layer_c_summary_700k_fault_3loops.csv `
  --watermark-summary artifacts/streaming/scale_up/watermark_summary_500k.csv `
  --load-quality-summary artifacts/streaming/scale_up/load_quality_summary_stress_full_testx4.csv `
  --out-md artifacts/streaming/scale_up/online_evaluation_report_scale_up.md `
  --out-json artifacts/streaming/scale_up/online_evaluation_report_scale_up.json
```

## Resume Rule
- If a phase fails, rerun only that phase with the same output file path.
- Do not rerun completed phases unless you intentionally replace their CSV.
- Always stop stale stream processes before rerun:
```powershell
docker compose exec -T ids-dev pkill -f run_structured_streaming.py ; true
```



> Legacy note: this file records earlier scale-up planning assumptions.
> Use [architecture.md](architecture.md), [execution_runbook.md](execution_runbook.md), and the current profile YAML files for active project behavior.
