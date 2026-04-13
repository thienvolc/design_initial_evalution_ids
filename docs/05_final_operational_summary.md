> Legacy note: this file captures earlier conclusions and scale notes.
> For the current SUT vs evaluation boundary and official execution flow, use [evaluation_methodology.md](evaluation_methodology.md), [architecture.md](architecture.md), and [execution_runbook.md](execution_runbook.md).

# Final Methodology, Limitations, and Operational Conclusions (Option A)

## 1) Final Methodology (Executed)

### Scope and non-goals
- Benchmark scope is binary online detection only (Benign vs Attack).
- Structured Streaming (micro-batch) is the only online engine in this final benchmark.
- Online multiclass benchmark is out of scope for this submission.

### Final experiment matrix and run counts
- Layer A (system knobs): 3 profiles x 5 repeats = 15 runs.
- Layer B (model x feature set): 3 models x 2 feature sets x 5 repeats = 30 runs.
- Layer C (fault/recovery): 4 scenarios x 3 repeats = 12 runs.
- Watermark matrix: delays {0, 10, 30, 60} = 4 runs.
- Load-quality matrix: profiles {low, medium, high} = 3 runs.

### Actual executed scale (rows)

| Phase | Planned scale (discussion target) | Executed scale (this repo) | Status |
|---|---:|---:|---|
| Smoke (all smoke artifacts combined) | 10k-50k | ~10,000 rows | Meets lower bound |
| Layer A (system) | 500k-1M | 12,000 rows | Below target |
| Layer B (model) | 500k-2M | 9,000 rows | Below target |
| Watermark test | 1M-3M | 4,000 rows | Below target |
| Failure test | time-based | 12 scenarios, recovery 7.996-30.391 s | Time-based validated |
| Stress | 5M-20M | Not executed | Missing |

Scale note:
- Current final results are valid for initial operational evaluation at small-to-medium replay volume, not for multi-million row stress claims.

### Final artifact set
- artifacts/streaming/online/layer_a_summary_final.csv
- artifacts/streaming/online/layer_b_summary_final.csv
- artifacts/streaming/online/layer_c_summary_final.csv
- artifacts/streaming/online/watermark_summary_final.csv
- artifacts/streaming/online/load_quality_summary_final.csv
- artifacts/streaming/online/online_evaluation_report_final.md
- artifacts/streaming/online/online_evaluation_report_final.json

## 2) Key Final Results

### Layer A: system profile selection
- Best profile by throughput-latency score: A_high.
- A_high median throughput: 224.21 rows/s.
- A_high p95 end-to-end latency (p95 across repeats): 35445 ms.

### Layer B: model-feature operating point
- Fastest combo by p95 latency: random_forest + full.
- p95 end-to-end latency (p95 across repeats): 28583 ms.

### Layer C: resilience baseline
- Fault scenarios validated: kafka_restart, spark_process_restart, producer_restart, network_slowdown.
- Worst observed recovery time: 30.39 s (spark_process_restart).
- Median recovery by scenario:
  - kafka_restart: 17.17 s
  - spark_process_restart: 25.84 s
  - producer_restart: 18.32 s
  - network_slowdown: 9.43 s

### Watermark experiment
- delay=0s caused high late_event_ratio (0.718).
- delay >= 10s reduced late_event_ratio to 0.0 in this setup.
- Operational recommendation: use 10s watermark as baseline; tune upward only when needed.

### Detection under load
- low profile: target 150 rps, actual 283.86 rps, FPR 0.011, FNR 0.0.
- medium profile: target 400 rps, actual 523.35 rps, FPR 0.0093, FNR 1.0.
- high profile: target 800 rps, actual 743.21 rps, FPR 0.0097, FNR 1.0.

Interpretation:
- System throughput remains stable and Kafka lag stays near zero in these runs.
- Detection quality is not stable at medium/high load in this sampled setting (FNR spike).
- Therefore, deployment recommendation must prioritize quality gates, not only latency/throughput.
- Large-scale conclusions (>= 500k rows per phase, multi-million stress) are out of scope of current executed artifacts.

## 3) Limitations (Final)

- Single-broker topology: no inter-broker replication/failover benchmark in this submission.
- Label distribution per short run can be sparse for attack class; some batches report F1=0.0.
- Resource narrative charts are included in the final report; CPU and executor metrics currently have 0% populated coverage in the final summary schema.
- One-shot official orchestrator exited early once (after Layer A/B); final completion used stage-resume execution for Layer C/watermark/load/report.

## 4) Operational Conclusions (Final)

- Recommended default profile for current environment:
  - Layer A: A_high (maxOffsetsPerTrigger=8000, shuffle=16, trigger=10 seconds)
  - Layer B: random_forest + full
- Current resilience baseline:
  - Use 30.39 s as the worst-case recovery reference for regression checks.
- Current event-time baseline:
  - Keep watermark at 10 seconds to avoid high late-event ratio seen at 0 seconds.
- Quality gate for deployment decisions:
  - Do not accept medium/high load profiles unless FNR is reduced from 1.0 to acceptable threshold.
  - Add attack-balanced replay windows before claiming production readiness.

## 5) Reproducibility Notes

- Official one-shot command was executed with final repeat counts and optional branches.
- Because the one-shot process exited with code 1 mid-pipeline, completion was resumed by stage scripts using final output paths.
- Final consolidated report was rebuilt from final CSVs and is the authoritative source for this submission.

## 6) Narrative Charts (Resource Observability)

Primary chart source:
- artifacts/streaming/online/online_evaluation_report_final.md (section "Resource Observability Narrative Charts")

Quick values (fallback if Mermaid is not rendered in your editor):

### Layer A driver RSS median (MB)
- A_high: 142.016
- A_low: 142.137
- A_mid: 142.012

### Layer B driver RSS median (MB)
- gradient_boosting+full: 141.941
- gradient_boosting+reduced: 141.938
- logistic_regression+full: 142.043
- logistic_regression+reduced: 141.973
- random_forest+full: 142.309
- random_forest+reduced: 141.984

### Telemetry coverage (% populated rows)
- Layer A executor utilization: 0.0%
- Layer B executor utilization: 0.0%
- Layer A CPU field coverage: 0.0% (field absent)
- Layer B CPU field coverage: 0.0% (field absent)
