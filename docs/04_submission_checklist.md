# Submission Checklist (Option A)

## 1) Scope Lock
- [x] Binary detection only (Benign vs Attack)
- [x] Spark Structured Streaming pipeline
- [x] Final write-up explicitly states non-goals (no online multiclass benchmark)

## 2) Benchmark Reliability
- [x] Layer A supports repeated runs and unique run tags
- [x] Layer B supports repeated runs and unique run tags
- [x] Layer C supports repeated runs and 4 fault scenarios
- [x] Consolidated report supports min/median/p95/max for A/B/C
- [x] Run final repeated benchmark at required scale (A/B: 5-10 repeats)

## 3) Metrics Coverage
- [x] Latency p50/p95/p99
- [x] Throughput (rows_per_sec)
- [x] Kafka lag approximation per batch
- [x] Event-time late ratio and watermark experiment hooks
- [x] Detection metrics per batch: precision/recall/F1/FPR/FNR
- [x] Driver/executor memory utilization fields
- [x] Add CPU/RAM and executor utilization charts in final report narrative

## 4) Event-Time and Reliability Experiments
- [x] Watermark matrix runner added
- [x] Fault matrix includes kafka_restart and spark_process_restart
- [x] Fault matrix extended with producer_restart and network_slowdown
- [x] Run full watermark matrix (0/10/30/60) with repeated runs

## 5) Detection Under Load
- [x] Load-quality matrix runner added
- [x] Run load profiles at configured final scales (1k/2k/3k rows)
- [x] Summarize trade-off between latency/throughput and F1/FPR/FNR

## 6) One-Command Pipeline
- [x] One-shot A->B->C->report orchestrator exists
- [x] Orchestrator supports optional watermark/load-quality branches
- [x] Execute one-shot final run with official repeat counts

## 7) Final Documents
- [x] Methodology section finalized
- [x] Limitations section finalized (single-broker caveat)
- [x] Operational conclusions finalized (recommended profile and resilience baseline)

## 8) Scale-Up Alignment
- [x] Document actual executed row scale by phase
- [ ] Align execution to large-scale targets (A/B >= 500k rows; watermark >= 1M rows)
- [ ] Run dedicated stress campaign (5M-20M rows)

Notes:
- One-shot official run was launched with final parameters and produced Layer A/B outputs, then exited with code 1.
- Remaining stages (Layer C, watermark, load-quality, final report build) were resumed and completed with final artifact paths.
- CPU/RAM/executor narrative charts are included in final report; CPU and executor coverage are currently 0% for this run schema.
- Current final artifacts represent initial operational scale (thousands to low tens of thousands of rows), not million-row stress scale.

## Suggested Final Run Commands

Run one-shot official benchmark with optional branches:

```powershell
python scripts/streaming/run_online_full_evaluation.py \
  --config configs/streaming/online.yaml \
  --layer-a-repeats 5 \
  --layer-b-repeats 5 \
  --layer-c-repeats 3 \
  --include-watermark-matrix \
  --include-load-quality
```

Build final consolidated report explicitly:

```powershell
python scripts/streaming/build_online_report.py \
  --layer-a artifacts/streaming/online/layer_a_summary_final.csv \
  --layer-b artifacts/streaming/online/layer_b_summary_final.csv \
  --layer-c artifacts/streaming/online/layer_c_summary_final.csv \
  --watermark-summary artifacts/streaming/online/watermark_summary_final.csv \
  --load-quality-summary artifacts/streaming/online/load_quality_summary_final.csv \
  --out-md artifacts/streaming/online/online_evaluation_report_final.md \
  --out-json artifacts/streaming/online/online_evaluation_report_final.json
```



