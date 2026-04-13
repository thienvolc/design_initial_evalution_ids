from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.orchestration.full_evaluation import (  # noqa: E402
    FullEvaluationOptions,
    run_full_evaluation,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one-shot online evaluation A->B->C->report")
    parser.add_argument("--config", type=str, default="configs/streaming/online.yaml")

    parser.add_argument("--layer-a-repeats", type=int, default=3)
    parser.add_argument("--layer-a-max-rows", type=int, default=1000)
    parser.add_argument("--layer-a-batch-size", type=int, default=500)
    parser.add_argument("--layer-a-warmup-rows", type=int, default=0)
    parser.add_argument("--layer-a-warmup-rows-per-sec", type=float, default=0.0)
    parser.add_argument("--layer-a-warmup-rate-schedule", type=str, default="")
    parser.add_argument("--layer-a-metrics-timeout-sec", type=int, default=60)
    parser.add_argument("--layer-a-metrics-idle-sec", type=int, default=5)
    parser.add_argument("--layer-a-summary-csv", type=str, default="artifacts/streaming/online/layer_a_summary_final.csv")

    parser.add_argument("--layer-b-repeats", type=int, default=3)
    parser.add_argument("--layer-b-max-rows", type=int, default=400)
    parser.add_argument("--layer-b-batch-size", type=int, default=200)
    parser.add_argument("--layer-b-warmup-rows", type=int, default=0)
    parser.add_argument("--layer-b-warmup-rows-per-sec", type=float, default=0.0)
    parser.add_argument("--layer-b-warmup-rate-schedule", type=str, default="")
    parser.add_argument("--layer-b-metrics-timeout-sec", type=int, default=45)
    parser.add_argument("--layer-b-metrics-idle-sec", type=int, default=5)
    parser.add_argument("--layer-b-summary-csv", type=str, default="artifacts/streaming/online/layer_b_summary_final.csv")

    parser.add_argument("--layer-c-repeats", type=int, default=3)
    parser.add_argument("--layer-c-warmup-rows", type=int, default=600)
    parser.add_argument("--layer-c-post-fault-rows", type=int, default=600)
    parser.add_argument("--layer-c-batch-size", type=int, default=300)
    parser.add_argument("--layer-c-summary-csv", type=str, default="artifacts/streaming/online/layer_c_summary_final.csv")
    parser.add_argument("--include-watermark-matrix", action="store_true")
    parser.add_argument("--include-load-quality", action="store_true")
    parser.add_argument("--watermark-summary-csv", type=str, default="artifacts/streaming/online/watermark_summary_final.csv")
    parser.add_argument("--watermark-warmup-rows", type=int, default=0)
    parser.add_argument("--watermark-warmup-rows-per-sec", type=float, default=0.0)
    parser.add_argument("--watermark-warmup-rate-schedule", type=str, default="")
    parser.add_argument("--watermark-metrics-timeout-sec", type=int, default=90)
    parser.add_argument("--watermark-metrics-idle-sec", type=int, default=15)
    parser.add_argument("--load-quality-summary-csv", type=str, default="artifacts/streaming/online/load_quality_summary_final.csv")
    parser.add_argument("--load-quality-warmup-rows", type=int, default=0)
    parser.add_argument("--load-quality-warmup-rows-per-sec", type=float, default=0.0)
    parser.add_argument("--load-quality-warmup-rate-schedule", type=str, default="")
    parser.add_argument("--load-quality-metrics-timeout-sec", type=int, default=90)
    parser.add_argument("--load-quality-metrics-idle-sec", type=int, default=8)

    parser.add_argument("--report-md", type=str, default="artifacts/streaming/online/online_evaluation_report_final.md")
    parser.add_argument("--report-json", type=str, default="artifacts/streaming/online/online_evaluation_report_final.json")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return run_full_evaluation(
        FullEvaluationOptions(
            config=args.config,
            layer_a_repeats=args.layer_a_repeats,
            layer_a_max_rows=args.layer_a_max_rows,
            layer_a_batch_size=args.layer_a_batch_size,
            layer_a_warmup_rows=args.layer_a_warmup_rows,
            layer_a_warmup_rows_per_sec=args.layer_a_warmup_rows_per_sec,
            layer_a_warmup_rate_schedule=args.layer_a_warmup_rate_schedule,
            layer_a_metrics_timeout_sec=args.layer_a_metrics_timeout_sec,
            layer_a_metrics_idle_sec=args.layer_a_metrics_idle_sec,
            layer_a_summary_csv=args.layer_a_summary_csv,
            layer_b_repeats=args.layer_b_repeats,
            layer_b_max_rows=args.layer_b_max_rows,
            layer_b_batch_size=args.layer_b_batch_size,
            layer_b_warmup_rows=args.layer_b_warmup_rows,
            layer_b_warmup_rows_per_sec=args.layer_b_warmup_rows_per_sec,
            layer_b_warmup_rate_schedule=args.layer_b_warmup_rate_schedule,
            layer_b_metrics_timeout_sec=args.layer_b_metrics_timeout_sec,
            layer_b_metrics_idle_sec=args.layer_b_metrics_idle_sec,
            layer_b_summary_csv=args.layer_b_summary_csv,
            layer_c_repeats=args.layer_c_repeats,
            layer_c_warmup_rows=args.layer_c_warmup_rows,
            layer_c_post_fault_rows=args.layer_c_post_fault_rows,
            layer_c_batch_size=args.layer_c_batch_size,
            layer_c_summary_csv=args.layer_c_summary_csv,
            include_watermark_matrix=args.include_watermark_matrix,
            include_load_quality=args.include_load_quality,
            watermark_summary_csv=args.watermark_summary_csv,
            watermark_warmup_rows=args.watermark_warmup_rows,
            watermark_warmup_rows_per_sec=args.watermark_warmup_rows_per_sec,
            watermark_warmup_rate_schedule=args.watermark_warmup_rate_schedule,
            watermark_metrics_timeout_sec=args.watermark_metrics_timeout_sec,
            watermark_metrics_idle_sec=args.watermark_metrics_idle_sec,
            load_quality_summary_csv=args.load_quality_summary_csv,
            load_quality_warmup_rows=args.load_quality_warmup_rows,
            load_quality_warmup_rows_per_sec=args.load_quality_warmup_rows_per_sec,
            load_quality_warmup_rate_schedule=args.load_quality_warmup_rate_schedule,
            load_quality_metrics_timeout_sec=args.load_quality_metrics_timeout_sec,
            load_quality_metrics_idle_sec=args.load_quality_metrics_idle_sec,
            report_md=args.report_md,
            report_json=args.report_json,
        ),
        python_executable=sys.executable,
    )


if __name__ == "__main__":
    raise SystemExit(main())

