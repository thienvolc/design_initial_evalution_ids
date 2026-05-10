from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.evaluation.reporting.report_builder import generate_online_evaluation_report  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build consolidated streaming evaluation report from Layer A/B/C summaries")
    parser.add_argument("--layer-a", type=str, default="artifacts/streaming/evaluation/layer_a_summary.csv")
    parser.add_argument("--layer-b", type=str, default="artifacts/streaming/evaluation/layer_b_summary.csv")
    parser.add_argument("--layer-c", type=str, default="artifacts/streaming/evaluation/layer_c_summary.csv")
    parser.add_argument("--watermark-summary", type=str, default="")
    parser.add_argument("--load-quality-summary", type=str, default="")
    parser.add_argument("--out-md", type=str, default="artifacts/streaming/evaluation/streaming_evaluation_report.md")
    parser.add_argument("--out-json", type=str, default="artifacts/streaming/evaluation/streaming_evaluation_report.json")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    _, out_md_path, out_json_path = generate_online_evaluation_report(
        layer_a_path=args.layer_a,
        layer_b_path=args.layer_b,
        layer_c_path=args.layer_c,
        watermark_path=args.watermark_summary.strip() or None,
        load_quality_path=args.load_quality_summary.strip() or None,
        out_md_path=args.out_md,
        out_json_path=args.out_json,
    )

    print(f"Saved report markdown: {out_md_path}", flush=True)
    print(f"Saved report json: {out_json_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
