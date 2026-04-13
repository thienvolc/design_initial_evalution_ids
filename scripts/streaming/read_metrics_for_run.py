from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.reporting.metrics_reader import read_metric_from_config  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read first metrics payload for a run_tag from Kafka")
    parser.add_argument("--config", type=str, default="configs/streaming/online.yaml")
    parser.add_argument("--run-tag", type=str, required=True)
    parser.add_argument("--timeout-sec", type=int, default=60)
    parser.add_argument("--after-ts-utc", type=str, default="")
    parser.add_argument("--after-epoch-ms", type=int, default=0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = read_metric_from_config(
        config_path=args.config,
        run_tag=args.run_tag,
        timeout_sec=args.timeout_sec,
        after_ts_utc=args.after_ts_utc,
        after_epoch_ms=args.after_epoch_ms,
    )
    if payload is None:
        return 2

    print(json.dumps(payload, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

