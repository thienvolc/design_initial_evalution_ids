from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.streaming.benchmark.orchestration.runner import BenchmarkMatrixOptions, run_benchmark_matrix  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run fairness benchmark matrix (layers A/B/C)")
    parser.add_argument(
        "--benchmark-config",
        type=str,
        default="experiments/streaming/benchmark.yaml",
        help="Path to benchmark matrix config",
    )
    parser.add_argument("--only-runs", nargs="*", default=None, help="Optional run_id filters")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return run_benchmark_matrix(
        BenchmarkMatrixOptions(
            benchmark_config_path=args.benchmark_config,
            only_runs=tuple(args.only_runs) if args.only_runs else None,
            python_executable=sys.executable,
            project_root=PROJECT_ROOT,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())

