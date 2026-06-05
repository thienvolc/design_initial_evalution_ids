from __future__ import annotations

import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PYTHON_EXE = sys.executable

PAPER_PLOTS_DIR = PROJECT_ROOT / "paper" / "figures" / "plots"


def _run(args: list[str]) -> None:
    subprocess.run([PYTHON_EXE, *args], cwd=PROJECT_ROOT, check=True)


def _build_cdf(
    inputs: list[Path],
    metric: str,
    title: str,
    output_name: str,
    *,
    group_by_label: bool = False,
    show_raw_replicates: bool = False,
) -> None:
    args = [
        "scripts/paper/build_latency_cdf_plot.py",
        "--inputs",
        *[str(path) for path in inputs],
        "--metric",
        metric,
        "--title",
        title,
        "--xlabel",
        "Latency (ms)",
        "--output",
        str(PAPER_PLOTS_DIR / output_name),
    ]
    if group_by_label:
        args.append("--group-by-label")
    if show_raw_replicates:
        args.append("--show-raw-replicates")
    _run(args)


def main() -> int:
    PAPER_PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    evaluation_dir = PROJECT_ROOT / "artifacts/streaming/evaluation"
    _build_cdf(
        [evaluation_dir / "capacity_calibration.csv"],
        "p95_e2e_ms",
        "Capacity Calibration: Empirical CDF of End-to-End P95 Latency",
        "latency_cdf_e2e_p95_ms.capacity_calibration.png",
        group_by_label=True,
        show_raw_replicates=True,
    )

    _build_cdf(
        [evaluation_dir / "model_feature_tradeoff.csv"],
        "e2e_p95_ms",
        "Model Feature Tradeoff: Empirical CDF of End-to-End P95 Latency",
        "latency_cdf_e2e_p95_ms.model_feature_tradeoff.png",
        group_by_label=True,
        show_raw_replicates=True,
    )

    _build_cdf(
        [evaluation_dir / "fault_recovery.csv"],
        "recovery_seconds",
        "Fault Recovery: Empirical CDF of Recovery Time",
        "recovery_seconds_cdf.fault_recovery.png",
        group_by_label=True,
        show_raw_replicates=True,
    )

    _build_cdf(
        [evaluation_dir / "overload_degradation.csv"],
        "p95_e2e_ms",
        "Overload Degradation: Empirical CDF of End-to-End P95 Latency",
        "latency_cdf_e2e_p95_ms.overload_degradation.png",
        group_by_label=True,
        show_raw_replicates=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
