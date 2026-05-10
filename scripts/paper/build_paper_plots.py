from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PYTHON_EXE = sys.executable

TIMESERIES_DIR = PROJECT_ROOT / "artifacts" / "streaming" / "metrics_timeseries"
OFFLINE_EVAL_DIR = PROJECT_ROOT / "artifacts" / "offline" / "evaluation"
PAPER_PLOTS_DIR = PROJECT_ROOT / "paper" / "figures" / "plots"


def _run(args: list[str]) -> None:
    subprocess.run([PYTHON_EXE, *args], cwd=PROJECT_ROOT, check=True)


def _read_run_tags(summary_path: Path) -> list[str]:
    frame = pd.read_csv(summary_path)
    if "run_tag" not in frame.columns:
        raise ValueError(f"Missing run_tag column in {summary_path}")
    return [str(value) for value in frame["run_tag"].dropna().tolist()]


def _read_run_tags_many(summary_paths: list[Path]) -> list[str]:
    run_tags: list[str] = []
    seen: set[str] = set()
    for summary_path in summary_paths:
        for run_tag in _read_run_tags(summary_path):
            if run_tag in seen:
                continue
            seen.add(run_tag)
            run_tags.append(run_tag)
    return run_tags


def _timeseries_inputs(run_tags: list[str]) -> list[str]:
    return [str(TIMESERIES_DIR / f"{run_tag}.csv") for run_tag in run_tags]


def _build_timeseries(
    inputs: list[str],
    output_name: str,
    *,
    group_by_label: bool = False,
    show_raw_replicates: bool = False,
    band_mode: str = "none",
    x_max: float | None = None,
) -> None:
    args = [
        "scripts/streaming/official/build_timeseries_plots.py",
        "--inputs",
        *inputs,
        "--output-dir",
        str(PAPER_PLOTS_DIR),
    ]
    if group_by_label:
        args.append("--group-by-label")
    if show_raw_replicates:
        args.append("--show-raw-replicates")
    if band_mode != "none":
        args.extend(["--band-mode", band_mode])
    if x_max is not None:
        args.extend(["--x-max", str(x_max)])
    _run(args)
    expected_output = PAPER_PLOTS_DIR / output_name
    if not expected_output.exists():
        raise FileNotFoundError(f"Expected plot was not generated: {expected_output}")


def _build_cdf(
    inputs: list[str],
    title: str,
    output_name: str,
    *,
    group_by_label: bool = False,
    show_raw_replicates: bool = False,
) -> None:
    args = [
        "scripts/streaming/official/build_latency_cdf_plot.py",
        "--inputs",
        *inputs,
        "--metric",
        "source_to_emit_p95_ms",
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


def _build_offline_reduced_feature_plot() -> None:
    sweep_path = OFFLINE_EVAL_DIR / "feature_sweep_results_feature_sweep_20260427_131113.csv"
    ablation_path = OFFLINE_EVAL_DIR / "ablation_results_ablation_20260427_124852.csv"
    output_path = PAPER_PLOTS_DIR / "appendix_reduced_feature_f1_vs_features.png"

    sweep = pd.read_csv(sweep_path)
    ablation = pd.read_csv(ablation_path)

    sweep = sweep.loc[sweep["fpr_budget"].round(4) == 0.03].copy()
    ablation = ablation.loc[ablation["fpr_budget"].round(4) == 0.03].copy()

    model_labels = {
        "random_forest": "Random Forest Sweep",
        "gradient_boosting": "Gradient Boosting Sweep",
    }
    ablation_labels = {
        "current_reduced_manifest_k17": "Ablation: current_reduced_manifest_k17",
        "topk_mutual_info_k17": "Ablation: topk_mutual_info_k17",
        "full_all_features": "Ablation: full_all_features",
    }
    model_colors = {
        "random_forest": "#1f77b4",
        "gradient_boosting": "#d62728",
    }

    try:
        import matplotlib.pyplot as plt
    except ImportError as exc:
        raise RuntimeError("matplotlib is required to build paper plots") from exc

    plt.figure(figsize=(8.4, 5.0))

    for model_key in ["random_forest", "gradient_boosting"]:
        plot_frame = (
            sweep.loc[sweep["model"] == model_key, ["n_features", "test_f1"]]
            .sort_values("n_features")
            .drop_duplicates(subset=["n_features"], keep="last")
        )
        if plot_frame.empty:
            continue
        plt.plot(
            plot_frame["n_features"],
            plot_frame["test_f1"],
            linewidth=2.0,
            marker="o",
            markersize=4.0,
            color=model_colors[model_key],
            label=model_labels[model_key],
        )

    marker_cycle = {
        "current_reduced_manifest_k17": "X",
        "topk_mutual_info_k17": "s",
        "full_all_features": "D",
    }
    for feature_label in ["current_reduced_manifest_k17", "topk_mutual_info_k17", "full_all_features"]:
        point = ablation.loc[
            (ablation["model"] == "gradient_boosting") & (ablation["feature_set_label"] == feature_label),
            ["n_features", "test_f1"],
        ].head(1)
        if point.empty:
            continue
        plt.scatter(
            point["n_features"],
            point["test_f1"],
            s=70,
            marker=marker_cycle[feature_label],
            color="#2f2f2f",
            label=ablation_labels[feature_label],
            zorder=3,
        )

    plt.title("Offline Reduced-Feature Diagnostics")
    plt.xlabel("Number of Features")
    plt.ylabel("Test F1")
    plt.grid(True, linestyle="--", alpha=0.28)
    plt.legend(frameon=False, fontsize=8, loc="best")
    plt.tight_layout()
    plt.savefig(output_path, dpi=180)
    plt.close()
    print(f"Saved plot: {output_path}")


def main() -> int:
    PAPER_PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    evaluation_dir = PROJECT_ROOT / "artifacts/streaming/evaluation"
    layer_a_run_tags = _read_run_tags_many(
        [
            evaluation_dir / "layer_a_summary_500k.csv",
            evaluation_dir / "layer_a_summary_500k_rerun02.csv",
            evaluation_dir / "layer_a_summary_500k_rerun03.csv",
        ]
    )
    layer_b_run_tags = _read_run_tags_many(
        [
            evaluation_dir / "layer_b_summary_500k.csv",
            evaluation_dir / "layer_b_summary_500k_rerun02.csv",
            evaluation_dir / "layer_b_summary_500k_rerun03.csv",
        ]
    )
    layer_c_run_tags = _read_run_tags_many(
        [
            evaluation_dir / "layer_c_summary_700k_fault.csv",
            evaluation_dir / "layer_c_summary_700k_fault_rerun02.csv",
            evaluation_dir / "layer_c_summary_700k_fault_rerun03.csv",
        ]
    )
    watermark_run_tags = _read_run_tags_many(
        [
            evaluation_dir / "watermark_summary_fullschedule_rerun01.csv",
            evaluation_dir / "watermark_summary_fullschedule_rerun02.csv",
            evaluation_dir / "watermark_summary_fullschedule_rerun03.csv",
        ]
    )
    load_run_tags = _read_run_tags_many(
        [
            evaluation_dir / "load_quality_summary_stress_fullschedule_rerun01.csv",
            evaluation_dir / "load_quality_summary_stress_fullschedule_rerun02.csv",
            evaluation_dir / "load_quality_summary_stress_fullschedule_rerun03.csv",
        ]
    )

    layer_b_baseline_run_tags = []
    for summary_name in [
        "layer_b_summary_500k_with_lr_full_rerun01.csv",
        "layer_b_summary_500k_with_lr_full_rerun02.csv",
        "layer_b_summary_500k_with_lr_full_rerun03.csv",
    ]:
        frame = pd.read_csv(PROJECT_ROOT / "artifacts/streaming/evaluation" / summary_name)
        baseline_rows = frame.loc[
            frame["load_profile"].isin(["logistic_regression:full", "random_forest:full"]),
            "run_tag",
        ]
        layer_b_baseline_run_tags.extend([str(value) for value in baseline_rows.dropna().tolist()])

    _build_timeseries(
        _timeseries_inputs(layer_a_run_tags),
        "timeseries_rows_per_sec.layer_a.png",
        group_by_label=True,
        show_raw_replicates=True,
        band_mode="minmax",
    )
    _build_timeseries(
        _timeseries_inputs(layer_a_run_tags),
        "timeseries_kafka_lag_records_total.layer_a.png",
        group_by_label=True,
        show_raw_replicates=True,
        band_mode="minmax",
    )
    _build_cdf(
        _timeseries_inputs(layer_a_run_tags),
        "Layer A: Empirical CDF of Source-to-Emit P95 Latency",
        "latency_cdf_source_to_emit_p95_ms.layer_a.png",
        group_by_label=True,
        show_raw_replicates=True,
    )

    _build_cdf(
        _timeseries_inputs(layer_b_run_tags),
        "Layer B: Empirical CDF of Source-to-Emit P95 Latency",
        "latency_cdf_source_to_emit_p95_ms.layer_b.png",
        group_by_label=True,
        show_raw_replicates=True,
    )
    _build_cdf(
        _timeseries_inputs(layer_b_baseline_run_tags),
        "Layer B Baseline: Empirical CDF of Source-to-Emit P95 Latency",
        "latency_cdf_source_to_emit_p95_ms.layer_b_lrfull_baseline.png",
        group_by_label=True,
        show_raw_replicates=True,
    )

    _build_timeseries(
        _timeseries_inputs(layer_c_run_tags),
        "timeseries_source_to_emit_p95_ms.layer_c.png",
        group_by_label=True,
        show_raw_replicates=True,
        band_mode="minmax",
    )
    _build_cdf(
        _timeseries_inputs(layer_c_run_tags),
        "Layer C: Empirical CDF of Source-to-Emit P95 Latency",
        "latency_cdf_source_to_emit_p95_ms.layer_c.png",
        group_by_label=True,
        show_raw_replicates=True,
    )

    _build_timeseries(
        _timeseries_inputs(watermark_run_tags),
        "timeseries_source_to_emit_p95_ms.watermark.png",
        group_by_label=True,
        show_raw_replicates=True,
        band_mode="minmax",
    )
    _build_cdf(
        _timeseries_inputs(watermark_run_tags),
        "Watermark: Empirical CDF of Source-to-Emit P95 Latency",
        "latency_cdf_source_to_emit_p95_ms.watermark.png",
        group_by_label=True,
        show_raw_replicates=True,
    )

    _build_timeseries(
        _timeseries_inputs(load_run_tags),
        "timeseries_rows_per_sec.load.png",
        group_by_label=True,
        show_raw_replicates=True,
        band_mode="minmax",
    )
    _build_timeseries(
        _timeseries_inputs(load_run_tags),
        "timeseries_kafka_lag_records_total.load.png",
        group_by_label=True,
        show_raw_replicates=True,
        band_mode="minmax",
    )
    _build_cdf(
        _timeseries_inputs(load_run_tags),
        "Load Quality: Empirical CDF of Source-to-Emit P95 Latency",
        "latency_cdf_source_to_emit_p95_ms.load.png",
        group_by_label=True,
        show_raw_replicates=True,
    )

    _build_offline_reduced_feature_plot()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
