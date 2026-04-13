from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st
import yaml


ROOT = Path(__file__).resolve().parents[1]
STREAMING_ARTIFACTS_ROOT = ROOT / "artifacts" / "streaming"
ONLINE_ARTIFACTS_DIR = STREAMING_ARTIFACTS_ROOT / "online"
SCALE_UP_ARTIFACTS_DIR = STREAMING_ARTIFACTS_ROOT / "scale_up"
BENCHMARK_ARTIFACTS_DIR = STREAMING_ARTIFACTS_ROOT / "benchmark"

SUMMARY_FILE_PATTERNS = {
    "layer_a": ("layer_a_summary.csv", "layer_a_summary*.csv"),
    "layer_b": ("layer_b_summary.csv", "layer_b_summary*.csv"),
    "layer_c": ("layer_c_summary.csv", "layer_c_summary*.csv"),
    "watermark": ("watermark_summary.csv", "watermark_summary*.csv"),
    "load_quality": ("load_quality_summary.csv", "load_quality_summary*.csv"),
}
REPORT_JSON_PATTERN = ("online_evaluation_report.json", "online_evaluation_report*.json")
REPORT_MD_PATTERN = ("online_evaluation_report.md", "online_evaluation_report*.md")


@dataclass(frozen=True)
class SummaryArtifact:
    name: str
    path: Path | None
    dataframe: pd.DataFrame


@dataclass(frozen=True)
class EvaluationBundle:
    environment: str
    base_dir: Path
    summaries: dict[str, SummaryArtifact]
    report_json_path: Path | None
    report_json: dict[str, Any] | None
    report_md_path: Path | None
    report_md: str


def _read_yaml_mapping(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return payload if isinstance(payload, dict) else {}


def _read_json_mapping(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.exists():
        return None
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else None


def _read_markdown(path: Path | None) -> str:
    if path is None or not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def _read_csv(path: Path | None) -> pd.DataFrame:
    if path is None or not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _find_preferred_file(base_dir: Path, exact_name: str, pattern: str) -> Path | None:
    exact_path = base_dir / exact_name
    if exact_path.exists():
        return exact_path

    candidates = sorted(
        base_dir.glob(pattern),
        key=lambda candidate: (candidate.stat().st_mtime, candidate.name),
        reverse=True,
    )
    return candidates[0] if candidates else None


@st.cache_data(show_spinner=False)
def load_profile_metadata() -> dict[str, dict[str, str]]:
    mapping: dict[str, dict[str, str]] = {}
    profile_files = [
        ROOT / "experiments" / "streaming" / "profiles" / "local_profiles.yaml",
        ROOT / "experiments" / "streaming" / "profiles" / "online_profiles.yaml",
    ]
    for profile_file in profile_files:
        payload = _read_yaml_mapping(profile_file)
        profiles = payload.get("profiles") or {}
        if not isinstance(profiles, dict):
            continue
        for profile_name, profile_payload in profiles.items():
            if profile_name.startswith("_") or not isinstance(profile_payload, dict):
                continue
            args = profile_payload.get("args") or {}
            if not isinstance(args, dict):
                continue
            summary_csv = args.get("summary_csv")
            if not summary_csv:
                continue
            resolved_summary = (ROOT / str(summary_csv)).resolve()
            mapping[str(resolved_summary)] = {
                "profile_name": profile_name,
                "mode": str(profile_payload.get("mode", "")).strip(),
                "resource_class": str(profile_payload.get("resource_class", "")).strip(),
            }
    return mapping


@st.cache_data(show_spinner=False)
def load_evaluation_bundle(environment: str) -> EvaluationBundle:
    base_dir = ONLINE_ARTIFACTS_DIR if environment == "online" else SCALE_UP_ARTIFACTS_DIR
    summaries: dict[str, SummaryArtifact] = {}

    for name, (exact_name, pattern) in SUMMARY_FILE_PATTERNS.items():
        artifact_path = _find_preferred_file(base_dir, exact_name, pattern)
        summaries[name] = SummaryArtifact(
            name=name,
            path=artifact_path,
            dataframe=_read_csv(artifact_path),
        )

    report_json_path = _find_preferred_file(base_dir, *REPORT_JSON_PATTERN)
    report_md_path = _find_preferred_file(base_dir, *REPORT_MD_PATTERN)

    return EvaluationBundle(
        environment=environment,
        base_dir=base_dir,
        summaries=summaries,
        report_json_path=report_json_path,
        report_json=_read_json_mapping(report_json_path),
        report_md_path=report_md_path,
        report_md=_read_markdown(report_md_path),
    )


@st.cache_data(show_spinner=False)
def load_benchmark_summary() -> SummaryArtifact:
    artifact_path = BENCHMARK_ARTIFACTS_DIR / "benchmark_summary.csv"
    return SummaryArtifact(
        name="benchmark",
        path=artifact_path if artifact_path.exists() else None,
        dataframe=_read_csv(artifact_path),
    )


def _latest_artifact_mtime(base_dir: Path) -> float:
    latest = 0.0
    for _, (exact_name, pattern) in SUMMARY_FILE_PATTERNS.items():
        artifact_path = _find_preferred_file(base_dir, exact_name, pattern)
        if artifact_path is not None:
            latest = max(latest, artifact_path.stat().st_mtime)
    for exact_name, pattern in (REPORT_JSON_PATTERN, REPORT_MD_PATTERN):
        artifact_path = _find_preferred_file(base_dir, exact_name, pattern)
        if artifact_path is not None:
            latest = max(latest, artifact_path.stat().st_mtime)
    return latest


def _default_environment() -> str:
    online_mtime = _latest_artifact_mtime(ONLINE_ARTIFACTS_DIR)
    scale_up_mtime = _latest_artifact_mtime(SCALE_UP_ARTIFACTS_DIR)
    if scale_up_mtime > online_mtime:
        return "scale_up"
    return "online"


def _safe_relative_path(path: Path | None) -> str:
    if path is None:
        return "Not found"
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def _coerce_numeric_column(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series(dtype="float64")
    return pd.to_numeric(df[column], errors="coerce")


def _ok_rows(df: pd.DataFrame) -> pd.DataFrame:
    if "status" not in df.columns:
        return df.copy()
    return df[df["status"].astype(str).str.lower().eq("ok")].copy()


def _render_metric_row(container: Any, title: str, value: str, delta: str | None = None) -> None:
    container.metric(title, value, delta=delta)


def _render_overview(bundle: EvaluationBundle, benchmark_artifact: SummaryArtifact) -> None:
    st.info(
        "This dashboard presents official post-run evaluation outputs from summary CSVs and consolidated reports. "
        "Those summaries are currently derived from SUT-emitted ids.metrics; Prometheus/Grafana remain the live telemetry view."
    )
    layer_a_df = _ok_rows(bundle.summaries["layer_a"].dataframe)
    layer_b_df = _ok_rows(bundle.summaries["layer_b"].dataframe)
    layer_c_df = _ok_rows(bundle.summaries["layer_c"].dataframe)
    if not layer_a_df.empty:
        layer_a_df["rows_per_sec_avg"] = _coerce_numeric_column(layer_a_df, "rows_per_sec_avg")
    if not layer_b_df.empty:
        layer_b_df["e2e_p95_ms"] = _coerce_numeric_column(layer_b_df, "e2e_p95_ms")
    if not layer_c_df.empty:
        layer_c_df["recovery_seconds"] = _coerce_numeric_column(layer_c_df, "recovery_seconds")

    cards = st.columns(4)
    if not layer_a_df.empty:
        best_layer_a = layer_a_df.sort_values("rows_per_sec_avg", ascending=False).iloc[0]
        _render_metric_row(cards[0], "Layer A Best Profile", str(best_layer_a.get("profile", "-")))
        _render_metric_row(cards[1], "Layer A Throughput", f"{float(best_layer_a['rows_per_sec_avg']):,.1f} rows/s")
    else:
        _render_metric_row(cards[0], "Layer A Best Profile", "No data")
        _render_metric_row(cards[1], "Layer A Throughput", "No data")

    if not layer_b_df.empty:
        best_layer_b = layer_b_df.sort_values("e2e_p95_ms", ascending=True).iloc[0]
        combo = f"{best_layer_b.get('model', '-')} / {best_layer_b.get('feature_set', '-')}"
        _render_metric_row(cards[2], "Layer B Fastest Combo", combo)
        _render_metric_row(cards[3], "Layer B p95 E2E", f"{float(best_layer_b['e2e_p95_ms']):,.0f} ms")
    else:
        _render_metric_row(cards[2], "Layer B Fastest Combo", "No data")
        _render_metric_row(cards[3], "Layer B p95 E2E", "No data")

    extra_cards = st.columns(3)
    if not layer_c_df.empty:
        worst_recovery = _coerce_numeric_column(layer_c_df, "recovery_seconds").max()
        _render_metric_row(
            extra_cards[0],
            "Layer C Worst Recovery",
            f"{worst_recovery:,.2f} s" if pd.notna(worst_recovery) else "No data",
        )
    else:
        _render_metric_row(extra_cards[0], "Layer C Worst Recovery", "No data")

    watermark_df = _ok_rows(bundle.summaries["watermark"].dataframe)
    if not watermark_df.empty:
        best_watermark = watermark_df.iloc[0]
        watermark_columns = [column for column in watermark_df.columns if "f1" in column.lower() or "fnr" in column.lower()]
        watermark_value = ", ".join(
            f"{column}={best_watermark[column]}" for column in watermark_columns[:2] if pd.notna(best_watermark[column])
        )
        _render_metric_row(extra_cards[1], "Watermark Summary", watermark_value or "Available")
    else:
        _render_metric_row(extra_cards[1], "Watermark Summary", "Not available")

    benchmark_df = benchmark_artifact.dataframe
    if not benchmark_df.empty:
        benchmark_best = benchmark_df.sort_values("rows_per_second", ascending=False).iloc[0]
        _render_metric_row(extra_cards[2], "Benchmark Peak Throughput", f"{float(benchmark_best['rows_per_second']):,.1f} rows/s")
    else:
        _render_metric_row(extra_cards[2], "Benchmark Peak Throughput", "No data")


def _render_layer_a(bundle: EvaluationBundle, profile_metadata: dict[str, dict[str, str]]) -> None:
    artifact = bundle.summaries["layer_a"]
    st.subheader("Layer A: System Knobs")
    st.caption(_safe_relative_path(artifact.path))
    df = artifact.dataframe.copy()
    if df.empty:
        st.info("Layer A summary is not available.")
        return
    if "evaluation_source" in df.columns:
        sources = sorted({str(value) for value in df["evaluation_source"].dropna().unique() if str(value).strip()})
        if sources:
            st.caption(f"Evaluation source: {', '.join(sources)}")

    resolved_meta = profile_metadata.get(str(artifact.path.resolve()), {}) if artifact.path else {}
    if resolved_meta:
        st.caption(
            f"Configured profile set: {resolved_meta.get('profile_name', '')} "
            f"({resolved_meta.get('mode', 'unknown')} / {resolved_meta.get('resource_class', 'unknown')})"
        )

    df["rows_per_sec_avg"] = _coerce_numeric_column(df, "rows_per_sec_avg")
    df["e2e_p95_ms_max"] = _coerce_numeric_column(df, "e2e_p95_ms_max")
    ok_df = _ok_rows(df)
    if ok_df.empty and not df.empty:
        st.warning("Layer A summary exists, but no successful rows were recorded. Showing all rows below.")
        st.dataframe(df, use_container_width=True)
        return
    ranking_df = ok_df.sort_values(["rows_per_sec_avg", "e2e_p95_ms_max"], ascending=[False, True])
    best_row = ranking_df.iloc[0] if not ranking_df.empty else None

    cols = st.columns(3)
    _render_metric_row(cols[0], "Profiles Tested", f"{len(df):,}")
    _render_metric_row(cols[1], "Successful Runs", f"{len(ok_df):,}")
    _render_metric_row(cols[2], "Best Profile", str(best_row["profile"]) if best_row is not None else "No data")

    if not ranking_df.empty:
        st.bar_chart(ranking_df.set_index("profile")["rows_per_sec_avg"])
    st.dataframe(ranking_df, use_container_width=True)


def _render_layer_b(bundle: EvaluationBundle) -> None:
    artifact = bundle.summaries["layer_b"]
    st.subheader("Layer B: Model x Feature Set")
    st.caption(_safe_relative_path(artifact.path))
    df = artifact.dataframe.copy()
    if df.empty:
        st.info("Layer B summary is not available.")
        return
    if "evaluation_source" in df.columns:
        sources = sorted({str(value) for value in df["evaluation_source"].dropna().unique() if str(value).strip()})
        if sources:
            st.caption(f"Evaluation source: {', '.join(sources)}")

    df["e2e_p95_ms"] = _coerce_numeric_column(df, "e2e_p95_ms")
    df["proc_p95_ms"] = _coerce_numeric_column(df, "proc_p95_ms")
    ok_df = _ok_rows(df)
    if ok_df.empty and not df.empty:
        st.warning("Layer B summary exists, but no successful rows were recorded. Showing all rows below.")
        st.dataframe(df, use_container_width=True)
        return
    ranking_df = ok_df.sort_values(["e2e_p95_ms", "proc_p95_ms"], ascending=[True, True])
    best_row = ranking_df.iloc[0] if not ranking_df.empty else None

    cols = st.columns(3)
    _render_metric_row(cols[0], "Combos Tested", f"{len(df):,}")
    _render_metric_row(cols[1], "Successful Runs", f"{len(ok_df):,}")
    _render_metric_row(
        cols[2],
        "Fastest Combo",
        f"{best_row['model']} / {best_row['feature_set']}" if best_row is not None else "No data",
    )

    if not ranking_df.empty:
        chart_index = ranking_df["model"].astype(str) + " / " + ranking_df["feature_set"].astype(str)
        st.bar_chart(pd.Series(ranking_df["e2e_p95_ms"].values, index=chart_index))
    st.dataframe(ranking_df, use_container_width=True)


def _render_layer_c(bundle: EvaluationBundle) -> None:
    artifact = bundle.summaries["layer_c"]
    st.subheader("Layer C: Fault Recovery")
    st.caption(_safe_relative_path(artifact.path))
    df = artifact.dataframe.copy()
    if df.empty:
        st.info("Layer C summary is not available.")
        return

    df["recovery_seconds"] = _coerce_numeric_column(df, "recovery_seconds")
    df["rows_per_sec_after_fault"] = _coerce_numeric_column(df, "rows_per_sec_after_fault")
    ok_df = _ok_rows(df)
    if ok_df.empty and not df.empty:
        st.warning("Layer C summary exists, but no successful rows were recorded. Showing all rows below.")
        st.dataframe(df, use_container_width=True)
        return
    ranking_df = ok_df.sort_values("recovery_seconds", ascending=False)
    worst_row = ranking_df.iloc[0] if not ranking_df.empty else None

    cols = st.columns(3)
    _render_metric_row(cols[0], "Scenarios Tested", f"{len(df):,}")
    _render_metric_row(cols[1], "Successful Recoveries", f"{len(ok_df):,}")
    _render_metric_row(
        cols[2],
        "Worst Recovery",
        f"{float(worst_row['recovery_seconds']):,.2f} s" if worst_row is not None else "No data",
    )

    if not ranking_df.empty:
        st.bar_chart(ranking_df.set_index("scenario")["recovery_seconds"])
    st.dataframe(ranking_df, use_container_width=True)


def _render_quality(bundle: EvaluationBundle) -> None:
    st.subheader("Watermark and Load Quality")
    watermark_artifact = bundle.summaries["watermark"]
    load_quality_artifact = bundle.summaries["load_quality"]

    col1, col2 = st.columns(2)
    with col1:
        st.caption(f"Watermark: {_safe_relative_path(watermark_artifact.path)}")
        if watermark_artifact.dataframe.empty:
            st.info("Watermark summary is not available for this environment.")
        else:
            if "evaluation_source" in watermark_artifact.dataframe.columns:
                sources = sorted(
                    {
                        str(value)
                        for value in watermark_artifact.dataframe["evaluation_source"].dropna().unique()
                        if str(value).strip()
                    }
                )
                if sources:
                    st.caption(f"Evaluation source: {', '.join(sources)}")
            if _ok_rows(watermark_artifact.dataframe).empty:
                st.warning("Watermark summary exists, but no successful rows were recorded.")
            st.dataframe(watermark_artifact.dataframe, use_container_width=True)

    with col2:
        st.caption(f"Load Quality: {_safe_relative_path(load_quality_artifact.path)}")
        if load_quality_artifact.dataframe.empty:
            st.info("Load-quality summary is not available for this environment.")
        else:
            if "evaluation_source" in load_quality_artifact.dataframe.columns:
                sources = sorted(
                    {
                        str(value)
                        for value in load_quality_artifact.dataframe["evaluation_source"].dropna().unique()
                        if str(value).strip()
                    }
                )
                if sources:
                    st.caption(f"Evaluation source: {', '.join(sources)}")
            if _ok_rows(load_quality_artifact.dataframe).empty:
                st.warning("Load-quality summary exists, but no successful rows were recorded.")
            st.dataframe(load_quality_artifact.dataframe, use_container_width=True)


def _render_benchmark(benchmark_artifact: SummaryArtifact) -> None:
    st.subheader("Benchmark")
    st.caption(_safe_relative_path(benchmark_artifact.path))
    df = benchmark_artifact.dataframe.copy()
    if df.empty:
        st.info("Benchmark summary is not available.")
        return

    df["rows_per_second"] = _coerce_numeric_column(df, "rows_per_second")
    df["total_seconds"] = _coerce_numeric_column(df, "total_seconds")
    best_row = df.sort_values("rows_per_second", ascending=False).iloc[0]

    cols = st.columns(3)
    _render_metric_row(cols[0], "Benchmark Runs", f"{len(df):,}")
    _render_metric_row(cols[1], "Best Throughput", f"{float(best_row['rows_per_second']):,.1f} rows/s")
    _render_metric_row(cols[2], "Best Run", str(best_row["run_id"]))

    chart_series = df.set_index("run_id")["rows_per_second"]
    st.bar_chart(chart_series)
    st.dataframe(df, use_container_width=True)


def _render_report(bundle: EvaluationBundle) -> None:
    st.subheader("Generated Evaluation Report")
    st.caption("Official evaluation report built from matrix summary artifacts. The current summary path is derived from ids.metrics, while Prometheus/Grafana remain the live telemetry view.")
    if bundle.report_md:
        st.caption(_safe_relative_path(bundle.report_md_path))
        st.markdown(bundle.report_md)
    else:
        st.info("Markdown report is not available for this environment.")

    if bundle.report_json:
        with st.expander("Structured Report JSON", expanded=False):
            st.json(bundle.report_json)


def main() -> None:
    st.set_page_config(page_title="Streaming Evaluation Dashboard", layout="wide")
    st.title("Streaming Evaluation Dashboard")
    st.caption(
        "Review official evaluation results generated from completed experiment summaries and consolidated reports."
    )

    with st.sidebar:
        default_environment = _default_environment()
        selected_environment = st.radio(
            "Summary Set",
            ["online", "scale_up"],
            index=0 if default_environment == "online" else 1,
        )
        if st.button("Refresh Data"):
            st.cache_data.clear()

    evaluation_bundle = load_evaluation_bundle(selected_environment)
    benchmark_artifact = load_benchmark_summary()
    profile_metadata = load_profile_metadata()

    if all(artifact.dataframe.empty for artifact in evaluation_bundle.summaries.values()) and benchmark_artifact.dataframe.empty:
        st.warning("No evaluation summaries were found under artifacts/streaming.")
        return

    overview_tab, layer_a_tab, layer_b_tab, layer_c_tab, quality_tab, benchmark_tab, report_tab = st.tabs(
        [
            "Overview",
            "Layer A",
            "Layer B",
            "Layer C",
            "Quality",
            "Benchmark",
            "Report",
        ]
    )

    with overview_tab:
        _render_overview(evaluation_bundle, benchmark_artifact)

    with layer_a_tab:
        _render_layer_a(evaluation_bundle, profile_metadata)

    with layer_b_tab:
        _render_layer_b(evaluation_bundle)

    with layer_c_tab:
        _render_layer_c(evaluation_bundle)

    with quality_tab:
        _render_quality(evaluation_bundle)

    with benchmark_tab:
        _render_benchmark(benchmark_artifact)

    with report_tab:
        _render_report(evaluation_bundle)


if __name__ == "__main__":
    main()
