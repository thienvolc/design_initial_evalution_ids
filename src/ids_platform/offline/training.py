"""Phase 03 – Train binary classifiers for scripts NIDS baseline.

Binary scripts baseline training:
  - Output serves online binary inference (benign vs attack).
  - Multiclass label is preserved for diagnostic / per-attack analysis only,
    NOT for multiclass online inference.

Architecture:
  Pass 1: stream FSEL_SAMPLE rows → feature selection (top-K or all)
  Pass 2: load selected features with proportional stratified subsample

Sampling: proportional subsample → class_weight in estimator (if config says so).
  No floor/undersample/SMOTE — training distribution matches production.

Threshold: maximise recall subject to FPR budget (default 5%).
  Reports operating points at FPR 1%, 3%, 5%, 10%.

Saves:
  artifacts/models/          – per-model .joblib + best_model.joblib
  artifacts/preprocessing/   – imputer.joblib, scaler.joblib, feature_manifest.json
  artifacts/models/          – valid_metrics.csv, best_model.json
"""

from __future__ import annotations

import gc
import shutil
import time as time_mod
from pathlib import Path
from typing import Optional

import joblib
import numpy as np
import pandas as pd
from pandas import DataFrame, Series
from sklearn.ensemble import HistGradientBoostingClassifier, RandomForestClassifier
from sklearn.feature_selection import (
    SelectKBest,
    VarianceThreshold,
    chi2,
    f_classif,
    mutual_info_classif,
)
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from ids_platform.offline.config import (
    FeatureSelectionConfig,
    PreprocessingConfig,
    load_feature_list,
    write_json,
)
from ids_platform.offline.log import get_logger
from ids_platform.offline.metrics import compute_binary_metrics, compute_per_attack_recall
from ids_platform.offline.paths import Paths

# ── knobs ────────────────────────────────────────────────────────────────
FSEL_SAMPLE = 400_000   # rows for feature selection (streamed, not all-at-once)
FPR_BUDGETS = [0.01, 0.03, 0.05, 0.10]  # operating points to evaluate
DEFAULT_FPR = 0.05      # primary FPR budget for threshold selection


# ══════════════════════════════════════════════════════════════════════
# 1. Data loading — proportional stratified subsample
# ══════════════════════════════════════════════════════════════════════


def _load_split(
    path: Path,
    features: list[str],
    max_rows: int | None,
    log,
    include_label: bool = False,
) -> tuple[DataFrame, Series, Optional[Series]]:
    """Load parquet with proportional stratified subsample.

    Preserves the natural class distribution (benign/attack ratio) so that
    class priors match production. class_weight in the estimator handles
    binary imbalance without distorting data.

    Returns (x, y, labels) where labels is multiclass label for diagnostics
    (None if include_label=False or column not present).
    """
    need_label = max_rows is not None or include_label
    cols = list(dict.fromkeys(
        [*features, "label_binary"] + (["label"] if need_label else [])
    ))
    df = pd.read_parquet(path, columns=cols)

    missing = [f for f in features if f not in df.columns]
    if missing:
        raise ValueError(f"Missing features in {path.name}: {missing}")

    n_total  = len(df)
    n_benign = int(df["label_binary"].eq(0).sum())
    n_attack = int(df["label_binary"].eq(1).sum())

    if max_rows is not None and n_total > max_rows:
        frac = max_rows / n_total
        parts = []
        for _, grp in df.groupby("label_binary"):
            n = max(1, int(len(grp) * frac))
            parts.append(grp.sample(n=n, random_state=42))
        df = pd.concat(parts, ignore_index=True)

        n_b = int(df["label_binary"].eq(0).sum())
        n_a = int(df["label_binary"].eq(1).sum())
        log.info(
            "  subsample %s: %d->%d | benign %d->%d (%.1f%%) | attack %d->%d (%.1f%%)",
            path.name, n_total, len(df),
            n_benign, n_b, 100 * n_b / len(df),
            n_attack, n_a, 100 * n_a / len(df),
        )
        if "label" in df.columns:
            for attack_label, count in df[df["label_binary"] == 1]["label"].value_counts().items():
                log.info("    %-40s %d", attack_label, count)
    else:
        log.info(
            "  loaded %s: %d rows | benign=%.1f%% | attack=%.1f%%",
            path.name, n_total,
            100 * n_benign / n_total if n_total else 0,
            100 * n_attack / n_total if n_total else 0,
        )

    labels: Optional[Series] = df["label"].copy() if ("label" in df.columns and include_label) else None
    df = df.drop(columns=["label"], errors="ignore")
    return df[features].copy(), df["label_binary"].astype(int), labels


# ══════════════════════════════════════════════════════════════════════
# 2. Sampling — no data resampling; class_weight optional by config
# ══════════════════════════════════════════════════════════════════════


def _apply_sampling(
    x_data,
    y_data,
    preprocessing_config: PreprocessingConfig,
    log,
):
    """No data resampling (no undersample/SMOTE).

    Data is never modified. Whether the estimator uses
    class_weight='balanced' is determined by config.sampling.strategy.
    If strategy='class_weight', estimators compensate for imbalance.
    Otherwise, training uses raw class distribution.
    """
    strategy = preprocessing_config.sampling.strategy
    if strategy == "class_weight":
        log.info("  sampling: class_weight=balanced (handled inside estimator, data unchanged)")
    else:
        log.info("  sampling: strategy=%s — no class_weight, raw distribution", strategy)
    return x_data, y_data


def _prediction_series_from_scores(score_series: Series, threshold: float) -> Series:
    predicted_labels = np.where(score_series.to_numpy() >= threshold, 1, 0)
    return pd.Series(predicted_labels, index=score_series.index)


# ══════════════════════════════════════════════════════════════════════
# 3. Feature selection (streamed sample, low-RAM)
# ══════════════════════════════════════════════════════════════════════

_SCORERS = {
    "f_classif":   f_classif,
    "chi2":        chi2,
    "mutual_info": mutual_info_classif,
}


def _resolve_registry_path(root: Path, raw_path: str | None, fallback: Path) -> Path:
    if not raw_path:
        return fallback
    candidate = Path(raw_path)
    return candidate if candidate.is_absolute() else (root / candidate)


def _should_apply_feature_selection(paths: Paths, config: FeatureSelectionConfig) -> bool:
    if not config.enabled:
        return False
    return paths.feature_set_name in set(config.apply_feature_sets)


def _merge_selected_features(
    *,
    strategy: str,
    ranked_selected_features: list[str],
    required_features: list[str],
    default_features: list[str],
    k: int,
    log,
) -> list[str]:
    normalized_strategy = strategy.strip().lower()

    if normalized_strategy == "manual":
        return list(default_features)

    if normalized_strategy == "topk":
        return list(ranked_selected_features)

    if normalized_strategy != "hybrid":
        log.warning("  unknown feature_selection.strategy='%s' -> using topk result", strategy)
        return list(ranked_selected_features)

    selected: list[str] = []
    seen: set[str] = set()

    for feature in required_features:
        if feature not in seen:
            selected.append(feature)
            seen.add(feature)

    target_count = max(int(k), len(selected))
    for feature in ranked_selected_features:
        if feature not in seen:
            selected.append(feature)
            seen.add(feature)
        if len(selected) >= target_count:
            break

    if len(required_features) > k:
        log.warning(
            "  hybrid feature selection kept %d required features although k=%d",
            len(required_features),
            k,
        )

    return selected


def _resolve_training_feature_lists(
    paths: Paths,
    preprocessing_config: PreprocessingConfig,
    log,
) -> tuple[list[str], list[str] | None, Path, Path | None]:
    base_registry_path = paths.feature_registry_path
    default_features = load_feature_list(base_registry_path)
    feature_selection_config = preprocessing_config.feature_selection

    if not _should_apply_feature_selection(paths, feature_selection_config):
        return default_features, None, base_registry_path, None

    candidate_registry_path = _resolve_registry_path(
        paths.root,
        feature_selection_config.candidate_registry,
        paths.root / "configs" / "modeling" / "feature_registry_full.yaml",
    )
    required_registry_path = _resolve_registry_path(
        paths.root,
        feature_selection_config.required_registry,
        base_registry_path,
    )

    candidate_features = load_feature_list(candidate_registry_path)
    required_features = load_feature_list(required_registry_path)

    candidate_set = set(candidate_features)
    required_features = [feature for feature in required_features if feature in candidate_set]

    log.info(
        "  feature selection active feature_set=%s strategy=%s candidates=%d required=%d",
        paths.feature_set_name,
        feature_selection_config.strategy,
        len(candidate_features),
        len(required_features),
    )

    return candidate_features, required_features, candidate_registry_path, required_registry_path


def _select_features_on_sample(
    train_path: Path,
    all_features: list[str],
    feature_selection_config: FeatureSelectionConfig,
    impute_strategy: str,
    log,
) -> tuple[list[str], list[tuple[str, float]]]:
    """Stream FSEL_SAMPLE rows → VarianceThreshold → SelectKBest.

    Returns (selected_features, ranking) where ranking is a list of
    (feature_name, score) sorted descending.
    """

    if not feature_selection_config.enabled:
        return all_features, []

    scorer = _SCORERS.get(feature_selection_config.method)
    if scorer is None:
        log.warning(
            "  unknown fsel method '%s' -> using all features",
            feature_selection_config.method,
        )
        return all_features, []

    import pyarrow.dataset as pa_ds

    log.info("  fsel: streaming %d rows ...", FSEL_SAMPLE)

    cols_to_read = list(dict.fromkeys([*all_features, "label_binary", "label"]))
    dataset = pa_ds.dataset(train_path, format="parquet")

    chunks: list[pd.DataFrame] = []
    collected = 0
    BATCH = 100_000

    dataset_scanner = pa_ds.Scanner.from_dataset(
        dataset=dataset,
        columns=cols_to_read,
        batch_size=BATCH,
    )
    for batch in dataset_scanner.to_batches():
        chunk = batch.to_pandas()
        chunks.append(chunk)
        collected += len(chunk)
        if collected >= FSEL_SAMPLE:
            break

    df_s = pd.concat(chunks, ignore_index=True)
    del chunks, dataset_scanner, dataset
    gc.collect()

    if len(df_s) > FSEL_SAMPLE:
        frac = FSEL_SAMPLE / len(df_s)
        parts = []
        for _, grp in df_s.groupby("label_binary"):
            n = max(1, int(len(grp) * frac))
            parts.append(grp.sample(n=min(n, len(grp)), random_state=42))
        df_s = pd.concat(parts, ignore_index=True)

    y_s = df_s["label_binary"].astype(int)
    x_s = df_s[[f for f in all_features if f in df_s.columns]].copy()
    all_features_present = list(x_s.columns)
    del df_s
    gc.collect()

    log.info("  fsel sample: %d rows  classes=%s",
             len(y_s), dict(y_s.value_counts().to_dict()))

    # step 1: drop constant features
    vt = VarianceThreshold(threshold=0.0)
    vt.fit(x_s.fillna(0))
    vt_mask_array = np.asarray(vt.get_support(), dtype=bool)
    vt_mask = [bool(flag) for flag in vt_mask_array]
    dropped = [c for c, m in zip(all_features_present, vt_mask) if not m]
    if dropped:
        log.info("  variance_threshold: dropped %d constant: %s", len(dropped), dropped)
    x_s = x_s.loc[:, vt_mask]
    remaining = list(x_s.columns)

    # step 2: impute
    imputer = SimpleImputer(strategy=impute_strategy)
    x_imp = pd.DataFrame(imputer.fit_transform(x_s), columns=remaining)
    del x_s
    gc.collect()

    # step 3: chi2 non-negative shift
    if feature_selection_config.method == "chi2":
        mn = x_imp.min()
        for col in mn[mn < 0].index:
            x_imp[col] += abs(float(mn[col])) + 1e-6

    # step 4: SelectKBest
    k = min(feature_selection_config.k, x_imp.shape[1])
    sel = SelectKBest(scorer, k=k)
    sel.fit(x_imp, y_s)

    mask_array = np.asarray(sel.get_support(), dtype=bool)
    mask = [bool(flag) for flag in mask_array.tolist()]
    selected = [c for c, m in zip(remaining, mask) if m]
    scores   = sel.scores_
    ranked   = sorted(zip(remaining, scores), key=lambda t: t[1], reverse=True)

    log.info("  fsel: %s top-%d -> %d selected (var-filter kept %d/%d)",
             feature_selection_config.method, k, len(selected), int(sum(vt_mask)), len(all_features_present))
    for feat, score in ranked[:15]:
        marker = "\u2713" if feat in selected else " "
        log.info("    [%s] %-35s score=%.4f", marker, feat, score)

    return selected, ranked


# ══════════════════════════════════════════════════════════════════════
# 4. Model building
# ══════════════════════════════════════════════════════════════════════


def _build_pipelines(
    preprocessing_config: PreprocessingConfig,
    use_class_weight: bool,
) -> dict[str, Pipeline]:
    pipelines: dict[str, Pipeline] = {}

    for toggle in preprocessing_config.models:
        if not toggle.enabled:
            continue

        steps: list = [("imputer", SimpleImputer(strategy=preprocessing_config.impute_strategy))]

        if toggle.name == "logistic_regression":
            if preprocessing_config.scale_enabled:
                steps.append(("scaler", StandardScaler()))
            steps.append(("model", LogisticRegression(
                max_iter=int(toggle.params.get("max_iter", 1000)),
                C=float(toggle.params.get("C", 1.0)),
                random_state=42,
                class_weight="balanced" if use_class_weight else None,
            )))

        elif toggle.name == "random_forest":
            steps.append(("model", RandomForestClassifier(
                n_estimators=int(toggle.params.get("n_estimators", 300)),
                max_depth=toggle.params.get("max_depth"),
                min_samples_leaf=int(toggle.params.get("min_samples_leaf", 2)),
                random_state=42,
                n_jobs=-1,
                class_weight="balanced_subsample" if use_class_weight else None,
            )))

        elif toggle.name == "gradient_boosting":
            steps.append(("model", HistGradientBoostingClassifier(
                max_iter=int(toggle.params.get("n_estimators", 300)),
                max_depth=int(toggle.params.get("max_depth", 8)),
                learning_rate=float(toggle.params.get("learning_rate", 0.05)),
                min_samples_leaf=int(toggle.params.get("min_samples_leaf", 20)),
                random_state=42,
                class_weight="balanced" if use_class_weight else None,
            )))
        else:
            continue

        pipelines[toggle.name] = Pipeline(steps)

    if not pipelines:
        raise ValueError("No models enabled in preprocessing.yaml")
    return pipelines


# ══════════════════════════════════════════════════════════════════════
# 5. Operating threshold — recall@FPR (NIDS objective)
# ══════════════════════════════════════════════════════════════════════


def _select_operating_threshold(
    pipe: Pipeline,
    x_valid: DataFrame,
    y_valid: Series,
    log,
    max_fpr: float = DEFAULT_FPR,
) -> tuple[float, dict]:
    """Maximise recall subject to FPR <= max_fpr.

    Returns (best_threshold, operating_points) where operating_points is a
    dict keyed by FPR budget with metrics at each operating point, plus
    the default 0.5 threshold for comparison.
    """

    if not hasattr(pipe, "predict_proba"):
        return 0.5, {}

    proba = pipe.predict_proba(x_valid)[:, 1]
    n_neg = int(y_valid.eq(0).sum())
    n_pos = int(y_valid.eq(1).sum())

    # compute metrics at every threshold
    curve = []
    for thr in np.linspace(0.001, 0.999, 999):
        preds = pd.Series(np.where(np.asarray(proba) >= thr, 1, 0), index=y_valid.index)
        tp = int((preds.eq(1) & y_valid.eq(1)).sum())
        fp = int((preds.eq(1) & y_valid.eq(0)).sum())
        fn = int((preds.eq(0) & y_valid.eq(1)).sum())
        fpr    = fp / n_neg if n_neg else 0
        recall = tp / (tp + fn) if (tp + fn) else 0
        prec   = tp / (tp + fp) if (tp + fp) else 0
        f1     = 2 * prec * recall / (prec + recall) if (prec + recall) else 0
        curve.append((float(thr), recall, fpr, prec, f1))

    # operating points at multiple FPR budgets
    log.info("  -- threshold sweep (n_pos=%d, n_neg=%d) --", n_pos, n_neg)
    best_recall, best_thr = 0.0, 0.5
    operating_points: dict = {}

    for budget in FPR_BUDGETS:
        br, bt = 0.0, 0.5
        for thr, rec, fpr, pre, f1 in curve:
            if fpr <= budget and rec > br:
                br, bt = rec, thr
        # get exact metrics at that threshold
        match = min(curve, key=lambda x: abs(x[0] - bt))
        point = {
            "threshold": round(bt, 4),
            "recall": round(match[1], 6),
            "fpr": round(match[2], 6),
            "precision": round(match[3], 6),
            "f1": round(match[4], 6),
        }
        operating_points[f"fpr_{int(budget*100)}pct"] = point

        marker = " <<" if abs(budget - max_fpr) < 0.001 else ""
        log.info("    FPR<=%.0f%%: thr=%.3f  recall=%.4f  prec=%.4f  f1=%.4f%s",
                 budget * 100, bt, point["recall"], point["precision"], point["f1"], marker)

        if abs(budget - max_fpr) < 0.001:
            best_recall, best_thr = br, bt

    # default 0.5
    d = min(curve, key=lambda x: abs(x[0] - 0.5))
    operating_points["default_0.5"] = {
        "threshold": 0.5,
        "recall": round(d[1], 6), "fpr": round(d[2], 6),
        "precision": round(d[3], 6), "f1": round(d[4], 6),
    }
    log.info("    default(0.5): recall=%.4f  prec=%.4f  fpr=%.4f  f1=%.4f",
             d[1], d[3], d[2], d[4])

    return best_thr, operating_points


def _save_feature_ranking(
    preprocessing_dir: Path,
    feature_ranking: list[tuple[str, float]],
    selected_features: list[str],
    output_name: str,
    log,
) -> None:
    if not feature_ranking:
        return

    ranking_df = pd.DataFrame(feature_ranking, columns=["feature", "score"])
    ranking_df["rank"] = range(1, len(ranking_df) + 1)
    ranking_df["selected"] = ranking_df["feature"].isin(selected_features)
    ranking_df.to_csv(preprocessing_dir / output_name, index=False)
    log.info("  feature ranking saved (%d features)", len(ranking_df))


def _save_operating_points(models_dir: Path, results: list[dict], *, filename_suffix: str = "") -> None:
    for result in results:
        write_json(
            models_dir / f"operating_points_{result['model']}{filename_suffix}.json",
            {
                "model": result["model"],
                "opt_threshold": result["opt_threshold"],
                "fpr_budget": DEFAULT_FPR,
                "operating_points": result["operating_points"],
            },
        )


def _save_preprocessor_artifacts(best_pipe: Pipeline, paths: Paths) -> None:
    if "imputer" in best_pipe.named_steps:
        joblib.dump(
            best_pipe.named_steps["imputer"],
            paths.preprocessing_dir / paths.suffixed_name("imputer.joblib"),
        )
    if "scaler" in best_pipe.named_steps:
        joblib.dump(
            best_pipe.named_steps["scaler"],
            paths.preprocessing_dir / paths.suffixed_name("scaler.joblib"),
        )


def _log_per_attack_recall_rows(
    per_attack_rows: list[dict],
    *,
    split_name: str,
    log,
) -> None:
    for row in per_attack_rows:
        log.info(
            "    [%s] %-35s recall=%.4f (%d/%d)",
            split_name,
            row["attack_type"],
            row["recall"],
            row["detected"],
            row["n_samples"],
        )


def _build_validation_result_row(
    *,
    model_name: str,
    validation_size: int,
    use_class_weight: bool,
    opt_threshold: float,
    final_metrics: dict,
    default_metrics: dict,
    train_time: float,
    valid_infer_time: float,
    operating_points: dict,
) -> dict:
    return {
        "model": model_name,
        "split": "valid",
        "n": validation_size,
        "class_weight": use_class_weight,
        "opt_threshold": opt_threshold,
        "fpr_budget": DEFAULT_FPR,
        **final_metrics,
        "default_threshold": 0.5,
        "default_recall": default_metrics["recall"],
        "default_precision": default_metrics["precision"],
        "default_f1": default_metrics["f1"],
        "default_fpr": default_metrics["fpr"],
        "train_time_s": round(train_time, 2),
        "valid_infer_time_s": round(valid_infer_time, 2),
        "valid_rows_per_sec": round(validation_size / valid_infer_time, 0) if valid_infer_time > 0 else 0,
        "operating_points": operating_points,
    }


def _build_feature_manifest(
    *,
    best_pipe: Pipeline,
    all_features: list[str],
    selected_features: list[str],
    feature_selection_applied: bool,
    feature_selection_strategy: str | None,
    candidate_registry_path: Path | None,
    required_registry_path: Path | None,
    best_model_name: str,
    best_model_path: Path,
    best_threshold: float,
    preprocessing_config: PreprocessingConfig,
    use_class_weight: bool,
) -> dict:
    imputer = best_pipe.named_steps.get("imputer")
    manifest: dict = {
        "version": 5,
        "target_column": "label_binary",
        "evaluation_scope": {
            "online_task": "binary_detection",
            "offline_analysis": ["per_attack_recall", "attack_family_breakdown"],
        },
        "registry_features": all_features,
        "feature_columns": selected_features,
        "n_features": len(selected_features),
        "opt_threshold": best_threshold,
        "fpr_budget": DEFAULT_FPR,
        "feature_selection": {
            "enabled": feature_selection_applied,
            "strategy": feature_selection_strategy,
            "method": (
                preprocessing_config.feature_selection.method
                if feature_selection_applied
                else None
            ),
            "k": (
                preprocessing_config.feature_selection.k
                if feature_selection_applied
                else None
            ),
            "candidate_registry": (
                str(candidate_registry_path) if candidate_registry_path is not None else None
            ),
            "required_registry": (
                str(required_registry_path) if required_registry_path is not None else None
            ),
        },
        "sampling": {
            "strategy": preprocessing_config.sampling.strategy,
            "class_weight_used": use_class_weight,
        },
        "impute_strategy": preprocessing_config.impute_strategy,
        "scale_enabled": preprocessing_config.scale_enabled,
        "best_model": best_model_name,
        "best_model_path": str(best_model_path),
    }
    if imputer is not None and hasattr(imputer, "statistics_"):
        manifest["imputer_fill_values"] = {
            feature: round(float(value), 8)
            for feature, value in zip(selected_features, imputer.statistics_)
        }
    return manifest


def _build_best_model_metadata(
    *,
    best_model_name: str,
    best_model_path: Path,
    best_threshold: float,
    best_row: dict,
    operating_points: dict,
    use_class_weight: bool,
) -> dict:
    return {
        "best_model": best_model_name,
        "model_path": str(best_model_path),
        "opt_threshold": best_threshold,
        "fpr_budget": DEFAULT_FPR,
        "selection_metric": f"recall@FPR<={int(DEFAULT_FPR*100)}%",
        "class_weight": use_class_weight,
        "valid_metrics": {
            key: float(best_row[key])
            for key in ("accuracy", "precision", "recall", "f1", "fpr", "fnr")
            if key in best_row
        },
        "default_metrics": {
            key: float(best_row[f"default_{key}"])
            for key in ("recall", "precision", "f1", "fpr")
            if f"default_{key}" in best_row
        },
        "operating_points": operating_points,
        "timing": {
            "train_time_s": float(best_row.get("train_time_s", 0)),
            "valid_infer_time_s": float(best_row.get("valid_infer_time_s", 0)),
            "valid_rows_per_sec": float(best_row.get("valid_rows_per_sec", 0)),
        },
    }


# ══════════════════════════════════════════════════════════════════════
# 6. Public API
# ══════════════════════════════════════════════════════════════════════


def run(paths: Paths) -> None:
    log = get_logger("phase03", paths.log_dir / "phase03_train.log")
    paths.ensure_dirs()

    if not paths.train_path.exists() or not paths.valid_path.exists():
        raise FileNotFoundError("Missing splits -- run Phase 02 first")

    preprocessing_config = PreprocessingConfig.from_yaml(paths.preprocessing_path)
    all_features, required_features, candidate_registry_path, required_registry_path = _resolve_training_feature_lists(
        paths,
        preprocessing_config,
        log,
    )
    max_rows = preprocessing_config.memory.max_train_rows

    # ── Pass 1: feature selection on streamed sample ─────────────────
    ranked_selected_features, feature_ranking = _select_features_on_sample(
        paths.train_path,
        all_features,
        preprocessing_config.feature_selection,
        preprocessing_config.impute_strategy,
        log,
    )
    selected_features = _merge_selected_features(
        strategy=preprocessing_config.feature_selection.strategy,
        ranked_selected_features=ranked_selected_features,
        required_features=required_features or [],
        default_features=all_features,
        k=preprocessing_config.feature_selection.k,
        log=log,
    )
    gc.collect()

    # save feature ranking (for report / ablation analysis)
    _save_feature_ranking(
        paths.preprocessing_dir,
        feature_ranking,
        selected_features,
        paths.suffixed_name("feature_ranking.csv"),
        log,
    )

    # ── Pass 2: load train/valid with selected features ──────────────
    log.info("  loading train with %d features (max_rows=%s) ...",
             len(selected_features), max_rows or "all")
    x_train, y_train, _ = _load_split(paths.train_path, selected_features, max_rows, log)

    log.info("  loading valid (no cap, include_label for diagnostics) ...")
    x_valid, y_valid, labels_valid = _load_split(
        paths.valid_path, selected_features, None, log, include_label=True,
    )
    gc.collect()

    log.info(
        "START  features=%d  train=%d  valid=%d  max_rows=%s",
        len(selected_features), len(y_train), len(y_valid), max_rows or "all",
    )

    # ── 3. Sampling (class_weight only) ──────────────────────────────
    x_train, y_train = _apply_sampling(x_train, y_train, preprocessing_config, log)

    # ── 4. Train + evaluate models ───────────────────────────────────
    use_class_weight = preprocessing_config.sampling.strategy == "class_weight"
    results: list[dict] = []
    saved_model_paths: dict[str, Path] = {}
    all_per_attack_rows: list[dict] = []

    for name, pipe in _build_pipelines(
        preprocessing_config,
        use_class_weight=use_class_weight,
    ).items():
        log.info("  training %-25s ...", name)

        t0 = time_mod.perf_counter()
        pipe.fit(x_train, y_train)
        train_time = time_mod.perf_counter() - t0

        t1 = time_mod.perf_counter()
        preds = pd.Series(pipe.predict(x_valid), index=y_valid.index)
        scores: Optional[Series] = None
        opt_preds: Optional[Series] = None
        if hasattr(pipe, "predict_proba"):
            scores = pd.Series(pipe.predict_proba(x_valid)[:, 1], index=y_valid.index)
        valid_infer_time = time_mod.perf_counter() - t1

        default_metrics, _ = compute_binary_metrics(y_valid, preds, scores)

        # operating threshold (recall@FPR)
        opt_thr, operating_points = _select_operating_threshold(
            pipe,
            x_valid,
            y_valid,
            log,
        )

        if scores is not None:
            opt_preds = _prediction_series_from_scores(scores, opt_thr)
            opt_metrics, _ = compute_binary_metrics(y_valid, opt_preds, scores)
            final_metrics = opt_metrics
        else:
            final_metrics = default_metrics

        # per-attack recall diagnostic (on valid)
        per_attack_rows: list[dict] = []
        if labels_valid is not None and opt_preds is not None:
            per_attack_rows = compute_per_attack_recall(
                y_valid,
                opt_preds,
                labels_valid,
                threshold=opt_thr,
                model_name=name,
            )
            _log_per_attack_recall_rows(per_attack_rows, split_name="valid", log=log)

        log.info(
            "  %-25s  f1=%.4f  recall=%.4f  prec=%.4f  fpr=%.4f  thr=%.2f  "
            "train=%.1fs  infer=%.1fs",
            name, final_metrics["f1"], final_metrics["recall"],
            final_metrics["precision"], final_metrics["fpr"], opt_thr,
            train_time, valid_infer_time,
        )

        model_path = paths.models_dir / paths.suffixed_name(f"{name}.joblib")
        joblib.dump(pipe, model_path)
        saved_model_paths[name] = model_path

        results.append(
            _build_validation_result_row(
                model_name=name,
                validation_size=len(y_valid),
                use_class_weight=use_class_weight,
                opt_threshold=opt_thr,
                final_metrics=final_metrics,
                default_metrics=default_metrics,
                train_time=train_time,
                valid_infer_time=valid_infer_time,
                operating_points=operating_points,
            )
        )
        all_per_attack_rows.extend(per_attack_rows)

    # ── Save per-attack recall CSV ───────────────────────────────────
    if all_per_attack_rows:
        per_attack_df = pd.DataFrame(all_per_attack_rows)
        per_attack_df.to_csv(
            paths.models_dir / paths.suffixed_name("valid_per_attack_recall.csv"),
            index=False,
        )
        log.info("  per-attack recall saved (%d rows)", len(per_attack_df))

    # ── Persist per-model operating points for Phase 04 ──────────────
    _save_operating_points(paths.models_dir, results, filename_suffix=paths.feature_set_suffix)

    # ── 5. Pick best ─────────────────────────────────────────────────
    # flatten operating_points for CSV (store as separate JSON)
    csv_rows = []
    for r in results:
        row = {k: v for k, v in r.items() if k != "operating_points"}
        csv_rows.append(row)
    validation_metrics_df = pd.DataFrame(csv_rows).sort_values(
        ["recall", "fpr"], ascending=[False, True],
    )
    validation_metrics_df.to_csv(
        paths.models_dir / paths.suffixed_name("valid_metrics.csv"),
        index=False,
    )

    best_name = str(validation_metrics_df.iloc[0]["model"])
    best_thr = float(validation_metrics_df.iloc[0].get("opt_threshold", 0.5))
    best_src = saved_model_paths[best_name]
    best_dst = paths.models_dir / paths.suffixed_name("best_model.joblib")
    shutil.copyfile(best_src, best_dst)

    # get operating points for best model
    best_operating_points = next(
        result["operating_points"]
        for result in results
        if result["model"] == best_name
    )

    # ── 6. Export preprocessor artifacts ─────────────────────────────
    best_pipe = joblib.load(best_dst)
    _save_preprocessor_artifacts(best_pipe, paths)

    # ── 7. Feature manifest ──────────────────────────────────────────
    manifest = _build_feature_manifest(
        best_pipe=best_pipe,
        all_features=all_features,
        selected_features=selected_features,
        feature_selection_applied=_should_apply_feature_selection(paths, preprocessing_config.feature_selection),
        feature_selection_strategy=(
            preprocessing_config.feature_selection.strategy
            if _should_apply_feature_selection(paths, preprocessing_config.feature_selection)
            else None
        ),
        candidate_registry_path=candidate_registry_path,
        required_registry_path=required_registry_path,
        best_model_name=best_name,
        best_model_path=best_dst,
        best_threshold=best_thr,
        preprocessing_config=preprocessing_config,
        use_class_weight=use_class_weight,
    )
    write_json(
        paths.preprocessing_dir / paths.suffixed_name("feature_manifest.json"),
        manifest,
    )

    # ── 8. Best model meta ───────────────────────────────────────────
    best_row = validation_metrics_df.iloc[0].to_dict()
    write_json(
        paths.models_dir / paths.suffixed_name("best_model.json"),
        _build_best_model_metadata(
            best_model_name=best_name,
            best_model_path=best_dst,
            best_threshold=best_thr,
            best_row=best_row,
            operating_points=best_operating_points,
            use_class_weight=use_class_weight,
        ),
    )

    log.info(
        "DONE   best=%s  thr=%.3f  features=%d  class_weight=%s",
        best_name, best_thr, len(selected_features), use_class_weight,
    )
