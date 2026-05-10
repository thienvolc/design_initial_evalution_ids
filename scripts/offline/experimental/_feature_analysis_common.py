from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from pandas import DataFrame, Series

from _common import ensure_src_on_path, project_path

ensure_src_on_path()

from ids_platform.offline.config import PreprocessingConfig, load_feature_list  # noqa: E402
from ids_platform.offline.log import get_logger  # noqa: E402
from ids_platform.offline.metrics import compute_binary_metrics, compute_per_attack_recall  # noqa: E402
from ids_platform.offline.paths import Paths  # noqa: E402
from ids_platform.offline.training import (  # noqa: E402
    _build_pipelines,
    _load_split,
    _merge_selected_features,
    _resolve_effective_max_rows,
    _resolve_training_feature_lists,
    _select_features_on_sample,
    _select_operating_threshold,
)


DEFAULT_FPR_BUDGETS = (0.01, 0.03, 0.05, 0.10)


def build_offline_paths(feature_set: str = "reduced") -> Paths:
    offline_dir = project_path("src", "ids_platform", "offline")
    return Paths.build(offline_dir, feature_set=feature_set)


def load_training_context(feature_set: str = "reduced"):
    paths = build_offline_paths(feature_set=feature_set)
    preprocessing_config = PreprocessingConfig.from_yaml(paths.preprocessing_path)
    log = get_logger("offline_feature_analysis", paths.log_dir / "feature_analysis.log")

    candidate_features, required_features, candidate_registry_path, required_registry_path = _resolve_training_feature_lists(
        paths,
        preprocessing_config,
        log,
    )
    max_rows = _resolve_effective_max_rows(
        preprocessing_config.memory.max_train_rows,
        len(candidate_features),
        preprocessing_config.memory.max_train_feature_cells,
        log,
    )
    use_class_weight = preprocessing_config.sampling.strategy == "class_weight"
    return {
        "paths": paths,
        "preprocessing_config": preprocessing_config,
        "log": log,
        "candidate_features": candidate_features,
        "required_features": required_features or [],
        "candidate_registry_path": candidate_registry_path,
        "required_registry_path": required_registry_path,
        "max_rows": max_rows,
        "use_class_weight": use_class_weight,
    }


def load_train_valid_test(
    *,
    paths: Paths,
    features: list[str],
    max_rows: Optional[int],
    log,
) -> tuple[DataFrame, Series, DataFrame, Series, Optional[Series], DataFrame, Series, Optional[Series]]:
    x_train, y_train, _ = _load_split(paths.train_path, features, max_rows, log)
    x_valid, y_valid, labels_valid = _load_split(paths.valid_path, features, None, log, include_label=True)
    x_test, y_test, labels_test = _load_split(paths.test_path, features, None, log, include_label=True)
    return x_train, y_train, x_valid, y_valid, labels_valid, x_test, y_test, labels_test


def compute_feature_ranking(
    *,
    train_path: Path,
    candidate_features: list[str],
    preprocessing_config: PreprocessingConfig,
    method: str,
    log,
) -> list[tuple[str, float]]:
    full_rank_cfg = replace(
        preprocessing_config.feature_selection,
        enabled=True,
        strategy="topk",
        method=method,
        k=len(candidate_features),
    )
    _, ranking = _select_features_on_sample(
        train_path,
        candidate_features,
        full_rank_cfg,
        preprocessing_config.impute_strategy,
        log,
    )
    return ranking


def select_features_from_ranking(
    *,
    ranking: list[tuple[str, float]],
    required_features: list[str],
    candidate_features: list[str],
    strategy: str,
    k: int,
    log,
) -> list[str]:
    ranked_features = [feature for feature, _ in ranking]
    normalized_strategy = strategy.strip().lower()

    if normalized_strategy == "topk":
        return list(ranked_features[: min(int(k), len(ranked_features))])

    if normalized_strategy == "manual":
        return list(candidate_features)

    return _merge_selected_features(
        strategy=strategy,
        ranked_selected_features=ranked_features,
        required_features=required_features,
        default_features=candidate_features,
        k=k,
        log=log,
    )


def build_pipeline_for_model(
    *,
    preprocessing_config: PreprocessingConfig,
    model_name: str,
    use_class_weight: bool,
):
    selected_config = preprocessing_config.with_selected_models([model_name])
    pipelines = _build_pipelines(selected_config, use_class_weight=use_class_weight)
    return pipelines[model_name]


def evaluate_model_with_thresholds(
    *,
    pipe,
    model_name: str,
    selected_features: list[str],
    feature_set_label: str,
    x_valid: DataFrame,
    y_valid: Series,
    labels_valid: Optional[Series],
    x_test: DataFrame,
    y_test: Series,
    labels_test: Optional[Series],
    budgets: tuple[float, ...] = DEFAULT_FPR_BUDGETS,
) -> tuple[list[dict], list[dict]]:
    results: list[dict] = []
    per_attack_rows: list[dict] = []

    opt_thr, _ = _select_operating_threshold(pipe, x_valid, y_valid, get_logger("threshold_eval", project_path("logs", "offline", "feature_threshold_eval.log")))

    valid_scores = pd.Series(pipe.predict_proba(x_valid)[:, 1], index=y_valid.index)
    test_scores = pd.Series(pipe.predict_proba(x_test)[:, 1], index=y_test.index)

    for budget in budgets:
        threshold, _ = _select_operating_threshold(pipe, x_valid, y_valid, get_logger("threshold_eval", project_path("logs", "offline", "feature_threshold_eval.log")), max_fpr=budget)
        valid_preds = pd.Series(np.where(valid_scores.to_numpy() >= threshold, 1, 0), index=y_valid.index)
        test_preds = pd.Series(np.where(test_scores.to_numpy() >= threshold, 1, 0), index=y_test.index)

        valid_metrics, _ = compute_binary_metrics(y_valid, valid_preds, valid_scores)
        test_metrics, _ = compute_binary_metrics(y_test, test_preds, test_scores)

        results.append(
            {
                "model": model_name,
                "feature_set_label": feature_set_label,
                "n_features": len(selected_features),
                "fpr_budget": budget,
                "threshold": round(float(threshold), 6),
                "valid_precision": valid_metrics["precision"],
                "valid_recall": valid_metrics["recall"],
                "valid_f1": valid_metrics["f1"],
                "valid_fpr": valid_metrics["fpr"],
                "valid_fnr": valid_metrics["fnr"],
                "test_precision": test_metrics["precision"],
                "test_recall": test_metrics["recall"],
                "test_f1": test_metrics["f1"],
                "test_fpr": test_metrics["fpr"],
                "test_fnr": test_metrics["fnr"],
                "opt_threshold_at_5pct": round(float(opt_thr), 6),
            }
        )

        if labels_test is not None:
            attack_rows = compute_per_attack_recall(
                y_test,
                test_preds,
                labels_test,
                threshold=threshold,
                model_name=model_name,
            )
            for row in attack_rows:
                row_copy = dict(row)
                row_copy["feature_set_label"] = feature_set_label
                row_copy["n_features"] = len(selected_features)
                row_copy["fpr_budget"] = budget
                per_attack_rows.append(row_copy)

    return results, per_attack_rows


def save_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


def stratified_subsample(
    *,
    x_data: DataFrame,
    y_data: Series,
    target_rows: int,
    random_seed: int = 42,
) -> tuple[DataFrame, Series]:
    if target_rows <= 0 or len(y_data) <= target_rows:
        return x_data, y_data

    frame = x_data.copy()
    frame["__label_binary__"] = y_data.to_numpy()

    frac = target_rows / len(frame)
    parts: list[DataFrame] = []
    for label_value, group_df in frame.groupby("__label_binary__"):
        target_count = max(1, int(len(group_df) * frac))
        if len(group_df) > target_count:
            group_df = group_df.sample(n=target_count, random_state=random_seed + int(label_value))
        parts.append(group_df)

    sampled = pd.concat(parts, ignore_index=False)
    if len(sampled) > target_rows:
        sampled = sampled.sample(n=target_rows, random_state=random_seed)

    sampled = sampled.sort_index()
    y_sampled = sampled["__label_binary__"].astype(y_data.dtype, copy=False)
    x_sampled = sampled.drop(columns=["__label_binary__"])
    return x_sampled, y_sampled


def resolve_effective_train_rows_for_features(
    *,
    preprocessing_config: PreprocessingConfig,
    n_features: int,
    loaded_rows: int,
    log,
) -> int:
    effective_rows = _resolve_effective_max_rows(
        preprocessing_config.memory.max_train_rows,
        n_features,
        preprocessing_config.memory.max_train_feature_cells,
        log,
    )
    if effective_rows is None:
        return int(loaded_rows)
    return min(int(loaded_rows), int(effective_rows))


def fit_with_config_capped_rows(
    *,
    pipe,
    x_train: DataFrame,
    y_train: Series,
    preprocessing_config: PreprocessingConfig,
    n_features: int,
    log,
    random_seed: int = 42,
) -> tuple[float, int]:
    target_rows = resolve_effective_train_rows_for_features(
        preprocessing_config=preprocessing_config,
        n_features=n_features,
        loaded_rows=len(y_train),
        log=log,
    )
    x_fit, y_fit = stratified_subsample(
        x_data=x_train,
        y_data=y_train,
        target_rows=target_rows,
        random_seed=random_seed,
    )
    started = pd.Timestamp.utcnow()
    pipe.fit(x_fit, y_fit)
    elapsed = (pd.Timestamp.utcnow() - started).total_seconds()
    return elapsed, len(y_fit)


def default_run_tag(prefix: str) -> str:
    return f"{prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"


def build_output_path(
    *,
    base_filename: str,
    run_tag: str,
    overwrite: bool,
) -> Path:
    base_path = project_path("artifacts", "offline", "evaluation", base_filename)
    if overwrite:
        return base_path

    stem = base_path.stem
    suffix = "".join(base_path.suffixes)
    return base_path.with_name(f"{stem}_{run_tag}{suffix}")


def resolve_current_reduced_features(*, paths: Paths, preprocessing_config: PreprocessingConfig, log) -> list[str]:
    manifest_path = paths.preprocessing_dir / "feature_manifest_reduced.json"
    if manifest_path.exists():
        import json

        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        feature_columns = payload.get("feature_columns") or []
        if feature_columns:
            return [str(item).strip() for item in feature_columns if str(item).strip()]

    candidate_features, required_features, _, _ = _resolve_training_feature_lists(paths, preprocessing_config, log)
    ranking = compute_feature_ranking(
        train_path=paths.train_path,
        candidate_features=candidate_features,
        preprocessing_config=preprocessing_config,
        method=preprocessing_config.feature_selection.method,
        log=log,
    )
    return select_features_from_ranking(
        ranking=ranking,
        required_features=required_features or [],
        candidate_features=candidate_features,
        strategy=preprocessing_config.feature_selection.strategy,
        k=preprocessing_config.feature_selection.k,
        log=log,
    )


def resolve_full_features() -> list[str]:
    return load_feature_list(project_path("configs", "modeling", "feature_registry_full.yaml"))


def validate_feature_set_size(*, label: str, expected_k: int | None, selected_features: list[str]) -> None:
    if expected_k is None:
        return
    if len(selected_features) != int(expected_k):
        raise ValueError(
            f"Feature set '{label}' expected {int(expected_k)} features but got {len(selected_features)}"
        )
