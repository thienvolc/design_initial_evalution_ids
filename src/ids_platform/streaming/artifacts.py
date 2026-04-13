from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from ids_platform.common.paths import PROJECT_ROOT, resolve_project_path


def load_thresholds(valid_metrics_csv: Path) -> dict[str, float]:
    if not valid_metrics_csv.exists():
        return {}
    df = pd.read_csv(valid_metrics_csv)
    if "model" not in df.columns or "opt_threshold" not in df.columns:
        return {}

    thresholds: dict[str, float] = {}
    for _, row in df.iterrows():
        try:
            thresholds[str(row["model"]).strip()] = float(row["opt_threshold"])
        except (TypeError, ValueError):
            continue
    return thresholds


def resolve_feature_set_path(
    paths_cfg: dict[str, Any],
    key: str,
    feature_set: str,
    *,
    project_root: Path = PROJECT_ROOT,
) -> Path:
    mapping = paths_cfg.get(f"{key}_by_feature_set")
    if isinstance(mapping, dict):
        raw = mapping.get(feature_set)
        if raw:
            return resolve_project_path(str(raw), project_root)
        raise ValueError(
            f"Missing paths.{key}_by_feature_set.{feature_set} for feature_set={feature_set}"
        )

    if feature_set != "full":
        raise ValueError(
            f"feature_set={feature_set} requires paths.{key}_by_feature_set.{feature_set} "
            f"to avoid mixing artifacts across feature sets"
        )

    return resolve_project_path(str(paths_cfg.get(key, "")), project_root)


def resolve_model_artifact_path(
    model_cfg: dict[str, Any],
    feature_set: str,
    *,
    project_root: Path = PROJECT_ROOT,
) -> Path:
    mapping = model_cfg.get("joblib_path_by_feature_set")
    if isinstance(mapping, dict):
        raw = mapping.get(feature_set)
        if raw:
            return resolve_project_path(str(raw), project_root)
        raise ValueError(
            f"Missing models[].joblib_path_by_feature_set.{feature_set} for model={model_cfg.get('name', '')}"
        )

    if feature_set != "full":
        raise ValueError(
            f"feature_set={feature_set} requires models[].joblib_path_by_feature_set.{feature_set} "
            f"to avoid mixing artifacts across feature sets"
        )

    return resolve_project_path(str(model_cfg.get("joblib_path")), project_root)
