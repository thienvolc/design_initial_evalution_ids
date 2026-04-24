"""Config loading: YAML/JSON I/O, feature registry, label mapping, preprocessing."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


def _ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


# ══════════════════════════════════════════════════════════════════════
# Generic I/O
# ══════════════════════════════════════════════════════════════════════


def load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Expected YAML mapping in {path}")
    return data


def write_yaml(path: Path, data: dict[str, Any]) -> None:
    _ensure_parent_dir(path)
    with path.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(data, fh, sort_keys=False, allow_unicode=True)


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def write_json(path: Path, data: dict[str, Any]) -> None:
    _ensure_parent_dir(path)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)


# ══════════════════════════════════════════════════════════════════════
# Feature registry
# ══════════════════════════════════════════════════════════════════════


def load_feature_list(path: Path) -> list[str]:
    payload = load_yaml(path)
    features = payload.get("keep_features") or []
    if not isinstance(features, list) or not features:
        raise ValueError(f"No keep_features in {path}")
    return [str(f).strip() for f in features]


# ══════════════════════════════════════════════════════════════════════
# Label mapping
# ══════════════════════════════════════════════════════════════════════


def load_label_mapping(path: Path) -> dict[str, str]:
    payload = load_yaml(path)
    mappings = payload.get("mappings") or []
    result: dict[str, str] = {}
    for item in mappings:
        if not isinstance(item, dict):
            continue
        raw = str(item.get("raw_label", "")).strip()
        norm = str(item.get("normalized_label", "")).strip()
        if raw and norm:
            result[raw] = norm
    if not result:
        raise ValueError(f"No valid mappings in {path}")
    return result


# ══════════════════════════════════════════════════════════════════════
# Preprocessing config  (expanded for sampling, feature selection, etc.)
# ══════════════════════════════════════════════════════════════════════


@dataclass(frozen=True)
class SamplingConfig:
    strategy: str = "class_weight"      # class_weight | undersample | smote
    undersample_ratio: float = 1.0      # target minority / majority
    smote_ratio: float = 0.5
    smote_max_rows: int = 2_000_000     # auto-subsample before SMOTE if larger


@dataclass(frozen=True)
class MemoryConfig:
    max_train_rows: int | None = None   # None = load all


@dataclass(frozen=True)
class FeatureSelectionConfig:
    enabled: bool = False
    strategy: str = "topk"            # topk | hybrid | manual
    method: str = "f_classif"           # f_classif | chi2 | mutual_info
    k: int = 20
    candidate_registry: str | None = None
    required_registry: str | None = None
    apply_feature_sets: tuple[str, ...] = ("reduced",)


@dataclass(frozen=True)
class ModelToggle:
    name: str
    enabled: bool
    params: dict[str, Any]


@dataclass(frozen=True)
class PreprocessingConfig:
    impute_strategy: str = "median"
    scale_enabled: bool = True
    sampling: SamplingConfig = SamplingConfig()
    memory: MemoryConfig = MemoryConfig()
    feature_selection: FeatureSelectionConfig = FeatureSelectionConfig()
    models: list[ModelToggle] = field(default_factory=list)

    @classmethod
    def from_yaml(cls, path: Path) -> PreprocessingConfig:
        """Build the preprocessing config from the YAML sections on disk."""
        data = load_yaml(path)

        imputer_config = data.get("imputer") or {}
        scaler_config = data.get("scaler") or {}
        sampling_config = data.get("sampling") or {}
        memory_config = data.get("memory") or {}
        feature_selection_config = data.get("feature_selection") or {}
        model_section = data.get("models") or {}

        models = _parse_model_toggles(model_section)

        return cls(
            impute_strategy=str(imputer_config.get("numeric_strategy", "median")),
            scale_enabled=bool(scaler_config.get("enabled", True)),
            sampling=SamplingConfig(
                strategy=str(sampling_config.get("strategy", "class_weight")),
                undersample_ratio=float(sampling_config.get("undersample_ratio", 1.0)),
                smote_ratio=float(sampling_config.get("smote_ratio", 0.5)),
                smote_max_rows=int(sampling_config.get("smote_max_rows", 2_000_000)),
            ),
            memory=MemoryConfig(
                max_train_rows=memory_config.get("max_train_rows"),
            ),
            feature_selection=FeatureSelectionConfig(
                enabled=bool(feature_selection_config.get("enabled", False)),
                strategy=str(feature_selection_config.get("strategy", "topk")),
                method=str(feature_selection_config.get("method", "f_classif")),
                k=int(feature_selection_config.get("k", 20)),
                candidate_registry=(
                    str(feature_selection_config.get("candidate_registry")).strip()
                    if feature_selection_config.get("candidate_registry") not in (None, "")
                    else None
                ),
                required_registry=(
                    str(feature_selection_config.get("required_registry")).strip()
                    if feature_selection_config.get("required_registry") not in (None, "")
                    else None
                ),
                apply_feature_sets=tuple(
                    str(item).strip()
                    for item in (feature_selection_config.get("apply_feature_sets") or ["reduced"])
                    if str(item).strip()
                ),
            ),
            models=models,
        )


def _parse_model_toggles(model_section: dict[str, Any]) -> list[ModelToggle]:
    """Parse model toggle config without mutating the loaded YAML payload."""

    models: list[ModelToggle] = []
    for name, raw_cfg in model_section.items():
        if isinstance(raw_cfg, dict):
            enabled = bool(raw_cfg.get("enabled", True))
            params = {k: v for k, v in raw_cfg.items() if k != "enabled"}
            models.append(ModelToggle(name=name, enabled=enabled, params=params))
        elif isinstance(raw_cfg, bool):
            models.append(ModelToggle(name=name, enabled=raw_cfg, params={}))
    return models
