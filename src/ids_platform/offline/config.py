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
# Preprocessing config
# ══════════════════════════════════════════════════════════════════════


@dataclass(frozen=True)
class SamplingConfig:
    strategy: str = "class_weight"


@dataclass(frozen=True)
class ModelToggle:
    name: str
    enabled: bool
    params: dict[str, Any]


@dataclass(frozen=True)
class PreprocessingConfig:
    impute_strategy: str = "median"
    sampling: SamplingConfig = SamplingConfig()
    models: list[ModelToggle] = field(default_factory=list)

    def with_selected_models(self, selected_models: list[str] | tuple[str, ...] | None) -> PreprocessingConfig:
        if not selected_models:
            return self

        requested = {str(name).strip() for name in selected_models if str(name).strip()}
        if not requested:
            return self

        available = {toggle.name for toggle in self.models}
        unknown = sorted(requested - available)
        if unknown:
            raise ValueError(f"Unknown model(s): {unknown} (available: {sorted(available)})")

        filtered_models = [
            ModelToggle(name=toggle.name, enabled=(toggle.enabled and toggle.name in requested), params=toggle.params)
            for toggle in self.models
        ]
        return PreprocessingConfig(
            impute_strategy=self.impute_strategy,
            sampling=self.sampling,
            models=filtered_models,
        )

    @classmethod
    def from_yaml(cls, path: Path) -> PreprocessingConfig:
        """Build the preprocessing config from the YAML sections on disk."""
        data = load_yaml(path)

        imputer_config = data.get("imputer") or {}
        sampling_config = data.get("sampling") or {}
        model_section = data.get("models") or {}

        models = _parse_model_toggles(model_section)

        return cls(
            impute_strategy=str(imputer_config.get("numeric_strategy", "median")),
            sampling=SamplingConfig(
                strategy=str(sampling_config.get("strategy", "class_weight")),
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
