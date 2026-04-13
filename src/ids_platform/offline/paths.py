"""Centralized path management for the offline pipeline.

Directory layout (supported):
    1) legacy
         project_root/offline/
    2) pre-refactor src layout
         project_root/src/offline/
    3) current package layout
         project_root/src/ids_platform/offline/

    In all layouts, shared data/configs/artifacts stay under project_root/
    and logs stay under project_root/logs/offline/.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Paths:
    """Every file/directory the pipeline reads or writes."""

    root: Path          # project root (parent of offline/)

    # ── data ────────────────────────────────────────────
    raw_dir: Path
    silver_path: Path
    split_dir: Path
    train_path: Path
    valid_path: Path
    test_path: Path

    # ── configs ─────────────────────────────────────────
    feature_registry_path: Path
    preprocessing_path: Path
    label_mapping_path: Path

    # ── artifacts (under artifacts/offline/) ────────────
    models_dir: Path
    preprocessing_dir: Path
    evaluation_dir: Path

    # ── logs ────────────────────────────────────────────
    log_dir: Path

    # ------------------------------------------------------------------
    @classmethod
    def build(cls, offline_dir: Path, *, feature_set: str = "reduced") -> Paths:
        """Build paths.

        Args:
            offline_dir: the offline/ directory (where run_pipeline.py lives).
            feature_set: 'reduced' (17 features) or 'full' (78 features).
        """
        offline_dir = offline_dir.resolve()

        # Support these layouts:
        # - project_root/offline
        # - project_root/src/offline
        # - project_root/src/ids_platform/offline
        if (
            offline_dir.parent.name == "ids_platform"
            and offline_dir.parent.parent.name == "src"
        ):
            root = offline_dir.parents[2]
        elif offline_dir.parent.name == "src":
            root = offline_dir.parent.parent
        else:
            root = offline_dir.parent

        split_dir = root / "data" / "gold" / "splits"
        artifact_base = root / "artifacts" / "offline"

        registry_name = (
            "feature_registry_full.yaml"
            if feature_set == "full"
            else "feature_registry.yaml"
        )

        return cls(
            root=root,
            raw_dir=root / "data" / "raw",
            silver_path=root / "data" / "silver" / "ids2018_cleaned.parquet",
            split_dir=split_dir,
            train_path=split_dir / "train.parquet",
            valid_path=split_dir / "valid.parquet",
            test_path=split_dir / "test.parquet",
            feature_registry_path=root / "configs" / "modeling" / registry_name,
            preprocessing_path=root / "configs" / "modeling" / "preprocessing.yaml",
            label_mapping_path=root / "configs" / "schema" / "label_mapping.yaml",
            models_dir=artifact_base / "models",
            preprocessing_dir=artifact_base / "preprocessing",
            evaluation_dir=artifact_base / "evaluation",
            log_dir=root / "logs" / "offline",
        )

    def ensure_dirs(self) -> None:
        for d in (
            self.silver_path.parent,
            self.split_dir,
            self.models_dir,
            self.preprocessing_dir,
            self.evaluation_dir,
            self.log_dir,
        ):
            d.mkdir(parents=True, exist_ok=True)
