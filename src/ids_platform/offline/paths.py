"""Centralized path management for the offline pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Paths:
    """Every file/directory the pipeline reads or writes."""

    root: Path

    raw_dir: Path
    silver_path: Path
    split_dir: Path
    train_fit_path: Path
    feature_select_path: Path
    calibration_path: Path
    test_seen_path: Path
    test_unseen_path: Path
    test_rare_web_path: Path
    test_path: Path
    train_path: Path
    valid_path: Path

    feature_registry_path: Path
    preprocessing_path: Path
    label_mapping_path: Path

    log_dir: Path

    @classmethod
    def build(cls, offline_dir: Path, *, feature_set: str = "reduced") -> Paths:
        offline_dir = offline_dir.resolve()
        if (
            offline_dir.parent.name == "ids_platform"
            and offline_dir.parent.parent.name == "src"
        ):
            root = offline_dir.parents[2]
        else:
            raise ValueError(
                "offline_dir must be the package directory src/ids_platform/offline"
            )

        split_dir = root / "data" / "gold" / "splits"
        registry_name = (
            "feature_registry_full.yaml"
            if feature_set == "full"
            else "feature_registry_reduced_anchor.yaml"
        )

        train_fit_path = split_dir / "train_fit.parquet"
        calibration_path = split_dir / "calibration.parquet"

        return cls(
            root=root,
            raw_dir=root / "data" / "raw",
            silver_path=root / "data" / "silver" / "ids2018_cleaned.parquet",
            split_dir=split_dir,
            train_fit_path=train_fit_path,
            feature_select_path=split_dir / "feature_select.parquet",
            calibration_path=calibration_path,
            test_seen_path=split_dir / "test_seen_temporal.parquet",
            test_unseen_path=split_dir / "test_unseen_family.parquet",
            test_rare_web_path=split_dir / "test_rare_web.parquet",
            test_path=split_dir / "test.parquet",
            train_path=train_fit_path,
            valid_path=calibration_path,
            feature_registry_path=root / "configs" / "modeling" / registry_name,
            preprocessing_path=root / "configs" / "modeling" / "preprocessing.yaml",
            label_mapping_path=root / "configs" / "schema" / "label_mapping.yaml",
            log_dir=root / "logs" / "offline",
        )

    @property
    def feature_set_name(self) -> str:
        return "full" if self.feature_registry_path.name == "feature_registry_full.yaml" else "reduced"

    def ensure_dirs(self) -> None:
        for d in (
            self.silver_path.parent,
            self.split_dir,
            self.log_dir,
        ):
            d.mkdir(parents=True, exist_ok=True)
