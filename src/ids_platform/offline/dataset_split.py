"""Phase 02 - Split Silver Parquet into gold experiment splits.

Protocol:
  - Train pool days are split deterministically into:
      train_fit        70%  model fitting
      feature_select   15%  feature ranking / reduction
      calibration      15%  threshold and model selection
  - Final evaluation is kept temporal:
      test_seen_temporal   2018-03-01 Infiltration day 2
      test_unseen_family   2018-03-02 Bot
      test_rare_web        2018-02-23 rare Web attack diagnostic

The goal is to avoid calibrating thresholds on the old Feb-23 split, which is
almost all benign and makes offline model selection misleading.
"""

from __future__ import annotations

import gc
from datetime import date
from typing import Any, cast

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from pandas import DataFrame, Series

from ids_platform.offline.cleaning import parse_timestamps
from ids_platform.offline.config import write_yaml
from ids_platform.offline.log import get_logger
from ids_platform.offline.paths import Paths


BATCH_SIZE = 500_000
HASH_BUCKETS = 10_000
TRAIN_FIT_CUTOFF = 7_000
FEATURE_SELECT_CUTOFF = 8_500

TRAIN_POOL_DAYS = [
    date(2018, 2, 14),  # BruteForce
    date(2018, 2, 15),  # DoS
    date(2018, 2, 16),  # DoS
    date(2018, 2, 20),  # DDoS
    date(2018, 2, 21),  # DDoS
    date(2018, 2, 22),  # Web attacks day 1
    date(2018, 2, 28),  # Infiltration day 1
]
TEST_RARE_WEB_DAYS = [date(2018, 2, 23)]
TEST_SEEN_DAYS = [date(2018, 3, 1)]
TEST_UNSEEN_DAYS = [date(2018, 3, 2)]

TRAIN_POOL_SET = set(TRAIN_POOL_DAYS)
TEST_RARE_WEB_SET = set(TEST_RARE_WEB_DAYS)
TEST_SEEN_SET = set(TEST_SEEN_DAYS)
TEST_UNSEEN_SET = set(TEST_UNSEEN_DAYS)
KNOWN_DAYS = TRAIN_POOL_SET | TEST_RARE_WEB_SET | TEST_SEEN_SET | TEST_UNSEEN_SET

SPLITS = (
    "train_fit",
    "feature_select",
    "calibration",
    "test_seen_temporal",
    "test_unseen_family",
    "test_rare_web",
    "test",
)


def _class_dist(counts: dict[int, int]) -> list[dict[str, Any]]:
    total = sum(counts.values()) or 1
    return [
        {"label": str(lbl), "count": cnt, "ratio": round(cnt / total, 6)}
        for lbl, cnt in sorted(counts.items())
    ]


def _remove_old_split_outputs(paths: Paths) -> None:
    for path in [
        paths.train_fit_path,
        paths.feature_select_path,
        paths.calibration_path,
        paths.test_seen_path,
        paths.test_unseen_path,
        paths.test_rare_web_path,
        paths.test_path,
        paths.split_dir / "train.parquet",
        paths.split_dir / "valid.parquet",
    ]:
        if path.exists():
            path.unlink()


def _write_subset(
    subset: DataFrame,
    split_name: str,
    writers: dict[str, pq.ParquetWriter | None],
    out_paths: dict[str, Any],
    split_rows: dict[str, int],
    split_ts_min: dict[str, Any],
    split_ts_max: dict[str, Any],
    split_class_counts: dict[str, dict[int, int]],
    split_family_counts: dict[str, dict[str, int]],
) -> None:
    if subset.empty:
        return

    from_pandas = cast(Any, pa.Table.from_pandas)
    table = from_pandas(subset, preserve_index=False)
    out_path = out_paths[split_name]
    if writers[split_name] is None:
        writers[split_name] = pq.ParquetWriter(out_path, table.schema)
    try:
        table = table.cast(writers[split_name].schema)
    except (pa.ArrowInvalid, ValueError):
        pass
    writers[split_name].write_table(table)

    split_rows[split_name] += len(subset)

    ts = subset["timestamp"] if "timestamp" in subset.columns else None
    if ts is not None:
        ts_min, ts_max = ts.min(), ts.max()
        if split_ts_min[split_name] is None or ts_min < split_ts_min[split_name]:
            split_ts_min[split_name] = ts_min
        if split_ts_max[split_name] is None or ts_max > split_ts_max[split_name]:
            split_ts_max[split_name] = ts_max

    if "label_binary" in subset.columns:
        for label_value, count in subset["label_binary"].value_counts().items():
            key = int(label_value)
            split_class_counts[split_name][key] = (
                split_class_counts[split_name].get(key, 0) + int(count)
            )

    if "label" in subset.columns and "label_binary" in subset.columns:
        attack_counts = subset.loc[subset["label_binary"] == 1, "label"].value_counts()
        for family, count in attack_counts.items():
            family_key = str(family)
            split_family_counts[split_name][family_key] = (
                split_family_counts[split_name].get(family_key, 0) + int(count)
            )


def _assign_train_pool_split(chunk: DataFrame, row_offset: int) -> Series:
    keys = chunk[["timestamp", "label", "source_file"]].copy()
    keys["__row_number__"] = np.arange(row_offset, row_offset + len(chunk), dtype=np.int64)
    buckets = pd.util.hash_pandas_object(keys, index=False).to_numpy() % HASH_BUCKETS
    return pd.Series(
        np.select(
            [
                buckets < TRAIN_FIT_CUTOFF,
                buckets < FEATURE_SELECT_CUTOFF,
            ],
            [
                "train_fit",
                "feature_select",
            ],
            default="calibration",
        ),
        index=chunk.index,
    )


def _write_metadata(
    paths: Paths,
    *,
    total_rows: int,
    skipped: int,
    split_rows: dict[str, int],
    split_ts_min: dict[str, Any],
    split_ts_max: dict[str, Any],
    split_class_counts: dict[str, dict[int, int]],
    split_family_counts: dict[str, dict[str, int]],
) -> None:
    split_paths = {
        "train_fit": paths.train_fit_path,
        "feature_select": paths.feature_select_path,
        "calibration": paths.calibration_path,
        "test_seen_temporal": paths.test_seen_path,
        "test_unseen_family": paths.test_unseen_path,
        "test_rare_web": paths.test_rare_web_path,
        "test": paths.test_path,
    }

    def relative_split_path(path: Any) -> str:
        try:
            return str(path.relative_to(paths.root)).replace("\\", "/")
        except ValueError:
            return str(path).replace("\\", "/")

    meta: dict[str, Any] = {
        "version": 8,
        "strategy": "train_pool_hash_split_plus_temporal_tests",
        "task": "binary_classification",
        "target_column": "label_binary",
        "n_rows_total": total_rows,
        "n_skipped": skipped,
        "train_pool_days": [str(d) for d in TRAIN_POOL_DAYS],
        "train_pool_ratios": {
            "train_fit": 0.70,
            "feature_select": 0.15,
            "calibration": 0.15,
        },
        "final_tests": {
            "test_seen_temporal": [str(d) for d in TEST_SEEN_DAYS],
            "test_unseen_family": [str(d) for d in TEST_UNSEEN_DAYS],
            "test_rare_web": [str(d) for d in TEST_RARE_WEB_DAYS],
            "test": [str(d) for d in [*TEST_SEEN_DAYS, *TEST_UNSEEN_DAYS]],
        },
        "split_roles": {
            "train_fit": "fit model parameters",
            "feature_select": "rank/select reduced features",
            "calibration": "select model and operating threshold",
            "test_seen_temporal": "final temporal test for seen attack family",
            "test_unseen_family": "final zero-day family test",
            "test_rare_web": "optional rare Web attack diagnostic",
            "test": "combined test_seen_temporal + test_unseen_family compatibility set",
        },
        "splits": {},
    }

    for split_name in SPLITS:
        meta["splits"][split_name] = {
            "path": relative_split_path(split_paths[split_name]),
            "n_rows": split_rows[split_name],
            "time_range": [
                str(split_ts_min[split_name]),
                str(split_ts_max[split_name]),
            ],
            "class_coverage": _class_dist(split_class_counts[split_name]),
            "attack_family_counts": dict(
                sorted(split_family_counts[split_name].items(), key=lambda item: -item[1])
            ),
        }

    write_yaml(paths.split_dir / "split_metadata.yaml", meta)


def run(paths: Paths) -> None:
    log = get_logger("phase02", paths.log_dir / "phase02_split.log")
    paths.ensure_dirs()

    if not paths.silver_path.exists():
        raise FileNotFoundError(f"Silver parquet not found: {paths.silver_path}")

    _remove_old_split_outputs(paths)

    log.info("START strategy=train_pool_hash_split_plus_temporal_tests")
    meta_df = pd.read_parquet(
        paths.silver_path,
        columns=["timestamp", "label", "label_binary"],
    )
    meta_df["timestamp"] = parse_timestamps(meta_df["timestamp"])
    meta_df = meta_df[meta_df["timestamp"].notna()].copy()
    meta_df["_day"] = meta_df["timestamp"].dt.date
    total_rows = len(meta_df)

    for day, group in meta_df.groupby("_day"):
        attack_rows = int(group["label_binary"].sum())
        if day in TRAIN_POOL_SET:
            role = "TRAIN_POOL"
        elif day in TEST_RARE_WEB_SET:
            role = "TEST_RARE_WEB"
        elif day in TEST_SEEN_SET:
            role = "TEST_SEEN_TEMPORAL"
        elif day in TEST_UNSEEN_SET:
            role = "TEST_UNSEEN_FAMILY"
        else:
            role = "SKIP"
        log.info(
            "  %s [%s] rows=%d attack=%d (%.3f%%)",
            day,
            role,
            len(group),
            attack_rows,
            100 * attack_rows / max(len(group), 1),
        )

    del meta_df
    gc.collect()

    out_paths = {
        "train_fit": paths.train_fit_path,
        "feature_select": paths.feature_select_path,
        "calibration": paths.calibration_path,
        "test_seen_temporal": paths.test_seen_path,
        "test_unseen_family": paths.test_unseen_path,
        "test_rare_web": paths.test_rare_web_path,
        "test": paths.test_path,
    }
    writers: dict[str, pq.ParquetWriter | None] = {name: None for name in SPLITS}
    split_rows = {name: 0 for name in SPLITS}
    split_ts_min = {name: None for name in SPLITS}
    split_ts_max = {name: None for name in SPLITS}
    split_class_counts: dict[str, dict[int, int]] = {name: {} for name in SPLITS}
    split_family_counts: dict[str, dict[str, int]] = {name: {} for name in SPLITS}
    skipped = 0
    row_offset = 0

    dataset = ds.dataset(paths.silver_path, format="parquet")
    dataset_scanner = cast(Any, dataset).scanner(batch_size=BATCH_SIZE)
    for batch in dataset_scanner.to_batches():
        chunk = batch.to_pandas()
        chunk["timestamp"] = parse_timestamps(chunk["timestamp"])
        chunk = chunk[chunk["timestamp"].notna()].copy()
        chunk["_day"] = chunk["timestamp"].dt.date

        train_pool_mask = chunk["_day"].isin(TRAIN_POOL_SET)
        if train_pool_mask.any():
            train_pool = chunk.loc[train_pool_mask].copy()
            train_pool_stage = _assign_train_pool_split(train_pool, row_offset)
            for split_name in ("train_fit", "feature_select", "calibration"):
                subset = train_pool.loc[train_pool_stage.eq(split_name)].drop(columns=["_day"])
                _write_subset(
                    subset,
                    split_name,
                    writers,
                    out_paths,
                    split_rows,
                    split_ts_min,
                    split_ts_max,
                    split_class_counts,
                    split_family_counts,
                )

        test_seen = chunk.loc[chunk["_day"].isin(TEST_SEEN_SET)].drop(columns=["_day"])
        test_unseen = chunk.loc[chunk["_day"].isin(TEST_UNSEEN_SET)].drop(columns=["_day"])
        test_rare_web = chunk.loc[chunk["_day"].isin(TEST_RARE_WEB_SET)].drop(columns=["_day"])

        _write_subset(
            test_seen,
            "test_seen_temporal",
            writers,
            out_paths,
            split_rows,
            split_ts_min,
            split_ts_max,
            split_class_counts,
            split_family_counts,
        )
        _write_subset(
            test_unseen,
            "test_unseen_family",
            writers,
            out_paths,
            split_rows,
            split_ts_min,
            split_ts_max,
            split_class_counts,
            split_family_counts,
        )
        _write_subset(
            test_rare_web,
            "test_rare_web",
            writers,
            out_paths,
            split_rows,
            split_ts_min,
            split_ts_max,
            split_class_counts,
            split_family_counts,
        )

        if not test_seen.empty:
            _write_subset(
                test_seen,
                "test",
                writers,
                out_paths,
                split_rows,
                split_ts_min,
                split_ts_max,
                split_class_counts,
                split_family_counts,
            )
        if not test_unseen.empty:
            _write_subset(
                test_unseen,
                "test",
                writers,
                out_paths,
                split_rows,
                split_ts_min,
                split_ts_max,
                split_class_counts,
                split_family_counts,
            )

        skipped += int((~chunk["_day"].isin(KNOWN_DAYS)).sum())
        row_offset += len(chunk)

        del chunk
        gc.collect()

    for writer in writers.values():
        if writer is not None:
            writer.close()

    del dataset_scanner
    del dataset
    gc.collect()

    _write_metadata(
        paths,
        total_rows=total_rows,
        skipped=skipped,
        split_rows=split_rows,
        split_ts_min=split_ts_min,
        split_ts_max=split_ts_max,
        split_class_counts=split_class_counts,
        split_family_counts=split_family_counts,
    )

    for split_name in SPLITS:
        cc = split_class_counts[split_name]
        n_rows = split_rows[split_name]
        n_attack = cc.get(1, 0)
        log.info(
            "  %-20s rows=%d attack=%d (%.3f%%)",
            split_name,
            n_rows,
            n_attack,
            100 * n_attack / max(n_rows, 1),
        )
    if skipped:
        log.warning("  skipped rows=%d", skipped)
    log.info("DONE")
