"""Phase 02 – Split Silver Parquet → Gold train / valid / test.

Day-block binary benchmark split for CSE-CIC-IDS2018.

Memory-safe two-pass streaming approach:
  Pass 1 – read timestamp + label (lightweight) → log per-day stats.
  Pass 2 – stream 500K-row batches → write directly to 3 ParquetWriters.

Split strategy: day-block (explicit day assignment).
  - Valid/test evaluate binary detection under temporal shift.
  - Label column is preserved for scripts per-attack analysis,
    not for multiclass online inference.
  - No intra-day splitting → avoids optimistic same-campaign bias.
"""

from __future__ import annotations

import gc
from datetime import date
from typing import Any, cast

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from pandas import DataFrame

from ids_platform.offline.cleaning import parse_timestamps
from ids_platform.offline.config import write_yaml
from ids_platform.offline.log import get_logger
from ids_platform.offline.paths import Paths

# ── knobs ───────────────────────────────────────────────────────────────

BATCH_SIZE = 500_000

# ── Day-block binary benchmark split ────────────────────────────────────
#   Train: covers all major attack families (7 days).
#   Valid: Web attacks Day 2 — seen family, temporal shift from Feb 22.
#   Test:  Infiltration Day 2 (seen) + Bot (unseen) — realistic difficulty.

TRAIN_DAYS = [
    date(2018, 2, 14),   # BruteForce (SSH, FTP)
    date(2018, 2, 15),   # DoS (GoldenEye, Slowloris)
    date(2018, 2, 16),   # DoS (Hulk, SlowHTTPTest)
    date(2018, 2, 20),   # DDoS (LOIC-HTTP)
    date(2018, 2, 21),   # DDoS (HOIC, LOIC-UDP)
    date(2018, 2, 22),   # Web attacks (BF-Web, XSS, SQLi)
    date(2018, 2, 28),   # Infiltration Day 1
]
VALID_DAYS = [
    date(2018, 2, 23),   # Web attacks Day 2
]
TEST_DAYS = [
    date(2018, 3, 1),    # Infiltration Day 2
    date(2018, 3, 2),    # Bot (unseen family → hard case)
]

TRAIN_SET = set(TRAIN_DAYS)
VALID_SET = set(VALID_DAYS)
TEST_SET  = set(TEST_DAYS)


# ── helpers ─────────────────────────────────────────────────────────────


def _class_dist(counts: dict[int, int]) -> list[dict[str, Any]]:
    total = sum(counts.values()) or 1
    return [
        {"label": str(lbl), "count": cnt, "ratio": round(cnt / total, 6)}
        for lbl, cnt in sorted(counts.items())
    ]


# ── write helper ─────────────────────────────────────────────────────────


def _write_subset(
    subset: DataFrame,
    split_name: str,
    writers: dict,
    out_paths: dict,
    split_rows: dict,
    split_ts_min: dict,
    split_ts_max: dict,
    split_class_counts: dict,
    split_attack_types: dict,
    split_family_counts: dict,
) -> None:
    """Write a subset DataFrame to the appropriate ParquetWriter."""
    from_pandas = cast(Any, pa.Table.from_pandas)
    table = from_pandas(subset, preserve_index=False)
    if writers[split_name] is None:
        writers[split_name] = pq.ParquetWriter(out_paths[split_name], table.schema)
    try:
        table = table.cast(writers[split_name].schema)
    except (pa.ArrowInvalid, ValueError):
        pass
    writers[split_name].write_table(table)

    n = len(subset)
    split_rows[split_name] += n

    if "timestamp" in subset.columns:
        ts = subset["timestamp"]
        ts_min, ts_max = ts.min(), ts.max()
        if split_ts_min[split_name] is None or ts_min < split_ts_min[split_name]:
            split_ts_min[split_name] = ts_min
        if split_ts_max[split_name] is None or ts_max > split_ts_max[split_name]:
            split_ts_max[split_name] = ts_max

    if "label_binary" in subset.columns:
        for lbl, cnt in subset["label_binary"].value_counts().items():
            split_class_counts[split_name][int(lbl)] = (
                split_class_counts[split_name].get(int(lbl), 0) + int(cnt)
            )
        if "label" in subset.columns:
            atk_counts = subset.loc[subset["label_binary"] == 1, "label"].value_counts()
            split_attack_types[split_name].update(atk_counts.index)
            for family, cnt in atk_counts.items():
                split_family_counts[split_name][family] = (
                    split_family_counts[split_name].get(family, 0) + int(cnt)
                )


# ── public API ──────────────────────────────────────────────────────────


def run(paths: Paths) -> None:
    log = get_logger("phase02", paths.log_dir / "phase02_split.log")
    paths.ensure_dirs()

    if not paths.silver_path.exists():
        raise FileNotFoundError(f"Silver parquet not found: {paths.silver_path}")

    # ══════════════════════════════════════════════════════════════════
    # Pass 1 – lightweight metadata
    # ══════════════════════════════════════════════════════════════════
    log.info("START  strategy=day_block  pass-1: reading metadata ...")

    meta_df = pd.read_parquet(
        paths.silver_path,
        columns=["timestamp", "label", "label_binary"],
    )
    meta_df["timestamp"] = parse_timestamps(meta_df["timestamp"])
    meta_df = meta_df[meta_df["timestamp"].notna()].copy()
    meta_df["_day"] = meta_df["timestamp"].dt.date
    total_rows = len(meta_df)

    # per-day stats
    for day, grp in meta_df.groupby("_day"):
        atk_types = sorted(grp.loc[grp["label_binary"] == 1, "label"].dropna().unique())
        n_atk = int(grp["label_binary"].sum())
        split = "TRAIN" if day in TRAIN_SET else "VALID" if day in VALID_SET else "TEST" if day in TEST_SET else "SKIP"
        log.info(
            "  %s [%s]  rows=%7d  attack=%7d (%5.1f%%)  types=%s",
            day, split, len(grp), n_atk,
            100 * n_atk / max(len(grp), 1),
            atk_types or ["(benign)"],
        )

    del meta_df
    gc.collect()

    # ══════════════════════════════════════════════════════════════════
    # Pass 2 – stream batches → 3 parquet writers
    # ══════════════════════════════════════════════════════════════════
    log.info("  pass-2: streaming %d-row batches ...", BATCH_SIZE)

    dataset = ds.dataset(paths.silver_path, format="parquet")

    writers: dict[str, pq.ParquetWriter | None] = {
        "train": None, "valid": None, "test": None,
    }
    out_paths = {
        "train": paths.train_path,
        "valid": paths.valid_path,
        "test":  paths.test_path,
    }
    split_rows:   dict[str, int]  = {"train": 0, "valid": 0, "test": 0}
    split_ts_min: dict[str, Any]  = {"train": None, "valid": None, "test": None}
    split_ts_max: dict[str, Any]  = {"train": None, "valid": None, "test": None}
    split_class_counts: dict[str, dict[int, int]] = {
        "train": {}, "valid": {}, "test": {},
    }
    split_attack_types: dict[str, set] = {
        "train": set(), "valid": set(), "test": set()
    }
    split_family_counts: dict[str, dict[str, int]] = {
        "train": {}, "valid": {}, "test": {},
    }
    skipped = 0

    dataset_scanner = cast(Any, dataset).scanner(batch_size=BATCH_SIZE)
    for batch in dataset_scanner.to_batches():
        chunk = batch.to_pandas()
        chunk["timestamp"] = parse_timestamps(chunk["timestamp"])
        chunk = chunk[chunk["timestamp"].notna()].copy()
        chunk["_day"] = chunk["timestamp"].dt.date

        for split_name, day_set in [
            ("train", TRAIN_SET), ("valid", VALID_SET), ("test", TEST_SET),
        ]:
            mask = chunk["_day"].isin(day_set)
            subset = chunk.loc[mask].drop(columns=["_day"])
            if subset.empty:
                continue
            _write_subset(subset, split_name, writers, out_paths,
                          split_rows, split_ts_min, split_ts_max,
                          split_class_counts, split_attack_types,
                          split_family_counts)

        all_known = chunk["_day"].isin(TRAIN_SET | VALID_SET | TEST_SET)
        skipped += int((~all_known).sum())

        del chunk
        gc.collect()

    for w in writers.values():
        if w is not None:
            w.close()
    del dataset_scanner
    del dataset
    gc.collect()

    if skipped > 0:
        log.warning("  %d rows skipped (day not in any split)", skipped)

    # ══════════════════════════════════════════════════════════════════
    # Coverage report
    # ══════════════════════════════════════════════════════════════════
    train_types = sorted(split_attack_types["train"])
    valid_types = sorted(split_attack_types["valid"])
    test_types  = sorted(split_attack_types["test"])
    unseen_in_valid = sorted(set(valid_types) - set(train_types))
    unseen_in_test  = sorted(set(test_types)  - set(train_types))

    log.info("  train attack types: %s", train_types)
    log.info("  valid attack types: %s", valid_types)
    log.info("  test  attack types: %s", test_types)

    if unseen_in_valid:
        log.warning("  UNSEEN in valid: %s", unseen_in_valid)
    if unseen_in_test:
        log.warning("  UNSEEN in test (zero-day): %s", unseen_in_test)
    if not unseen_in_valid and not unseen_in_test:
        log.info("  all valid/test attack types seen in train")

    for s in ("train", "valid", "test"):
        cc = split_class_counts[s]
        n  = split_rows[s]
        n_benign, n_attack = cc.get(0, 0), cc.get(1, 0)
        log.info(
            "  %-5s  rows=%8d  benign=%8d (%.1f%%)  attack=%7d (%.1f%%)",
            s, n, n_benign, 100 * n_benign / max(n, 1), n_attack, 100 * n_attack / max(n, 1),
        )
        # per-family counts
        for family, cnt in sorted(split_family_counts[s].items(), key=lambda x: -x[1]):
            log.info("    %-40s %d", family, cnt)

    # ══════════════════════════════════════════════════════════════════
    # Metadata
    # ══════════════════════════════════════════════════════════════════
    seen_test = sorted(set(test_types) & set(train_types))

    meta: dict[str, Any] = {
        "version": 7,
        "strategy": "day_block",
        "task": "binary_classification",
        "target_column": "label_binary",
        "evaluation_scope": {
            "online_task": "binary_detection",
            "offline_analysis": ["per_attack_recall", "attack_family_breakdown"],
        },
        "n_rows_total": total_rows,
        "n_skipped": skipped,
        "n_train": split_rows["train"],
        "n_valid": split_rows["valid"],
        "n_test":  split_rows["test"],
        "train_days": [str(d) for d in TRAIN_DAYS],
        "valid_days": [str(d) for d in VALID_DAYS],
        "test_days":  [str(d) for d in TEST_DAYS],
        "train_time_range": [str(split_ts_min["train"]), str(split_ts_max["train"])],
        "valid_time_range": [str(split_ts_min["valid"]), str(split_ts_max["valid"])],
        "test_time_range":  [str(split_ts_min["test"]),  str(split_ts_max["test"])],
        "attack_type_coverage": {
            "train":          train_types,
            "valid":          valid_types,
            "test":           test_types,
            "seen_in_test":   seen_test,
            "unseen_in_valid": unseen_in_valid,
            "unseen_in_test":  unseen_in_test,
        },
        "attack_family_counts": {
            s: dict(sorted(split_family_counts[s].items(), key=lambda x: -x[1]))
            for s in ("train", "valid", "test")
        },
        "class_coverage": {
            s: _class_dist(split_class_counts[s]) for s in ("train", "valid", "test")
        },
    }
    write_yaml(paths.split_dir / "split_metadata.yaml", meta)

    log.info(
        "DONE   train=%d  valid=%d  test=%d  skipped=%d",
        split_rows["train"], split_rows["valid"], split_rows["test"], skipped,
    )
