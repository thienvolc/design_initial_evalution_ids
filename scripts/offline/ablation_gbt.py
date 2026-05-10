from __future__ import annotations

import argparse
from pathlib import Path

from _common import print_section
from _feature_analysis_common import (
    DEFAULT_FPR_BUDGETS,
    build_pipeline_for_model,
    build_output_path,
    compute_feature_ranking,
    default_run_tag,
    evaluate_model_with_thresholds,
    fit_with_config_capped_rows,
    load_train_valid_test,
    load_training_context,
    resolve_current_reduced_features,
    resolve_full_features,
    save_csv,
    select_features_from_ranking,
    validate_feature_set_size,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Reduced-feature ablation aligned with the current offline pipeline",
    )
    parser.add_argument("--model", type=str, default="gradient_boosting")
    parser.add_argument(
        "--run-tag",
        type=str,
        default="",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
    )
    parser.add_argument(
        "--output-csv",
        type=str,
        default="",
    )
    parser.add_argument(
        "--output-features-csv",
        type=str,
        default="",
    )
    parser.add_argument(
        "--output-per-attack-csv",
        type=str,
        default="",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    ctx = load_training_context(feature_set="reduced")
    paths = ctx["paths"]
    preprocessing_config = ctx["preprocessing_config"]
    log = ctx["log"]
    candidate_features = ctx["candidate_features"]
    required_features = ctx["required_features"]
    max_rows = ctx["max_rows"]
    use_class_weight = ctx["use_class_weight"]

    base_method = preprocessing_config.feature_selection.method
    base_k = preprocessing_config.feature_selection.k
    run_tag = args.run_tag.strip() or default_run_tag("ablation")
    output_csv = Path(args.output_csv) if args.output_csv.strip() else build_output_path(
        base_filename="ablation_results.csv",
        run_tag=run_tag,
        overwrite=args.overwrite,
    )
    output_features_csv = Path(args.output_features_csv) if args.output_features_csv.strip() else build_output_path(
        base_filename="ablation_selected_features.csv",
        run_tag=run_tag,
        overwrite=args.overwrite,
    )
    output_per_attack_csv = Path(args.output_per_attack_csv) if args.output_per_attack_csv.strip() else build_output_path(
        base_filename="ablation_per_attack_recall.csv",
        run_tag=run_tag,
        overwrite=args.overwrite,
    )
    current_reduced = resolve_current_reduced_features(
        paths=paths,
        preprocessing_config=preprocessing_config,
        log=log,
    )

    mi_ranking = compute_feature_ranking(
        train_path=paths.train_path,
        candidate_features=candidate_features,
        preprocessing_config=preprocessing_config,
        method=base_method,
        log=log,
    )
    f_ranking = compute_feature_ranking(
        train_path=paths.train_path,
        candidate_features=candidate_features,
        preprocessing_config=preprocessing_config,
        method="f_classif",
        log=log,
    )

    feature_sets = [
        {
            "label": "full_all_features",
            "selection_strategy": "manual_full",
            "selection_method": "registry_full",
            "features": resolve_full_features(),
        },
        {
            "label": f"current_reduced_manifest_k{len(current_reduced)}",
            "selection_strategy": preprocessing_config.feature_selection.strategy,
            "selection_method": base_method,
            "features": current_reduced,
        },
        {
            "label": f"topk_{base_method}_k{base_k}",
            "selection_strategy": "topk",
            "selection_method": base_method,
            "features": select_features_from_ranking(
                ranking=mi_ranking,
                required_features=[],
                candidate_features=candidate_features,
                strategy="topk",
                k=base_k,
                log=log,
            ),
        },
        {
            "label": f"topk_f_classif_k{base_k}",
            "selection_strategy": "topk",
            "selection_method": "f_classif",
            "features": select_features_from_ranking(
                ranking=f_ranking,
                required_features=[],
                candidate_features=candidate_features,
                strategy="topk",
                k=base_k,
                log=log,
            ),
        },
        {
            "label": f"hybrid_{base_method}_k25",
            "selection_strategy": "hybrid",
            "selection_method": base_method,
            "features": select_features_from_ranking(
                ranking=mi_ranking,
                required_features=required_features,
                candidate_features=candidate_features,
                strategy="hybrid",
                k=25,
                log=log,
            ),
        },
    ]

    for feature_set in feature_sets:
        label = str(feature_set["label"])
        expected_k = None
        if label.startswith("topk_") and "_k" in label:
            try:
                expected_k = int(label.rsplit("_k", 1)[1])
            except ValueError:
                expected_k = None
        validate_feature_set_size(
            label=label,
            expected_k=expected_k,
            selected_features=feature_set["features"],
        )

    all_features_for_loading = sorted({feature for item in feature_sets for feature in item["features"]})
    x_train, y_train, x_valid, y_valid, labels_valid, x_test, y_test, labels_test = load_train_valid_test(
        paths=paths,
        features=all_features_for_loading,
        max_rows=max_rows,
        log=log,
    )

    results: list[dict] = []
    feature_rows: list[dict] = []
    per_attack_rows: list[dict] = []

    print_section("Reduced-Feature Ablation")
    print(f"run_tag={run_tag}")
    print(f"model={args.model}")
    print(f"base_method={base_method}")
    print(f"base_k={base_k}")
    print(f"overwrite={args.overwrite}")

    for feature_set in feature_sets:
        selected_features = feature_set["features"]
        label = feature_set["label"]
        print_section(f"{label} ({len(selected_features)} features)")
        print("top features:", ", ".join(selected_features[:12]))

        for feature_rank, feature_name in enumerate(selected_features, start=1):
            feature_rows.append(
                {
                    "feature_set_label": label,
                    "selection_strategy": feature_set["selection_strategy"],
                    "selection_method": feature_set["selection_method"],
                    "feature_rank": feature_rank,
                    "feature_name": feature_name,
                }
            )

        pipe = build_pipeline_for_model(
            preprocessing_config=preprocessing_config,
            model_name=args.model,
            use_class_weight=use_class_weight,
        )

        x_train_sel = x_train[selected_features]
        x_valid_sel = x_valid[selected_features]
        x_test_sel = x_test[selected_features]

        train_time_s, train_rows_used = fit_with_config_capped_rows(
            pipe=pipe,
            x_train=x_train_sel,
            y_train=y_train,
            preprocessing_config=preprocessing_config,
            n_features=len(selected_features),
            log=log,
        )

        model_results, model_per_attack_rows = evaluate_model_with_thresholds(
            pipe=pipe,
            model_name=args.model,
            selected_features=selected_features,
            feature_set_label=label,
            x_valid=x_valid_sel,
            y_valid=y_valid,
            labels_valid=labels_valid,
            x_test=x_test_sel,
            y_test=y_test,
            labels_test=labels_test,
            budgets=DEFAULT_FPR_BUDGETS,
        )

        for row in model_results:
            row["selection_strategy"] = feature_set["selection_strategy"]
            row["selection_method"] = feature_set["selection_method"]
            row["train_time_s"] = round(train_time_s, 3)
            row["train_rows_used"] = train_rows_used
            results.append(row)

        for row in model_per_attack_rows:
            row["selection_strategy"] = feature_set["selection_strategy"]
            row["selection_method"] = feature_set["selection_method"]
            per_attack_rows.append(row)

        primary_row = next((row for row in model_results if abs(float(row["fpr_budget"]) - 0.05) < 1e-9), None)
        if primary_row is not None:
            print(
                f"test_f1={primary_row['test_f1']:.4f} "
                f"test_recall={primary_row['test_recall']:.4f} "
                f"test_fnr={primary_row['test_fnr']:.4f} "
                f"train_rows_used={train_rows_used}"
            )

    for row in results:
        row["run_tag"] = run_tag
    for row in feature_rows:
        row["run_tag"] = run_tag
    for row in per_attack_rows:
        row["run_tag"] = run_tag

    save_csv(output_csv, results)
    save_csv(output_features_csv, feature_rows)
    save_csv(output_per_attack_csv, per_attack_rows)

    print_section("Saved")
    print(output_csv)
    print(output_features_csv)
    print(output_per_attack_csv)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
