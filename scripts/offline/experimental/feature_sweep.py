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
    save_csv,
    select_features_from_ranking,
    validate_feature_set_size,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rerunnable reduced-feature sweep aligned with the current offline pipeline",
    )
    parser.add_argument(
        "--models",
        nargs="*",
        default=["logistic_regression", "random_forest", "gradient_boosting"],
    )
    parser.add_argument(
        "--k-values",
        nargs="*",
        type=int,
        default=[10, 17, 25, 40, 78],
    )
    parser.add_argument(
        "--selection-method",
        type=str,
        default="",
        help="Override feature ranking method. Default: use preprocessing.yaml",
    )
    parser.add_argument(
        "--selection-strategy",
        type=str,
        default="",
        help="Override merge strategy. Default: use preprocessing.yaml",
    )
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

    selection_method = args.selection_method.strip() or preprocessing_config.feature_selection.method
    selection_strategy = args.selection_strategy.strip() or preprocessing_config.feature_selection.strategy
    k_values = sorted({int(k) for k in args.k_values if int(k) > 0})
    run_tag = args.run_tag.strip() or default_run_tag("feature_sweep")

    output_csv = Path(args.output_csv) if args.output_csv.strip() else build_output_path(
        base_filename="feature_sweep_results.csv",
        run_tag=run_tag,
        overwrite=args.overwrite,
    )
    output_features_csv = Path(args.output_features_csv) if args.output_features_csv.strip() else build_output_path(
        base_filename="feature_sweep_selected_features.csv",
        run_tag=run_tag,
        overwrite=args.overwrite,
    )
    output_per_attack_csv = Path(args.output_per_attack_csv) if args.output_per_attack_csv.strip() else build_output_path(
        base_filename="feature_sweep_per_attack_recall.csv",
        run_tag=run_tag,
        overwrite=args.overwrite,
    )

    print_section("Feature Sweep")
    print(f"run_tag={run_tag}")
    print(f"models={args.models}")
    print(f"k_values={k_values}")
    print(f"selection_method={selection_method}")
    print(f"selection_strategy={selection_strategy}")
    print(f"max_rows={max_rows or 'all'}")
    print(f"overwrite={args.overwrite}")

    ranking = compute_feature_ranking(
        train_path=paths.train_path,
        candidate_features=candidate_features,
        preprocessing_config=preprocessing_config,
        method=selection_method,
        log=log,
    )

    x_train, y_train, x_valid, y_valid, labels_valid, x_test, y_test, labels_test = load_train_valid_test(
        paths=paths,
        features=candidate_features,
        max_rows=max_rows,
        log=log,
    )

    results: list[dict] = []
    feature_rows: list[dict] = []
    per_attack_rows: list[dict] = []

    for k in k_values:
        selected_features = select_features_from_ranking(
            ranking=ranking,
            required_features=required_features,
            candidate_features=candidate_features,
            strategy=selection_strategy,
            k=min(k, len(candidate_features)),
            log=log,
        )
        feature_set_label = f"{selection_strategy}_{selection_method}_k{len(selected_features)}"
        if selection_strategy.strip().lower() == "topk":
            validate_feature_set_size(
                label=feature_set_label,
                expected_k=min(k, len(candidate_features)),
                selected_features=selected_features,
            )

        print_section(f"{feature_set_label} ({len(selected_features)} features)")
        print("top features:", ", ".join(selected_features[:12]))

        for feature_rank, feature_name in enumerate(selected_features, start=1):
            feature_rows.append(
                {
                    "feature_set_label": feature_set_label,
                    "k_requested": k,
                    "k_effective": len(selected_features),
                    "selection_strategy": selection_strategy,
                    "selection_method": selection_method,
                    "feature_rank": feature_rank,
                    "feature_name": feature_name,
                }
            )

        x_train_sel = x_train[selected_features]
        x_valid_sel = x_valid[selected_features]
        x_test_sel = x_test[selected_features]

        for model_name in args.models:
            pipe = build_pipeline_for_model(
                preprocessing_config=preprocessing_config,
                model_name=model_name,
                use_class_weight=use_class_weight,
            )

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
                model_name=model_name,
                selected_features=selected_features,
                feature_set_label=feature_set_label,
                x_valid=x_valid_sel,
                y_valid=y_valid,
                labels_valid=labels_valid,
                x_test=x_test_sel,
                y_test=y_test,
                labels_test=labels_test,
                budgets=DEFAULT_FPR_BUDGETS,
            )

            for row in model_results:
                row["k_requested"] = k
                row["selection_strategy"] = selection_strategy
                row["selection_method"] = selection_method
                row["train_time_s"] = round(train_time_s, 3)
                row["train_rows_used"] = train_rows_used
                results.append(row)

            for row in model_per_attack_rows:
                row["k_requested"] = k
                row["selection_strategy"] = selection_strategy
                row["selection_method"] = selection_method
                per_attack_rows.append(row)

            primary_row = next((row for row in model_results if abs(float(row["fpr_budget"]) - 0.05) < 1e-9), None)
            if primary_row is not None:
                print(
                    f"{model_name:<22} test_f1={primary_row['test_f1']:.4f} "
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
