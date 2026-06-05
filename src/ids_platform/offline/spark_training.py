"""Spark ML offline training and evaluation.

This module is the offline model training path.  The flow is:

    SparkOfflineConfig -> SparkSession -> train models -> calibrate threshold
    -> evaluate test splits -> write Spark artifacts.
"""

from __future__ import annotations

import shutil
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from ids_platform.offline.config import (
    PreprocessingConfig,
    load_feature_list,
    load_json,
    write_json,
)
from ids_platform.offline.paths import Paths


FPR_BUDGETS = (0.01, 0.03, 0.05, 0.10)
PRIMARY_FPR_BUDGET = 0.03


@dataclass(frozen=True)
class SparkModelConfig:
    name: str
    kind: str
    params: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SparkTestSplitConfig:
    name: str
    path: Path


@dataclass(frozen=True)
class SparkOfflineConfig:
    feature_set: str
    feature_columns: list[str]
    train_path: Path
    calibration_path: Path
    test_splits: tuple[SparkTestSplitConfig, ...]
    artifact_root: Path
    models: tuple[SparkModelConfig, ...]
    impute_strategy: str = "median"
    use_class_weight: bool = True
    label_column: str = "label_binary"
    attack_label_column: str = "label"
    app_name: str = "ids_spark_offline_training"
    master: str = "local[4]"
    driver_host: str = "127.0.0.1"
    driver_bind_address: str = "127.0.0.1"
    shuffle_partitions: int = 8
    driver_memory: str = "8g"
    executor_memory: str = "8g"
    parquet_vectorized_reader: bool = False
    primary_fpr_budget: float = PRIMARY_FPR_BUDGET
    fpr_budgets: tuple[float, ...] = FPR_BUDGETS

    @property
    def model_dir(self) -> Path:
        return self.artifact_root / "models"

    @property
    def preprocessing_dir(self) -> Path:
        return self.artifact_root / "preprocessing"

    @property
    def evaluation_dir(self) -> Path:
        return self.artifact_root / "evaluation"


@dataclass(frozen=True)
class TrainedSparkModel:
    config: SparkModelConfig
    pipeline_model: object
    model_path: Path
    fill_values: dict[str, float]
    train_time_s: float


def build_spark_offline_config(
    paths: Paths,
    *,
    selected_models: tuple[str, ...] | list[str] | None = None,
    master: str = "local[4]",
) -> SparkOfflineConfig:
    preprocessing = PreprocessingConfig.from_yaml(paths.preprocessing_path)
    preprocessing = preprocessing.with_selected_models(list(selected_models) if selected_models else None)
    feature_columns = _load_spark_feature_columns(paths)
    model_configs = tuple(
        SparkModelConfig(
            name=toggle.name,
            kind=toggle.name,
            params=_spark_model_params(toggle.name, toggle.params),
        )
        for toggle in preprocessing.models
        if toggle.enabled
    )
    if not model_configs:
        raise ValueError("No Spark models enabled")

    return SparkOfflineConfig(
        feature_set=paths.feature_set_name,
        feature_columns=feature_columns,
        train_path=paths.train_fit_path,
        calibration_path=paths.calibration_path,
        test_splits=(
            SparkTestSplitConfig("test_seen_temporal", paths.test_seen_path),
            SparkTestSplitConfig("test_unseen_family", paths.test_unseen_path),
            SparkTestSplitConfig("test_rare_web", paths.test_rare_web_path),
        ),
        artifact_root=paths.root / "artifacts" / "offline" / "spark" / paths.feature_set_name,
        models=model_configs,
        impute_strategy=preprocessing.impute_strategy,
        use_class_weight=preprocessing.sampling.strategy == "class_weight",
        master=master,
    )


def run(paths: Paths, *, selected_models: list[str] | None = None) -> None:
    config = build_spark_offline_config(
        paths,
        selected_models=tuple(selected_models) if selected_models else None,
    )
    run_spark_offline_experiment(config)


def _spark_model_params(model_name: str, raw_params: dict[str, Any]) -> dict[str, Any]:
    params = dict(raw_params)
    if model_name == "logistic_regression":
        return {
            "max_iter": min(int(params.get("max_iter", 50)), 50),
            "reg_param": float(params.get("reg_param", 0.0)),
            "elastic_net_param": float(params.get("elastic_net_param", 0.0)),
        }
    if model_name == "random_forest":
        return {
            "num_trees": min(int(params.get("num_trees", params.get("n_estimators", 64))), 64),
            "max_depth": min(int(params.get("max_depth", 12)), 12),
            "min_instances_per_node": max(
                1,
                int(params.get("min_instances_per_node", params.get("min_samples_leaf", 1))),
            ),
            "seed": int(params.get("seed", 42)),
        }
    if model_name == "gradient_boosting":
        return {
            "max_iter": min(int(params.get("max_iter", params.get("n_estimators", 40))), 40),
            "max_depth": min(int(params.get("max_depth", 6)), 6),
            "step_size": float(params.get("step_size", params.get("learning_rate", 0.1))),
            "min_instances_per_node": max(
                1,
                int(params.get("min_instances_per_node", params.get("min_samples_leaf", 1))),
            ),
            "seed": int(params.get("seed", 42)),
        }
    return params


def _load_spark_feature_columns(paths: Paths) -> list[str]:
    return load_feature_list(paths.feature_registry_path)


def create_spark_session(config: SparkOfflineConfig):
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    os.environ.setdefault("SPARK_LOCAL_IP", config.driver_host)
    configure_windows_hadoop_home()

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName(config.app_name)
        .master(config.master)
        .config("spark.driver.host", config.driver_host)
        .config("spark.driver.bindAddress", config.driver_bind_address)
        .config("spark.sql.shuffle.partitions", str(config.shuffle_partitions))
        .config("spark.driver.memory", config.driver_memory)
        .config("spark.executor.memory", config.executor_memory)
        .config("spark.sql.parquet.enableVectorizedReader", str(config.parquet_vectorized_reader).lower())
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def configure_windows_hadoop_home() -> None:
    if os.name != "nt":
        return

    hadoop_home = os.environ.get("HADOOP_HOME")
    if not hadoop_home:
        winutils = Path("C:/hadoop/bin/winutils.exe")
        if winutils.exists():
            hadoop_home = str(winutils.parents[1])
            os.environ["HADOOP_HOME"] = hadoop_home

    if not hadoop_home:
        return

    hadoop_bin = Path(hadoop_home) / "bin"
    os.environ["PATH"] = f"{hadoop_bin};{os.environ.get('PATH', '')}"

    winutils_path = hadoop_bin / "winutils.exe"
    hadoop_dll_path = hadoop_bin / "hadoop.dll"
    if winutils_path.exists() and not hadoop_dll_path.exists():
        raise RuntimeError(
            "Windows Spark training requires both winutils.exe and hadoop.dll under "
            f"{hadoop_bin}. Found winutils.exe but missing hadoop.dll."
        )


def run_spark_offline_experiment(config: SparkOfflineConfig) -> dict[str, Any]:
    spark = create_spark_session(config)
    try:
        return run_spark_offline_experiment_with_session(spark, config)
    finally:
        spark.stop()


def run_spark_offline_experiment_with_session(spark, config: SparkOfflineConfig) -> dict[str, Any]:
    _prepare_artifact_dirs(config)
    train_df = _load_modeling_split(spark, config.train_path, config)
    calibration_df = _load_modeling_split(
        spark,
        config.calibration_path,
        config,
        include_attack_label=True,
    )

    if config.use_class_weight:
        train_df = _add_class_weight_column(train_df, config.label_column)

    trained_models: list[TrainedSparkModel] = []
    validation_rows: list[dict[str, Any]] = []
    evaluation_rows: list[dict[str, Any]] = []

    for model_config in config.models:
        trained = train_one_model(train_df, config, model_config)
        trained_models.append(trained)

        calibration_scored = score_split(trained.pipeline_model, calibration_df, config)
        labels, predictions, scores, attack_labels = collect_prediction_arrays(
            calibration_scored,
            label_column=config.label_column,
            attack_label_column=config.attack_label_column,
        )
        threshold, operating_points = select_operating_threshold(
            labels,
            scores,
            budgets=config.fpr_budgets,
            primary_budget=config.primary_fpr_budget,
        )
        calibrated_predictions = predict_from_scores(scores, threshold)
        validation_metrics, calibration_cm = binary_metrics_from_arrays(
            labels,
            calibrated_predictions,
            scores,
        )
        default_metrics, _ = binary_metrics_from_arrays(labels, predictions, scores)
        per_attack_rows = per_attack_recall_rows(
            labels,
            calibrated_predictions,
            attack_labels,
            threshold=threshold,
            model_name=model_config.name,
            split_name="calibration",
        )
        validation_row = {
            "model": model_config.name,
            "split": "calibration",
            "n": int(len(labels)),
            "class_weight": bool(config.use_class_weight),
            "opt_threshold": float(threshold),
            "fpr_budget": float(config.primary_fpr_budget),
            **validation_metrics,
            "default_threshold": 0.5,
            "default_recall": default_metrics["recall"],
            "default_precision": default_metrics["precision"],
            "default_f1": default_metrics["f1"],
            "default_fpr": default_metrics["fpr"],
            "train_time_s": round(trained.train_time_s, 2),
            "operating_points": operating_points,
        }
        validation_rows.append(validation_row)
        _write_model_calibration_artifacts(
            config,
            model_config.name,
            validation_row,
            calibration_cm,
            per_attack_rows,
        )

        for test_split in config.test_splits:
            if not test_split.path.exists():
                continue
            test_df = _load_modeling_split(
                spark,
                test_split.path,
                config,
                include_attack_label=True,
            )
            test_scored = score_split(trained.pipeline_model, test_df, config, threshold=threshold)
            save_predictions(test_scored, config, model_config.name, test_split.name)
            evaluation_rows.append(
                evaluate_scored_split(
                    test_scored,
                    config,
                    model_config.name,
                    test_split.name,
                    threshold,
                )
            )

    current_valid_df = pd.DataFrame([{k: v for k, v in row.items() if k != "operating_points"} for row in validation_rows])
    valid_df = _merge_summary_csv(
        config.model_dir / "valid_metrics.csv",
        current_valid_df,
        key_columns=["model", "split"],
        sort_columns=["recall", "fpr", "precision"],
        ascending=[False, True, False],
    )

    test_summary = pd.DataFrame(evaluation_rows)
    if not test_summary.empty:
        test_summary = _merge_summary_csv(
            config.evaluation_dir / "test_summary.csv",
            test_summary,
            key_columns=["model", "split"],
            sort_columns=["split", "recall", "fpr"],
            ascending=[True, False, True],
        )

    best_name = str(valid_df.iloc[0]["model"])
    best_trained = next((model for model in trained_models if model.config.name == best_name), None)
    best_row = next((row for row in validation_rows if row["model"] == best_name), None)
    if best_trained is not None and best_row is not None:
        _write_best_model_artifacts(config, best_trained, best_row)

    return {
        "artifact_root": str(config.artifact_root),
        "best_model": best_name,
        "validation_rows": validation_rows,
        "evaluation_rows": evaluation_rows,
    }


def _merge_summary_csv(
    path: Path,
    new_rows: pd.DataFrame,
    *,
    key_columns: list[str],
    sort_columns: list[str],
    ascending: list[bool],
) -> pd.DataFrame:
    if path.exists():
        existing_rows = pd.read_csv(path)
        merged = pd.concat([existing_rows, new_rows], ignore_index=True, sort=False)
        merged = merged.drop_duplicates(subset=key_columns, keep="last")
    else:
        merged = new_rows
    merged = merged.sort_values(sort_columns, ascending=ascending)
    merged.to_csv(path, index=False)
    return merged


def _prepare_artifact_dirs(config: SparkOfflineConfig) -> None:
    for path in (config.model_dir, config.preprocessing_dir, config.evaluation_dir):
        path.mkdir(parents=True, exist_ok=True)


def _load_modeling_split(spark, path: Path, config: SparkOfflineConfig, *, include_attack_label: bool = False):
    from pyspark.sql import functions as F

    required_columns = [*config.feature_columns, config.label_column]
    if include_attack_label:
        required_columns.append(config.attack_label_column)

    df = spark.read.schema(_spark_schema_from_parquet(path, required_columns)).parquet(str(path))
    projected_columns = [F.col(feature_name).cast("double").alias(feature_name) for feature_name in config.feature_columns]
    projected_columns.append(F.col(config.label_column).cast("int").alias(config.label_column))
    if include_attack_label:
        projected_columns.append(F.col(config.attack_label_column).cast("string").alias(config.attack_label_column))
    return df.select(*projected_columns)


def _spark_schema_from_parquet(path: Path, columns: list[str]):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    arrow_schema = pq.ParquetFile(path).schema_arrow
    arrow_fields = {field.name: field for field in arrow_schema}
    missing_columns = [column for column in columns if column not in arrow_fields]
    if missing_columns:
        raise ValueError(f"Missing parquet columns in {path}: {missing_columns}")

    spark_fields = []
    for column in columns:
        arrow_type = arrow_fields[column].type
        if pa.types.is_integer(arrow_type):
            spark_type = IntegerType() if arrow_type.bit_width <= 32 else LongType()
        elif pa.types.is_floating(arrow_type):
            spark_type = DoubleType()
        elif pa.types.is_boolean(arrow_type):
            spark_type = BooleanType()
        else:
            spark_type = StringType()
        spark_fields.append(StructField(column, spark_type, nullable=True))
    return StructType(spark_fields)


def _add_class_weight_column(df, label_column: str):
    from pyspark.sql import functions as F

    counts = {
        int(row[label_column]): int(row["count"])
        for row in df.groupBy(label_column).count().collect()
    }
    n_benign = counts.get(0, 0)
    n_attack = counts.get(1, 0)
    total = n_benign + n_attack
    if not n_benign or not n_attack:
        return df.withColumn("class_weight", F.lit(1.0))

    benign_weight = total / (2.0 * n_benign)
    attack_weight = total / (2.0 * n_attack)
    return df.withColumn(
        "class_weight",
        F.when(F.col(label_column).cast("int") == F.lit(1), F.lit(float(attack_weight))).otherwise(
            F.lit(float(benign_weight))
        ),
    )


def train_one_model(df, config: SparkOfflineConfig, model_config: SparkModelConfig) -> TrainedSparkModel:
    pipeline = build_training_pipeline(config, model_config)
    started = time.perf_counter()
    pipeline_model = pipeline.fit(df)
    train_time = time.perf_counter() - started
    model_path = config.model_dir / model_config.name
    if model_path.exists():
        shutil.rmtree(model_path)
    pipeline_model.write().overwrite().save(str(model_path))
    return TrainedSparkModel(
        config=model_config,
        pipeline_model=pipeline_model,
        model_path=model_path,
        fill_values=_extract_imputer_fill_values(pipeline_model, config.feature_columns),
        train_time_s=train_time,
    )


def build_training_pipeline(config: SparkOfflineConfig, model_config: SparkModelConfig):
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import Imputer, VectorAssembler

    imputed_columns = [f"{column}__imputed" for column in config.feature_columns]
    imputer_strategy = _spark_imputer_strategy(config.impute_strategy)
    imputer = Imputer(
        inputCols=config.feature_columns,
        outputCols=imputed_columns,
        strategy=imputer_strategy,
    )
    assembler = VectorAssembler(
        inputCols=imputed_columns,
        outputCol="features",
        handleInvalid="keep",
    )
    estimator = build_estimator(model_config, config)
    return Pipeline(stages=[imputer, assembler, estimator])


def _spark_imputer_strategy(strategy: str) -> str:
    normalized = strategy.strip().lower()
    if normalized in {"median", "mean"}:
        return normalized
    raise ValueError(f"Spark Imputer supports only median/mean, got {strategy!r}")


def build_estimator(model_config: SparkModelConfig, config: SparkOfflineConfig):
    params = model_config.params
    common = {
        "featuresCol": "features",
        "labelCol": config.label_column,
        "predictionCol": "spark_prediction",
        "probabilityCol": "probability",
        "rawPredictionCol": "raw_prediction",
    }

    if model_config.kind == "logistic_regression":
        from pyspark.ml.classification import LogisticRegression

        estimator = LogisticRegression(
            maxIter=int(params.get("max_iter", params.get("maxIter", 100))),
            regParam=float(params.get("reg_param", params.get("regParam", 0.0))),
            elasticNetParam=float(params.get("elastic_net_param", params.get("elasticNetParam", 0.0))),
            standardization=True,
            **common,
        )
    elif model_config.kind == "random_forest":
        from pyspark.ml.classification import RandomForestClassifier

        estimator = RandomForestClassifier(
            numTrees=int(params.get("num_trees", params.get("n_estimators", 100))),
            maxDepth=int(params.get("max_depth", 12)),
            minInstancesPerNode=int(params.get("min_instances_per_node", params.get("min_samples_leaf", 1))),
            seed=int(params.get("seed", 42)),
            **common,
        )
    elif model_config.kind == "gradient_boosting":
        from pyspark.ml.classification import GBTClassifier

        gbt_common = {
            key: value
            for key, value in common.items()
            if key not in {"probabilityCol", "rawPredictionCol"}
        }
        estimator = GBTClassifier(
            maxIter=int(params.get("max_iter", params.get("n_estimators", 100))),
            maxDepth=int(params.get("max_depth", 6)),
            stepSize=float(params.get("step_size", params.get("learning_rate", 0.1))),
            minInstancesPerNode=int(params.get("min_instances_per_node", params.get("min_samples_leaf", 1))),
            seed=int(params.get("seed", 42)),
            **gbt_common,
        )
    else:
        raise ValueError(f"Unsupported Spark model kind: {model_config.kind}")

    if config.use_class_weight and hasattr(estimator, "setWeightCol"):
        estimator.setWeightCol("class_weight")
    return estimator


def _extract_imputer_fill_values(pipeline_model, feature_columns: list[str]) -> dict[str, float]:
    imputer_model = pipeline_model.stages[0]
    if not hasattr(imputer_model, "surrogateDF"):
        return {feature: 0.0 for feature in feature_columns}
    row = imputer_model.surrogateDF.collect()[0].asDict()
    fill_values: dict[str, float] = {}
    for feature in feature_columns:
        value = row.get(feature, row.get(f"{feature}__imputed", 0.0))
        fill_values[feature] = float(value) if value is not None else 0.0
    return fill_values


def score_split(pipeline_model, df, config: SparkOfflineConfig, *, threshold: float = 0.5):
    from pyspark.ml.functions import vector_to_array
    from pyspark.sql import functions as F

    scored = pipeline_model.transform(df)
    if "probability" in scored.columns:
        score_column = vector_to_array(F.col("probability")).getItem(1)
    else:
        score_column = vector_to_array(F.col("raw_prediction")).getItem(1)
    return (
        scored.withColumn("score", score_column.cast("double"))
        .withColumn(
            "prediction",
            F.when(F.col("score") >= F.lit(float(threshold)), F.lit(1)).otherwise(F.lit(0)),
        )
        .select(
            config.label_column,
            "prediction",
            "score",
            *([config.attack_label_column] if config.attack_label_column in scored.columns else []),
        )
    )


def collect_prediction_arrays(
    scored_df,
    *,
    label_column: str,
    attack_label_column: str,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, pd.Series | None]:
    columns = [label_column, "prediction", "score"]
    include_attack_label = attack_label_column in scored_df.columns
    if include_attack_label:
        columns.append(attack_label_column)
    pdf = scored_df.select(*columns).toPandas()
    labels = pdf[label_column].astype(int).to_numpy()
    predictions = pdf["prediction"].astype(int).to_numpy()
    scores = pdf["score"].astype(float).to_numpy()
    attack_labels = pdf[attack_label_column] if include_attack_label else None
    return labels, predictions, scores, attack_labels


def predict_from_scores(scores: np.ndarray, threshold: float) -> np.ndarray:
    return np.where(np.asarray(scores, dtype=float) >= float(threshold), 1, 0).astype(int)


def binary_metrics_from_arrays(
    labels: np.ndarray,
    predictions: np.ndarray,
    scores: np.ndarray | None = None,
) -> tuple[dict[str, float], list[list[int]]]:
    y_true = np.asarray(labels, dtype=int)
    y_pred = np.asarray(predictions, dtype=int)

    tp = int(np.count_nonzero((y_true == 1) & (y_pred == 1)))
    tn = int(np.count_nonzero((y_true == 0) & (y_pred == 0)))
    fp = int(np.count_nonzero((y_true == 0) & (y_pred == 1)))
    fn = int(np.count_nonzero((y_true == 1) & (y_pred == 0)))

    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2.0 * precision * recall / (precision + recall) if (precision + recall) else 0.0
    accuracy = (tp + tn) / len(y_true) if len(y_true) else 0.0
    fpr = fp / (fp + tn) if (fp + tn) else 0.0
    fnr = fn / (fn + tp) if (fn + tp) else 0.0

    metrics = {
        "accuracy": round(float(accuracy), 6),
        "precision": round(float(precision), 6),
        "recall": round(float(recall), 6),
        "f1": round(float(f1), 6),
        "fpr": round(float(fpr), 6),
        "fnr": round(float(fnr), 6),
    }
    if scores is not None:
        metrics["roc_auc"] = round(float(roc_auc_from_scores(y_true, np.asarray(scores, dtype=float))), 6)
    return metrics, [[tn, fp], [fn, tp]]


def roc_auc_from_scores(labels: np.ndarray, scores: np.ndarray) -> float:
    y_true = np.asarray(labels, dtype=int)
    y_score = np.asarray(scores, dtype=float)
    n_pos = int(np.count_nonzero(y_true == 1))
    n_neg = int(np.count_nonzero(y_true == 0))
    if not n_pos or not n_neg:
        return 0.0
    ranks = pd.Series(y_score).rank(method="average").to_numpy()
    positive_rank_sum = float(ranks[y_true == 1].sum())
    auc = (positive_rank_sum - (n_pos * (n_pos + 1) / 2.0)) / (n_pos * n_neg)
    return max(0.0, min(1.0, auc))


def select_operating_threshold(
    labels: np.ndarray,
    scores: np.ndarray,
    *,
    budgets: tuple[float, ...],
    primary_budget: float,
) -> tuple[float, dict[str, dict[str, float]]]:
    y_true = np.asarray(labels, dtype=int)
    y_score = np.asarray(scores, dtype=float)
    operating_points: dict[str, dict[str, float]] = {}
    best_threshold = 0.5

    for budget in budgets:
        threshold = threshold_for_fpr_budget(y_true, y_score, float(budget))
        predictions = predict_from_scores(y_score, threshold)
        metrics, _ = binary_metrics_from_arrays(y_true, predictions, y_score)
        key = f"fpr_{int(round(budget * 100))}pct"
        operating_points[key] = {"threshold": round(float(threshold), 6), **metrics}
        if abs(float(budget) - float(primary_budget)) < 1e-9:
            best_threshold = threshold

    default_predictions = predict_from_scores(y_score, 0.5)
    default_metrics, _ = binary_metrics_from_arrays(y_true, default_predictions, y_score)
    operating_points["default_0.5"] = {"threshold": 0.5, **default_metrics}
    return float(best_threshold), operating_points


def threshold_for_fpr_budget(labels: np.ndarray, scores: np.ndarray, budget: float) -> float:
    y_true = np.asarray(labels, dtype=int)
    y_score = np.asarray(scores, dtype=float)
    n_pos = int(np.count_nonzero(y_true == 1))
    n_neg = int(np.count_nonzero(y_true == 0))
    if not n_pos or not n_neg:
        return 0.5

    order = np.argsort(-y_score, kind="mergesort")
    sorted_labels = y_true[order]
    sorted_scores = y_score[order]
    tp = np.cumsum(sorted_labels == 1)
    fp = np.cumsum(sorted_labels == 0)
    recall = tp / n_pos
    fpr = fp / n_neg

    candidates = np.flatnonzero(fpr <= float(budget))
    if len(candidates) == 0:
        return float(np.nextafter(sorted_scores[0], np.inf))
    candidate_recalls = recall[candidates]
    best_local = int(np.argmax(candidate_recalls))
    best_index = int(candidates[best_local])
    return float(sorted_scores[best_index])


def per_attack_recall_rows(
    labels: np.ndarray,
    predictions: np.ndarray,
    attack_labels: pd.Series | None,
    *,
    threshold: float,
    model_name: str,
    split_name: str,
) -> list[dict[str, Any]]:
    if attack_labels is None:
        return []
    frame = pd.DataFrame(
        {
            "label_binary": np.asarray(labels, dtype=int),
            "prediction": np.asarray(predictions, dtype=int),
            "label": attack_labels.astype(str).to_numpy(),
        }
    )
    rows: list[dict[str, Any]] = []
    attack_frame = frame[frame["label_binary"] == 1]
    for attack_type, group in attack_frame.groupby("label", sort=True):
        n_samples = int(len(group))
        detected = int(group["prediction"].sum())
        rows.append(
            {
                "split": split_name,
                "model": model_name,
                "attack_type": str(attack_type),
                "n_samples": n_samples,
                "detected": detected,
                "recall": round(detected / n_samples if n_samples else 0.0, 6),
                "threshold": round(float(threshold), 6),
            }
        )
    return rows


def save_predictions(scored_df, config: SparkOfflineConfig, model_name: str, split_name: str) -> None:
    output_path = config.evaluation_dir / f"predictions_{model_name}_{split_name}"
    if output_path.exists():
        shutil.rmtree(output_path)
    scored_df.write.mode("overwrite").parquet(str(output_path))


def evaluate_scored_split(
    scored_df,
    config: SparkOfflineConfig,
    model_name: str,
    split_name: str,
    threshold: float,
) -> dict[str, Any]:
    started = time.perf_counter()
    labels, predictions, scores, attack_labels = collect_prediction_arrays(
        scored_df,
        label_column=config.label_column,
        attack_label_column=config.attack_label_column,
    )
    elapsed = time.perf_counter() - started
    metrics, cm = binary_metrics_from_arrays(labels, predictions, scores)
    per_attack_rows = per_attack_recall_rows(
        labels,
        predictions,
        attack_labels,
        threshold=threshold,
        model_name=model_name,
        split_name=split_name,
    )
    _write_test_artifacts(config, split_name, model_name, threshold, metrics, cm, per_attack_rows)
    return {
        "split": split_name,
        "model": model_name,
        "n": int(len(labels)),
        "opt_threshold": round(float(threshold), 6),
        "fpr_budget": float(config.primary_fpr_budget),
        **metrics,
        "collect_latency_ms": round(elapsed * 1000.0, 1),
        "collect_throughput_rps": round(len(labels) / elapsed, 0) if elapsed > 0 else 0,
    }


def _write_model_calibration_artifacts(
    config: SparkOfflineConfig,
    model_name: str,
    validation_row: dict[str, Any],
    confusion_matrix: list[list[int]],
    per_attack_rows: list[dict[str, Any]],
) -> None:
    write_json(
        config.model_dir / f"valid_metrics_{model_name}.json",
        {
            key: value
            for key, value in validation_row.items()
            if key != "operating_points"
        },
    )
    write_json(
        config.model_dir / f"operating_points_{model_name}.json",
        {
            "model": model_name,
            "opt_threshold": validation_row["opt_threshold"],
            "fpr_budget": validation_row["fpr_budget"],
            "operating_points": validation_row["operating_points"],
        },
    )
    pd.DataFrame(
        confusion_matrix,
        index=["actual_benign", "actual_attack"],
        columns=["pred_benign", "pred_attack"],
    ).to_csv(config.model_dir / f"valid_confusion_matrix_{model_name}.csv")
    if per_attack_rows:
        pd.DataFrame(per_attack_rows).to_csv(
            config.model_dir / f"valid_per_attack_recall_{model_name}.csv",
            index=False,
        )


def _write_test_artifacts(
    config: SparkOfflineConfig,
    split_name: str,
    model_name: str,
    threshold: float,
    metrics: dict[str, float],
    confusion_matrix: list[list[int]],
    per_attack_rows: list[dict[str, Any]],
) -> None:
    write_json(
        config.evaluation_dir / f"test_metrics_{model_name}_{split_name}.json",
        {
            "dataset": split_name,
            "model": model_name,
            "opt_threshold": round(float(threshold), 6),
            "fpr_budget": float(config.primary_fpr_budget),
            **metrics,
            "per_attack_recall": per_attack_rows,
        },
    )
    pd.DataFrame(
        confusion_matrix,
        index=["actual_benign", "actual_attack"],
        columns=["pred_benign", "pred_attack"],
    ).to_csv(config.evaluation_dir / f"confusion_matrix_{model_name}_{split_name}.csv")
    if per_attack_rows:
        pd.DataFrame(per_attack_rows).to_csv(
            config.evaluation_dir / f"test_per_attack_recall_{model_name}_{split_name}.csv",
            index=False,
        )


def _write_best_model_artifacts(
    config: SparkOfflineConfig,
    best_model: TrainedSparkModel,
    best_row: dict[str, Any],
) -> None:
    best_payload = {
        "artifact_format": "spark_ml_pipeline",
        "best_model": best_model.config.name,
        "model_path": str(best_model.model_path),
        "opt_threshold": float(best_row["opt_threshold"]),
        "fpr_budget": float(config.primary_fpr_budget),
        "selection_metric": f"recall@FPR<={int(config.primary_fpr_budget * 100)}%",
        "class_weight": bool(config.use_class_weight),
        "calibration_metrics": {
            key: float(best_row[key])
            for key in ("accuracy", "precision", "recall", "f1", "fpr", "fnr")
            if key in best_row
        },
        "default_metrics": {
            key: float(best_row[f"default_{key}"])
            for key in ("recall", "precision", "f1", "fpr")
            if f"default_{key}" in best_row
        },
        "timing": {"train_time_s": float(best_model.train_time_s)},
    }
    write_json(config.model_dir / "best_model.json", best_payload)
    write_json(
        config.preprocessing_dir / "feature_manifest.json",
        {
            "version": 1,
            "artifact_format": "spark_ml_pipeline",
            "target_column": config.label_column,
            "feature_set": config.feature_set,
            "feature_columns": config.feature_columns,
            "n_features": len(config.feature_columns),
            "opt_threshold": float(best_row["opt_threshold"]),
            "fpr_budget": float(config.primary_fpr_budget),
            "impute_strategy": config.impute_strategy,
            "imputer_fill_values": best_model.fill_values,
            "best_model": best_model.config.name,
            "best_model_path": str(best_model.model_path),
        },
    )
