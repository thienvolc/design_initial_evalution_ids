from __future__ import annotations

def add_spark_ml_prediction_columns(
    prepared_df,
    *,
    model_path: str,
    threshold: float,
    model_name: str,
    feature_set: str,
    run_tag: str,
):
    from pyspark.sql import functions as F
    from pyspark.ml.functions import vector_to_array
    from pyspark.ml import PipelineModel

    model = PipelineModel.load(model_path)
    transformed_df = model.transform(prepared_df)
    if "probability" in transformed_df.columns:
        prediction_score_column = vector_to_array(F.col("probability")).getItem(1)
    elif "raw_prediction" in transformed_df.columns:
        prediction_score_column = vector_to_array(F.col("raw_prediction")).getItem(1)
    elif "rawPrediction" in transformed_df.columns:
        prediction_score_column = vector_to_array(F.col("rawPrediction")).getItem(1)
    else:
        prediction_score_column = F.col("spark_prediction").cast("double")

    prediction_label_condition = prediction_score_column >= F.lit(float(threshold))
    projected_columns = [F.col(column_name) for column_name in prepared_df.columns]
    projected_columns.extend(
        [
            prediction_score_column.alias("prediction_score"),
            F.when(prediction_label_condition, F.lit(1)).otherwise(F.lit(0)).alias("prediction_label"),
            F.current_timestamp().alias("emit_time"),
            F.lit(model_name).alias("model_name"),
            F.lit(feature_set).alias("feature_set"),
            F.lit(run_tag).alias("run_tag"),
            F.lit(float(threshold)).alias("threshold_used"),
        ]
    )
    return transformed_df.select(*projected_columns)


def add_passthrough_prediction_columns(
    prepared_df,
    *,
    model_name: str,
    feature_set: str,
    run_tag: str,
):
    from pyspark.sql import functions as F

    projected_columns = [F.col(column_name) for column_name in prepared_df.columns]
    projected_columns.extend(
        [
            F.lit(None).cast("double").alias("prediction_score"),
            F.lit(None).cast("int").alias("prediction_label"),
            F.current_timestamp().alias("emit_time"),
            F.lit(model_name).alias("model_name"),
            F.lit(feature_set).alias("feature_set"),
            F.lit(run_tag).alias("run_tag"),
            F.lit(None).cast("double").alias("threshold_used"),
        ]
    )
    return prepared_df.select(*projected_columns)
