
import sys
import os
import datetime
import json

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from xgboost.spark import SparkXGBClassifier
from google.cloud import storage

def get_session():
    spark = (
        SparkSession.builder
        .appName("ML_job_gdelt_data")
        .getOrCreate()
    )
    return spark

def read_clean_data(input_data_table_id, spark):
    df = spark.read.format("bigquery").option("table", input_data_table_id).load()

    df_clean = df.dropna(
        subset=[
            "avg_tone_ma_5w",
            "goldstein_ma_5w",
            "Target",
            "goldstein_sd_5w",
            "avg_tone_sd_5w",
        ]
    )
    return df_clean

def prepare_split_data(df, cutoff_date="2025-01-01"):
    feature_cols = [
        col
        for col in df.columns
        if col not in ["Target", "Country_code", "week"]
    ]

    assembler = VectorAssembler(
        inputCols=feature_cols, outputCol="features", handleInvalid="keep"
    )
    df_transformed = assembler.transform(df)

    train_df = df_transformed.filter(df_transformed["week"] < cutoff_date).cache()
    test_df = df_transformed.filter(df_transformed["week"] >= cutoff_date).cache()

    train_df.count()
    test_df.count()

    return train_df, test_df, feature_cols

def train_model_XGB(train_df, workers=8):
    classifier = SparkXGBClassifier(
        label_col="Target",
        num_workers=workers,
        eval_metric="logloss",
        tree_method="hist",
        max_depth=6,
        learning_rate=0.1,
        subsample=0.8,
        scale_pos_weight=1.61
    )

    model = classifier.fit(train_df)

    return model

def evaluate_model(model, test_df):
    predictions = model.transform(test_df).cache()

    evaluator = BinaryClassificationEvaluator(
        labelCol="Target",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC",
    )
    auc_score = round(float(evaluator.evaluate(predictions)), 4)
    accuracy = round(float((tp + tn) / total), 4) if total > 0 else 0.0
    specificity = round(float(tn / (tn + fp)), 4) if (tn + fp) > 0 else 0.0

    metrics = {
        "auc_roc": auc_score,
        "accuracy": accuracy,
        "specificity": specificity
    }

    return metrics

def export_metrics(metrics, bucket_name, blob_path):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    blob.upload_from_string(
        data=json.dumps(metrics, indent=4),
        content_type="application/json"
    )

def save_model(bucket_name, model, feature_cols):
    booster = model.get_booster()

    booster.feature_names = feature_cols
    booster.feature_types = ["q"] * len(feature_cols)

    raw_json_str = booster.save_raw(raw_format="json").decode("utf-8")
    model_dict = json.loads(raw_json_str)

    if "learner" in model_dict:
        model_dict["learner"]["feature_names"] = feature_cols
        model_dict["learner"]["feature_types"] = ["q"] * len(feature_cols)

    updated_json_bytes = json.dumps(model_dict, indent=2).encode("utf-8")

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob("models/xgboost_model.json")

    blob.upload_from_string(updated_json_bytes, content_type="application/json")

if __name__ == "__main__":
    PROJECT_ID = sys.argv[1]
    DATASET_ID = sys.argv[2]
    DM_TABLE_NAME = sys.argv[3]
    BUCKET_NAME = sys.argv[4].replace("gs://", "").strip("/")

    DM_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{DM_TABLE_NAME}"

    spark = get_session()

    try:
        df_clean = read_clean_data(DM_TABLE_ID, spark)
        tr_df, te_df, feature_cols = prepare_split_data(df_clean)
        model = train_model_XGB(tr_df, workers=8)

        metrics_blob_path = "metrics/xgboost_metrics.json"
        export_metrics(evaluate_model(model, te_df), BUCKET_NAME, metrics_blob_path)

        save_model(BUCKET_NAME, model, feature_cols)

    finally:
        spark.stop()
