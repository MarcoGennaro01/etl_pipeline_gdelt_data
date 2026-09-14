from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from xgboost.spark import SparkXGBRegressor


def get_session():
    spark = SparkSession.Builder.GetOrCreate()
    return spark


def read_data_bigquery(input_data_table_id):
    spark = get_session()
    df = spark.read.format("bigquery").option("table", input_data_table_id).load()

    df.dropna(
        subset=[
            "Prev1_WeightedGoldsteinScale",
            "Prev2_WeightedGoldsteinScale",
            "Target_AvgTone",
            "Prev1_WeightedAvgTone",
            "Prev2_WeightedAvgTone",
        ]
    )
    return df


def train_model_XGB(df_train_set, workers=2):
    spark = SparkSession.builder.getOrCreate()
    feature_cols = [
        col
        for col in df_train_set.columns
        if col not in ["Target_AvgTone", "Country_code", "week"]
    ]
    assembler = VectorAssembler(
        inputCols=feature_cols, outputCol="features", handleInvalid="keep"
    )
    df_train_set = assembler.transform(df_train_set)
    regressor = SparkXGBRegressor(
        features_col="features", label_col="Target_AvgTone", num_workers=workers
    )
    model = regressor.fit(df_train_set)
    return model
