# Databricks notebook source

from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup

fe = FeatureEngineeringClient()

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

features_df = spark.table(
    f"{catalog}.{schema}.gold_feature_history"
)

display(features_df)

fe.create_table(
    name=f"{catalog}.{schema}.customer_feature_store",
    primary_keys=["customer_id", "feature_timestamp"],
    timeseries_columns="feature_timestamp",
    df=features_df,
    description="Customer historical fraud detection features"
)


# COMMAND ----------

observations = spark.table(
    "f"{catalog}.{schema}.gold_training_observations"
)
display(observations)

# COMMAND ----------

lookup = FeatureLookup(
    table_name=f"{catalog}.{schema}.customer_feature_store",
    lookup_key="customer_id",
    timestamp_lookup_key="transaction_time",
    feature_names=[
        "transaction_count_7d",
        "avg_amount_7d",
        "max_amount_7d",
        "account_age_days"
    ]
)

training_set = fe.create_training_set(
    df=observations,
    feature_lookups=[lookup],
    label="is_fraud",
    exclude_columns=[
        "transaction_id",
        "customer_id",
        "transaction_time"
    ]
)

training_df = training_set.load_df()

display(training_df)

# COMMAND ----------

import mlflow
import mlflow.sklearn

from sklearn.ensemble import RandomForestClassifier

pdf = training_df.toPandas()

X = pdf.drop(columns=["is_fraud"])
y = pdf["is_fraud"]

print(X.head())
print(y.value_counts())

model = RandomForestClassifier(
    n_estimators=100,
    max_depth=5,
    random_state=42
)

model.fit(X, y)

print("Model training completed")

mlflow.set_registry_uri("databricks-uc")

with mlflow.start_run():

    fe.log_model(
        model=model,
        artifact_path="fraud_model",
        flavor=mlflow.sklearn,
        training_set=training_set,
        registered_model_name=f"{catalog}.{schema}.fraud_detection_model"
    )
