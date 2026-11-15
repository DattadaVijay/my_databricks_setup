# Databricks notebook source
from pyspark.sql import functions as F
from databricks.feature_engineering import FeatureEngineeringClient
model_uri = "models:/finance_domain.default.fraud_detection_model/1"

fe = FeatureEngineeringClient()
# COMMAND ----------
new_transactions = spark.createDataFrame(
    [
        ("T013", "C101", 6000.0, "2026-11-11 10:00:00"),
        ("T014", "C102", 150.0, "2026-11-11 11:00:00")
    ],
    ["transaction_id", "customer_id", "amount", "transaction_time"]
).withColumn(
    "transaction_time",
    F.to_timestamp("transaction_time")
)

# COMMAND ----------
predictions = fe.score_batch(
    model_uri=model_uri,
    df=new_transactions
)

display(predictions)