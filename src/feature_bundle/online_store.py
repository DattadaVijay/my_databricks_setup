# Databricks notebook source

from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()

online_store = fe.create_online_store(
    name="fraud-online-store",
    capacity="CU_1"
)

# COMMAND ----------

store = fe.get_online_store(
    name="fraud-online-store"
)

print(store.name)
print(store.state)

# COMMAND ----------
# MAGIC %sql
ALTER TABLE finance_domain.default.customer_feature_store
SET TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true'
);

# COMMAND ----------
online_store = fe.get_online_store(
    name="fraud-online-store"
)

result = fe.publish_table(
    online_store=online_store,
    source_table_name="finance_domain.default.customer_feature_store",
    online_table_name="finance_domain.default.customer_features_online",
    publish_mode="TRIGGERED"
)
