# Databricks notebook source

import yaml
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
warehouse_id = "ece081f3bd7cb8a7"  # Serverless Starter Warehouse

contract_path = dbutils.widgets.get("contract_path")

with open(contract_path) as f:
    contract = yaml.safe_load(f)

for table in contract.get("schema", []):
    table_name = table.get("physicalName", table["name"])

    for column in table.get("properties", []):
        tags = column.get("tags", [])

        classification = next(
            (tag for tag in ["direct_pii", "pseudonymous"] if tag in tags),
            None
        )

        if not classification:
            continue

        sql = f"""
        SET TAG ON COLUMN
          `finance_domain`.`default`.`{table_name}`.`{column['name']}`
          `data_classification` = '{classification}'
        """

        w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=sql,
            wait_timeout="30s"
        )

        print(f"{table_name}.{column['name']} → {classification}")

# COMMAND ----------
%sql
CREATE OR REPLACE FUNCTION finance_domain.default.mask_direct_pii(v STRING)
RETURNS STRING
RETURN IF(v IS NULL, NULL, '****');

CREATE OR REPLACE FUNCTION finance_domain.default.mask_pseudonymous(v STRING)
RETURNS STRING
RETURN IF(v IS NULL, NULL, CONCAT('****', RIGHT(v, 4)));
# COMMAND ----------
%sql
CREATE POLICY mask_direct_pii_policy
ON SCHEMA finance_domain.default
COMMENT 'Mask direct PII'
COLUMN MASK finance_domain.default.mask_direct_pii
TO `account users` EXCEPT `fraud_pii_readers`
FOR TABLES
MATCH COLUMNS
  has_tag_value('data_classification', 'direct_pii') AS col
ON COLUMN col;

# COMMAND ----------
%sql
CREATE POLICY mask_pseudonymous_policy
ON SCHEMA finance_domain.default
COMMENT 'Mask customer identifiers'
COLUMN MASK finance_domain.default.mask_pseudonymous
TO `account users` EXCEPT `fraud_pii_readers`
FOR TABLES
MATCH COLUMNS
  has_tag_value('data_classification', 'pseudonymous') AS col
ON COLUMN col;

# COMMAND ----------
%sql
SELECT customer_id, customer_name
FROM finance_domain.default.silver_customers
LIMIT 5;
