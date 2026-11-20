# Databricks notebook source

import yaml
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