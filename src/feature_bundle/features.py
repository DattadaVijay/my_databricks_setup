# Databricks notebook source

from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup

fe = FeatureEngineeringClient()

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

