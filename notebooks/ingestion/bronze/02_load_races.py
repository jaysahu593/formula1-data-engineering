# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_catalog_ext.f1_raw.races(
# MAGIC   raceId INT,
# MAGIC   year INT,
# MAGIC   round INT,
# MAGIC   circuitId INT,
# MAGIC   name STRING,
# MAGIC   date DATE,
# MAGIC   time STRING,
# MAGIC   url STRING
# MAGIC )
# MAGIC USING CSV
# MAGIC OPTIONS (path 'abfss://container@storageAccount.dfs.core.windows.net/races.csv', header true)

# COMMAND ----------

