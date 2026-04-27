# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_catalog_ext.f1_raw.lap_times(
# MAGIC   raceId INT,
# MAGIC   driverId INT,
# MAGIC   lap INT,
# MAGIC   position INT,
# MAGIC   time STRING,
# MAGIC   milliseconds INT
# MAGIC )
# MAGIC USING CSV
# MAGIC OPTIONS (path "abfss://container@storageAccount.dfs.core.windows.net/lap_times", header true)