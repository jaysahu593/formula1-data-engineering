# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_catalog_ext.f1_raw.pit_stops(
# MAGIC   driverId INT,
# MAGIC   duration STRING,
# MAGIC   lap INT,
# MAGIC   milliseconds INT,
# MAGIC   raceId INT,
# MAGIC   stop INT,
# MAGIC   time STRING
# MAGIC )
# MAGIC USING JSON
# MAGIC OPTIONS (path 'abfss://container@storageAccount.dfs.core.windows.net/pit_stops.json', multiLine true)