# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_catalog_ext.f1_raw.qualifying(
# MAGIC   constructorId INT,
# MAGIC   driverId INT,
# MAGIC   number INT,
# MAGIC   position INT,
# MAGIC   q1 STRING,
# MAGIC   q2 STRING,
# MAGIC   q3 STRING,
# MAGIC   qualifyId INT,
# MAGIC   raceId INT
# MAGIC )
# MAGIC USING JSON
# MAGIC OPTIONS(path "abfss://container@storageAccount.dfs.core.windows.net/qualifying", multiLine true)