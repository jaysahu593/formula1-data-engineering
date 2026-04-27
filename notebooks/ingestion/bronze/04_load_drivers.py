# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_catalog_ext.f1_raw.drivers(
# MAGIC   driverId INT,
# MAGIC   driverRef STRING,
# MAGIC   number INT,
# MAGIC   code STRING,
# MAGIC   name STRUCT<forename: STRING, surname: STRING>,
# MAGIC   dob DATE,
# MAGIC   nationality STRING,
# MAGIC   url STRING
# MAGIC )
# MAGIC USING JSON
# MAGIC OPTIONS (path 'abfss://container@storageAccount.dfs.core.windows.net/drivers.json')