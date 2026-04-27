# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_catalog_ext.f1_raw.circuits(ciruitId INT,
# MAGIC circuitRef STRING,
# MAGIC name STRING,
# MAGIC location STRING,
# MAGIC country STRING,
# MAGIC lat DOUBLE,
# MAGIC lng DOUBLE,
# MAGIC alt INT,
# MAGIC url STRING
# MAGIC )
# MAGIC USING csv
# MAGIC OPTIONS (path "abfss://container@storageAccount.dfs.core.windows.net/circuits.csv", header true)