# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_catalog_ext.f1_raw.constructors(
# MAGIC   constructorId INT,
# MAGIC   constructorRef STRING,
# MAGIC   name STRING,
# MAGIC   nationality STRING,
# MAGIC   url STRING
# MAGIC )
# MAGIC USING JSON
# MAGIC OPTIONS ('path' 'abfss://container@storageAccount.dfs.core.windows.net/constructors.json')