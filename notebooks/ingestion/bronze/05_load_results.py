# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_catalog_ext.f1_raw.results(
# MAGIC   resultId INT, 
# MAGIC   raceId INT, 
# MAGIC   driverId INT, 
# MAGIC   constructorId INT, 
# MAGIC   number INT, 
# MAGIC   grid INT,
# MAGIC   position INT, 
# MAGIC   positionText STRING, 
# MAGIC   positionOrder INT, 
# MAGIC   points FLOAT, 
# MAGIC   laps INT, 
# MAGIC   time STRING, 
# MAGIC   milliseconds INT, 
# MAGIC   fastestLap INT, 
# MAGIC   rank INT, 
# MAGIC   fastestLapTime STRING, 
# MAGIC   fastestLapSpeed FLOAT, 
# MAGIC   statusId INT
# MAGIC )
# MAGIC USING JSON
# MAGIC OPTIONS (path 'abfss://container@storageAccount.dfs.core.windows.net/results.json')