-- Databricks notebook source
-- MAGIC %md
-- MAGIC # DEMO for HOW TO CREATE TABLES

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %run "../includes/auth_ADLS_databrickswsj01stg"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Create circuits table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_catalog_ext.f1_raw.circuits(ciruitId INT,
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING
)
USING csv
OPTIONS (path "abfss://container@storageAccount.dfs.core.windows.net/circuits.csv", header true)

-- COMMAND ----------

SELECT * FROM f1_catalog_ext.f1_raw.circuits

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Create races table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_catalog_ext.f1_raw.races(
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date DATE,
  time STRING,
  url STRING
)
USING CSV
OPTIONS (path 'abfss://container@storageAccount.dfs.core.windows.net/races.csv', header true)

-- COMMAND ----------

SELECT * FROM f1_catalog_ext.f1_raw.races

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create tables for JSON files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###3. Create constructors table
-- MAGIC * Single Line JSON
-- MAGIC * Simple structure

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_catalog_ext.f1_raw.constructors(
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING
)
USING JSON
OPTIONS ('path' 'abfss://container@storageAccount.dfs.core.windows.net/constructors.json')

-- COMMAND ----------

SELECT * FROM f1_catalog_ext.f1_raw.constructors

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4. Create drivers table
-- MAGIC * Single Line JSON
-- MAGIC * Complex structure

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_catalog_ext.f1_raw.drivers(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename: STRING, surname: STRING>,
  dob DATE,
  nationality STRING,
  url STRING
)
USING JSON
OPTIONS (path 'abfss://container@storageAccount.dfs.core.windows.net/drivers.json')

-- COMMAND ----------

SELECT * FROM f1_catalog_ext.f1_raw.drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###5. Create results table
-- MAGIC * Single Line JSON
-- MAGIC * Simple structure

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_catalog_ext.f1_raw.results(
  resultId INT, 
  raceId INT, 
  driverId INT, 
  constructorId INT, 
  number INT, 
  grid INT,
  position INT, 
  positionText STRING, 
  positionOrder INT, 
  points FLOAT, 
  laps INT, 
  time STRING, 
  milliseconds INT, 
  fastestLap INT, 
  rank INT, 
  fastestLapTime STRING, 
  fastestLapSpeed FLOAT, 
  statusId INT
)
USING JSON
OPTIONS (path 'abfss://container@storageAccount.dfs.core.windows.net/results.json')

-- COMMAND ----------

SELECT * FROM f1_catalog_ext.f1_raw.results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###6. Create pit stops table
-- MAGIC * Multi Line JSON
-- MAGIC * Simple structure

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_catalog_ext.f1_raw.pit_stops(
  driverId INT,
  duration STRING,
  lap INT,
  milliseconds INT,
  raceId INT,
  stop INT,
  time STRING
)
USING JSON
OPTIONS (path 'abfss://container@storageAccount.dfs.core.windows.net/pit_stops.json', multiLine true)

-- COMMAND ----------

SELECT * FROM f1_catalog_ext.f1_raw.pit_stops

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create tables for list of files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###7. Create Lap Times Table
-- MAGIC * CSV files
-- MAGIC * Multiple files

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_catalog_ext.f1_raw.lap_times(
  raceId INT,
  driverId INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT
)
USING CSV
OPTIONS (path "abfss://container@storageAccount.dfs.core.windows.net/lap_times", header true)

-- COMMAND ----------

SELECT * FROM f1_catalog_ext.f1_raw.lap_times

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###7. Create Qualifying Table
-- MAGIC * JSON file
-- MAGIC * MultiLine JSON
-- MAGIC * Multiple files

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_catalog_ext.f1_raw.qualifying(
  constructorId INT,
  driverId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING,
  qualifyId INT,
  raceId INT
)
USING JSON
OPTIONS(path "abfss://container@storageAccount.dfs.core.windows.net/qualifying", multiLine true)


-- COMMAND ----------

SELECT * FROM f1_catalog_ext.f1_raw.qualifying

-- COMMAND ----------

