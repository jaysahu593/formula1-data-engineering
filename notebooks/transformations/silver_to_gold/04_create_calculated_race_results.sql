-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.text("p_file_date", "2022-03-21")
-- MAGIC v_file_date = dbutils.widgets.get("p_file_date")

-- COMMAND ----------

-- USE CATALOG f1_catalog
-- SELECT * FROM f1_catalog.f1_processed.races
SHOW TABLES IN f1_catalog.f1_processed

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_catalog.f1_presentation.calculated_race_results
USING delta
AS
SELECT f1_catalog.f1_processed.races.race_year,
        constructors.name AS team_name,
        drivers.name As driver_name,
        results.position,
        results.points,
        11 - results.position AS calculated_points
  FROM f1_catalog.f1_processed.results
  JOIN f1_catalog.f1_processed.drivers ON (results.driver_id = drivers.driver_id)
  JOIN f1_catalog.f1_processed.constructors ON (results.constructor_id = constructors.constructor_id)
  JOIN f1_catalog.f1_processed.races ON (results.race_id = races.race_id)
  WHERE results.position <= 10

-- COMMAND ----------

SELECT COUNT(1) FROM f1_catalog.f1_presentation.calculated_race_results_merge

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_catalog_ext.f1_presentation_ext.calculated_race_results
USING delta
LOCATION 'abfss://contianer@storageAccount.dfs.core.windows.net/f1_catalog_ext/f1_presentation_ext/calculated_race_results'
AS
SELECT f1_catalog_ext.f1_processed_ext.races.race_year,
        constructors.name AS team_name,
        drivers.name As driver_name,
        results.position,
        results.points,
        11 - results.position AS calculated_points
  FROM f1_catalog_ext.f1_processed_ext.results
  JOIN f1_catalog_ext.f1_processed_ext.drivers ON (results.driver_id = drivers.driver_id)
  JOIN f1_catalog_ext.f1_processed_ext.constructors ON (results.constructor_id = constructors.constructor_id)
  JOIN f1_catalog_ext.f1_processed_ext.races ON (results.race_id = races.race_id)
  WHERE results.position <= 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## BY USING MERGE STATEMENT

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW race_results_updated
AS
SELECT f1_catalog_ext.f1_processed_ext.races.race_year,
        constructors.name AS team_name,
        drivers.driver_id,
        drivers.name As driver_name,
        races.race_id,
        results.position,
        results.points,
        11 - results.position AS calculated_points
  FROM f1_catalog_ext.f1_processed_ext.results
  JOIN f1_catalog_ext.f1_processed_ext.drivers ON (results.driver_id = drivers.driver_id)
  JOIN f1_catalog_ext.f1_processed_ext.constructors ON (results.constructor_id = constructors.constructor_id)
  JOIN f1_catalog_ext.f1_processed_ext.races ON (results.race_id = races.race_id)
  WHERE results.position <= 10
  AND results.file_date = '${p_file_date}'

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_catalog.f1_presentation.calculated_race_results_merge(
  race_year INT,
  team_name STRING,
  driver_id INT,
  driver_name STRING,
  race_id INT,
  position INT,
  points INT,
  calculated_points INT,
  created_date TIMESTAMP,
  updated_date TIMESTAMP
)

-- COMMAND ----------

MERGE INTO f1_catalog.f1_presentation.calculated_race_results_merge tgt
USING race_results_updated upd
ON (tgt.driver_id = upd.driver_id AND tgt.race_id = upd.race_id)
WHEN MATCHED THEN
  UPDATE SET tgt.position = upd.position,
             tgt.points = upd.points,
             tgt.calculated_points = upd.calculated_points,
             tgt.updated_date = current_timestamp
  WHEN NOT MATCHED 
  THEN INSERT (race_year, team_name, driver_id, driver_name, race_id, position, calculated_points, created_date)
  VALUES (race_year, team_name, driver_id, driver_name, race_id, position, calculated_points, current_timestamp)

-- COMMAND ----------

