-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_catalog.f1_presentation

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_catalog_ext.f1_presentation_ext

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * made changes in processed notebook to create a managed table as well.
-- MAGIC * Similarly will make changes in presentation/trans notebook to create managed tables here as well.
-- MAGIC * eg: final_df.write.mode("overwrite").format("delta").saveAsTable("f1_catalog.f1_presentation.constructor_standings")

-- COMMAND ----------

desc extended f1_catalog.f1_presentation.constructor_standings

-- COMMAND ----------

