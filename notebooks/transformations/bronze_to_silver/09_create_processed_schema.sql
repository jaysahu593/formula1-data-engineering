-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Here I have used a different naming convention for Medallion architecture.
-- MAGIC
-- MAGIC * **BRONZE <-> RAW**
-- MAGIC * **SILVER <-> PROCESSED**
-- MAGIC * **GOLD <-> PRESENTATION**

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS f1_catalog
MANAGED LOCATION 'abfss://container@storageAccount.dfs.core.windows.net/'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * BRONZE OR RAW LAYER

-- COMMAND ----------

-- MANAGED DB
CREATE SCHEMA IF NOT EXISTS f1_catalog.f1_raw;

--MANAGED DB OF EXTERNAL CATALOG
CREATE SCHEMA IF NOT EXISTS f1_catalog_ext.f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * SILVER OR PROCESSED LAYER

-- COMMAND ----------


-- MANAGED DB
CREATE SCHEMA IF NOT EXISTS f1_catalog.f1_processed;

--MANAGED DB OF EXTERNAL CATALOG
CREATE SCHEMA IF NOT EXISTS f1_catalog_ext.f1_processed;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * GOLD OR PRESENTATION LAYER

-- COMMAND ----------

-- MANAGED DB
CREATE SCHEMA IF NOT EXISTS f1_catalog.f1_presentation;

--MANAGED DB OF EXTERNAL CATALOG
CREATE SCHEMA IF NOT EXISTS f1_catalog_ext.f1_presentation;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now we need to create tables for this, so either we can use spark sql or pyspark, in this let's use pyspark
-- MAGIC
-- MAGIC * Made changes in all the ingestion notebooks to also create a managed table
-- MAGIC * eg: df_name.write.mode("overwrite").format("delta").saveAsTable("f1_catalog.f1_processed.table_name")
-- MAGIC
-- MAGIC * Similarly made changes in presentation layer as well.

-- COMMAND ----------

