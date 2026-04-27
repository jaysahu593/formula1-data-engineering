# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../../utils/config"

# COMMAND ----------

# MAGIC %run "../../utils/common"

# COMMAND ----------

# MAGIC %run "../../utils/auth/adls_service_principal_auth"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest constructors.json file
# MAGIC
# MAGIC * Step 1 - Read the JSON file using the spark dataframe reader
# MAGIC
# MAGIC * Previoulsy we were using struct type and fields to define the schema, this time we will be using DDL type method to define the schema

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

constructors_df = spark.read\
                        .schema(constructors_schema)\
                        .json(f"{bronze_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 2 - Drop unwanted columns from the dataframe

# COMMAND ----------

from pyspark.sql.functions import *

constructors_drop_df = constructors_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 3 - Rename columns and add ingestion data

# COMMAND ----------

constructors_final_df = constructors_drop_df.withColumnRenamed("constructorId", "constructor_id") \
                                            .withColumnRenamed("constructorRef", "constructor_ref")\
                                            .withColumn("data_source", lit(v_data_source))\
                                            .withColumn("file_date", lit(v_file_date))

constructors_final_df = add_ingestion_date(constructors_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 4 - Write the output to required target.

# COMMAND ----------

# CREATING PARQUET FILES
constructors_final_df.write.mode("overwrite").parquet(f"{silver_folder_path}constructors")

# CREATING MANAGED TABLES 
constructors_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_catalog.f1_processed.constructors")

# CREATING EXTERNAL TABLES
constructors_final_df.write.mode("overwrite").format("delta").option('path', 'abfss://container@storageAccount.dfs.core.windows.net/f1_catalog_ext/f1_processed_ext/constructors').saveAsTable("f1_catalog_ext.f1_processed_ext.constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")