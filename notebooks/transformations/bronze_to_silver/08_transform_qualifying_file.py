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
# MAGIC ###Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

qualifying_schema = StructType(fields = [
                                        StructField("qualifyId", IntegerType(), False),
                                        StructField("raceId", IntegerType(), True),
                                        StructField("driverId", IntegerType(), True),
                                        StructField("constructorId", IntegerType(), True),
                                        StructField("number", IntegerType(), True),
                                        StructField("position", IntegerType(), True),
                                        StructField("q1", StringType(), True),
                                        StructField("q2", StringType(), True),
                                        StructField("q3", StringType(), True)
])


# COMMAND ----------

qualifying_df = spark.read\
                    .schema(qualifying_schema)\
                    .option("multiLine", True)\
                    .json(f"{bronze_folder_path}/{v_file_date}/qualifying/")

# You can select folder itself or you can use wildcard to select all files in folder
# .json(f"{bronze_folder_path}/qualifying/qualifying*.json")


# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 2 - Rename columns and add new columns
# MAGIC 1. Rename dirverId and raceId
# MAGIC 2. Add ingestion_date with current timestamp

# COMMAND ----------

qualifying_renamed_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id")\
                                    .withColumnRenamed("raceId", "race_id")\
                                    .withColumnRenamed("driverId", "driver_id")\
                                    .withColumnRenamed("constructorId", "constructor_id")\
                                    .withColumn("data_source", lit(v_data_source))\
                                    .withColumn("file_date", lit(v_file_date))

qualifying_renamed_df = add_ingestion_date(qualifying_renamed_df)

# COMMAND ----------

# HANDLING NULLS
qualifying_renamed_df = qualifying_renamed_df.replace("\\N", None)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 3 - Write to output

# COMMAND ----------

# CREATING MANAGED TABLES
overwrite_partition(qualifying_renamed_df, "f1_catalog", "f1_processed", "qualifying", "race_id")

# CREATING EXTERNAL TABLES
overwrite_partition_ext(qualifying_renamed_df, "f1_catalog_ext", "f1_processed_ext", "qualifying", "race_id")

# CREATING PARQUET FILES
# Use below query with caution as it will overwrite the data in external location.

# qualifying_renamed_df.write.mode("append").parquet(f"{silver_folder_path}qualifying")

# COMMAND ----------

dbutils.notebook.exit("Success")