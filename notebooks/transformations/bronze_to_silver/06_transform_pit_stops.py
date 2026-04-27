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

pit_stops_schema = StructType(fields = [
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("stop", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

pit_stops_df = spark.read\
                    .schema(pit_stops_schema)\
                    .option("multiLine", True)\
                    .json(f"{bronze_folder_path}/{v_file_date}/pit_stops.json")


# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 2 - Rename columns and add new columns
# MAGIC 1. Rename dirverId and raceId
# MAGIC 2. Add ingestion_date with current timestamp

# COMMAND ----------

pit_stops_renamed_df = pit_stops_df.withColumnRenamed("raceId", "race_id")\
                                   .withColumnRenamed("driverId", "driver_id")\
                                    .withColumn("data_source", lit(v_data_source))\
                                    .withColumn("file_date", lit(v_file_date))

pit_stops_renamed_df = add_ingestion_date(pit_stops_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 3 - Write to output 

# COMMAND ----------

# CREATING MANAGED TABLES
overwrite_partition(pit_stops_renamed_df, 'f1_catalog', 'f1_processed', 'pit_stops', 'race_id')

# CREATING EXTERNAL TABLES
overwrite_partition_ext(pit_stops_renamed_df, 'f1_catalog_ext', 'f1_processed_ext', 'pit_stops', 'race_id')

# CREATING PARQUET FILES
# Use below query with caution as it will overwrite the data in external location.

# pit_stops_renamed_df.write.mode("append").parquet(f"{silver_folder_path}pit_stops")

# COMMAND ----------

dbutils.notebook.exit("Success")