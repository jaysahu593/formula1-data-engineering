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
# MAGIC ### Ingest Drivers.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

name_schmea = StructType(fields = [StructField('forename', StringType(), True),
                                    StructField('surname', StringType(), True)]
                                   )

# COMMAND ----------

drivers_schema = StructType(fields= [StructField("driverId", IntegerType(), False),
                                     StructField("driverRef", StringType(), True),
                                     StructField("number", IntegerType(), True),
                                     StructField("code", StringType(), True),
                                     StructField("name", name_schmea),
                                     StructField("dob", DateType(), True),
                                     StructField("nationality", StringType(), True),
                                     StructField("url", StringType(), True)
                                    ])



# COMMAND ----------

drivers_df = spark.read.schema(drivers_schema).json(f"{bronze_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Step 2 - Rename columns and add new columns
# MAGIC 1. driverId reanmed to driver_id
# MAGIC 2. driverRef renamed to driver_ref
# MAGIC 3. ingestion date added
# MAGIC 4. name added with concatenation of forename and surname

# COMMAND ----------

driver_with_columns_df = drivers_df.withColumnRenamed('driverId', 'driver_id')\
                                    .withColumnRenamed("driverRef", "driver_ref")\
                                    .withColumn("name", concat(col('name.forename'), lit(' '), col('name.surname')))\
                                    .withColumn("data_source", lit(v_data_source))\
                                    .withColumn("file_date", lit(v_file_date))

driver_with_columns_df = add_ingestion_date(driver_with_columns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Step 3 - Drop the unwanted columns
# MAGIC
# MAGIC 1. name.forename
# MAGIC 2. name.surname.
# MAGIC 3. url
# MAGIC
# MAGIC
# MAGIC note: as we have concatinated forename and surname to name column, so there is no need to drop these columns

# COMMAND ----------

drivers_final_df = driver_with_columns_df.drop('url')

# COMMAND ----------

# CREATING PARQUET FILE
drivers_final_df.write.mode("overwrite").parquet(f"{silver_folder_path}drivers")

# CREATING MANAGED TABLES
drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_catalog.f1_processed.drivers")

# CREATING EXTERNAL TABLES
drivers_final_df.write.mode("overwrite").format("delta").option('path', 'abfss://container@storageAccount.dfs.core.windows.net/f1_catalog_ext/f1_processed_ext/drivers').saveAsTable("f1_catalog_ext.f1_processed_ext.drivers")

# COMMAND ----------

dbutils.notebook.exit("Success")