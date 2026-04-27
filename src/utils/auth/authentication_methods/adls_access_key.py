# Databricks notebook source
# MAGIC %md
# MAGIC ***Access Azure Data Lake using Access Key***
# MAGIC 1. Set Spark configuraiton fs.azure.account.key
# MAGIC 2. List files form demo container
# MAGIC 3. Read Data from circuits.csv file

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.storageAcoount.dfs.core.windows.net",
    "key_secret"
)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@databrickswsj01stg.dfs.core.windows.net/circuits.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC Its failing because in Storage acc, access using key is disabled for us. But this is the process to connect