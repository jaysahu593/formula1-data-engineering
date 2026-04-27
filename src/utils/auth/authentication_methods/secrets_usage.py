# Databricks notebook source
# MAGIC %md
# MAGIC explore_dbutils_secrets_utility

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope = 'formula1-scope')

# COMMAND ----------

dbutils.secrets.get(scope = 'formula1-scope', key = 'formuladl-service-principle-secret')

# COMMAND ----------

