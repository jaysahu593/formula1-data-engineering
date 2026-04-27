# Databricks notebook source
client_id = dbutils.secrets.get(scope='formula1-scope', key ='formuladl-service-principle-client-id')

tenant_id = dbutils.secrets.get(scope='formula1-scope', key ='formuladl-service-principle-tenant-id')

client_secret = dbutils.secrets.get(scope="formula1-scope", key="formuladl-service-principle-secret")

spark.conf.set("fs.azure.account.auth.type.storageAccount.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.storageAccount.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.storageAccount.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.storageAccount.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.storageAccount.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

