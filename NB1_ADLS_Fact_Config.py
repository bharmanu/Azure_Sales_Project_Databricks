# Databricks notebook source
service_credential = dbutils.secrets.get(scope="Databricks_KeyVault",key="databricks-Secret")

spark.conf.set("fs.azure.account.auth.type.ap203storageaccount.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.ap203storageaccount.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.ap203storageaccount.dfs.core.windows.net", "69812d2b-8e9d-466d-94a2-068a37b1cf30")
spark.conf.set("fs.azure.account.oauth2.client.secret.ap203storageaccount.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.ap203storageaccount.dfs.core.windows.net", "https://login.microsoftonline.com/56b62dbd-1ca2-4eb6-a10e-93cc69f0bdfe/oauth2/token")



# COMMAND ----------

filepath_FactInternetSales ="abfss://data@ap203storageaccount.dfs.core.windows.net/fact/FactInternetSales.parquet"
filepath_FactInternetSalesReason ="abfss://data@ap203storageaccount.dfs.core.windows.net/fact/FactInternetSalesReason.parquet"

# COMMAND ----------

df_FactInternetSales = spark.read.format("parquet").load(filepath_FactInternetSales).limit(10)
df_FactInternetSalesReason = spark.read.format("parquet").load(filepath_FactInternetSalesReason).limit(10)

# COMMAND ----------

display(df_FactInternetSales)

# COMMAND ----------

display(df_FactInternetSalesReason)

# COMMAND ----------

storage_account_name = "ap203storageaccount"
storage_account_access_key_scope = "Databricks_KeyVault"
storage_account_access_key_name = "storageaccount-key"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    dbutils.secrets.get(scope=storage_account_access_key_scope, key=storage_account_access_key_name)
)

# COMMAND ----------

filepath_DimProduct ="abfss://data@ap203storageaccount.dfs.core.windows.net/dimension/DimProduct.csv"

# COMMAND ----------

df_DimProduct = spark.read.format("csv").option("sep", "|").load(filepath_DimProduct).limit(10)
display(df_DimProduct)
