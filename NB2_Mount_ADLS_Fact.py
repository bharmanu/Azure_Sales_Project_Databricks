# Databricks notebook source
service_credential = dbutils.secrets.get(scope="Databricks_KeyVault",key="databricks-Secret")

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "69812d2b-8e9d-466d-94a2-068a37b1cf30",
           "fs.azure.account.oauth2.client.secret": service_credential,
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/56b62dbd-1ca2-4eb6-a10e-93cc69f0bdfe/oauth2/token"}

# COMMAND ----------

service_credential = dbutils.secrets.get(scope="Databricks_KeyVault",key="databricks-Secret")

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "69812d2b-8e9d-466d-94a2-068a37b1cf30",
           "fs.azure.account.oauth2.client.secret": service_credential,
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/56b62dbd-1ca2-4eb6-a10e-93cc69f0bdfe/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://data@ap203storageaccount.dfs.core.windows.net/",
  mount_point = "/mnt/ap203storageaccount/data",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("mnt/ap203storageaccount/data/fact")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

dataSchema = StructType([   
    StructField("ProductKey", StringType(), True),
    StructField("OrderDateKey", StringType(), True),
    StructField("DueDateKey", StringType(), True),
    StructField("ShipDateKey", IntegerType(), True),
    StructField("CustomerKey", IntegerType(), True),
    StructField("PromotionKey", IntegerType(), True),
    StructField("CurrencyKey", IntegerType(), True),
    StructField("SalesTerritoryKey", IntegerType(), True),
    StructField("SalesOrderNumber", StringType(), True),
    StructField("SalesOrderLineNumber", IntegerType(), True),
    StructField("RevisionNumber", IntegerType(), True),
    StructField("OrderQuantity", IntegerType(), True),
    StructField("UnitPrice", IntegerType(), True),
    StructField("ExtendedAmount", IntegerType(), True),
    StructField("UnitPriceDiscountPct", IntegerType(), True),
    StructField("DiscountAmount", IntegerType(), True),
    StructField("ProductStandardCost", IntegerType(), True),
    StructField("TotalProductCost", IntegerType(), True),
    StructField("SalesAmount", IntegerType(), True),
    StructField("TaxAmt", IntegerType(), True),
    StructField("Freight", IntegerType(), True),
    StructField("CarrierTrackingNumber", StringType(), True),
    StructField("CustomerPONumber", StringType(), True),
    StructField("OrderDate", TimestampType(), True),
    StructField("DueDate", TimestampType(), True),
    StructField("ShipDate", TimestampType(), True)
    ])

# COMMAND ----------


df_FactInternetSales = spark.read.schema(dataSchema).parquet("/mnt/ap203storageaccount/data/fact").limit(10)

# COMMAND ----------

display(df_FactInternetSales)
