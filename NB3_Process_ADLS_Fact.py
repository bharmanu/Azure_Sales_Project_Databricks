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

dbutils.fs.ls("mnt/ap203storageaccount/data/fact")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

dataSchema = StructType([   
    StructField("ProductKey", StringType(), True),
    StructField("OrderDateKey", StringType(), True),
    StructField("DueDateKey", StringType(), True),
    StructField("ShipDateKey", StringType(), True),
    StructField("CustomerKey", StringType(), True),
    StructField("PromotionKey", StringType(), True),
    StructField("CurrencyKey", StringType(), True),
    StructField("SalesTerritoryKey", StringType(), True),
    StructField("SalesOrderNumber", StringType(), True),
    StructField("SalesOrderLineNumber", StringType(), True),
    StructField("RevisionNumber", StringType(), True),
    StructField("OrderQuantity", StringType(), True),
    StructField("UnitPrice", StringType(), True),
    StructField("ExtendedAmount", StringType(), True),
    StructField("UnitPriceDiscountPct", StringType(), True),
    StructField("DiscountAmount", StringType(), True),
    StructField("ProductStandardCost", StringType(), True),
    StructField("TotalProductCost", StringType(), True),
    StructField("SalesAmount", StringType(), True),
    StructField("TaxAmt", StringType(), True),
    StructField("Freight", StringType(), True),
    StructField("CarrierTrackingNumber", StringType(), True),
    StructField("CustomerPONumber", StringType(), True),
    StructField("OrderDate", StringType(), True),
    StructField("DueDate", StringType(), True),
    StructField("ShipDate", StringType(), True)
    ])

# COMMAND ----------


df_FactInternetSales = spark.read.schema(dataSchema).csv("/mnt/ap203storageaccount/data/fact",sep="|", header=True)

# COMMAND ----------

display(df_FactInternetSales)

# COMMAND ----------

from pyspark.sql.functions import col, to_date
from pyspark.sql.types import IntegerType,LongType

df_selected = df_FactInternetSales.select(
    col("ProductKey"),
    col("OrderDateKey"), 
    col("CustomerKey"), 
    col("SalesTerritoryKey"), 
    col("SalesOrderNumber"), 
    col("SalesOrderLineNumber"), 
    col("OrderQuantity"), 
    col("ProductStandardCost"), 
    col("SalesAmount"), 
    (col("OrderDate"))
)

display(df_selected)


# COMMAND ----------

df_selected.write.mode("overwrite").parquet("/mnt/ap203storageaccount/data/fact/processed/FactInternetSales")
