# Databricks notebook source
# MAGIC %md
# MAGIC Databricks file system utilities

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

# MAGIC %md
# MAGIC Sample datasets

# COMMAND ----------

dbutils.fs.ls('/databricks-datasets/')

# COMMAND ----------

for files in dbutils.fs.ls('/databricks-datasets/structured-streaming/events/'):
    print(files)

# COMMAND ----------

dbutils.fs.ls('/mnt/ap203storageaccount/data/dimension')

# COMMAND ----------

for files in dbutils.fs.ls('/mnt/ap203storageaccount/data/fact/raw'):
    print(files.name)

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.help('head')
