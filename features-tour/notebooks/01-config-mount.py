# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Notebook setting up the mount points

# COMMAND ----------

import os

#  Declare the root path of our lakes
sourceLakeRootPath = os.getenv('LAKE_SOURCE')
destinationLakeRootPath = os.getenv('LAKE_DESTINATION')

# COMMAND ----------

#  Extra config for doing passthrough authentication on mounts
configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class":   spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

# COMMAND ----------

# Mount Source
dbutils.fs.mount(
  source = sourceLakeRootPath,
  mount_point = "/mnt/source",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls /mnt/source

# COMMAND ----------

# Mount Destination
dbutils.fs.mount(
  source = destinationLakeRootPath,
  mount_point = "/mnt/destination",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls /mnt/destination

# COMMAND ----------

