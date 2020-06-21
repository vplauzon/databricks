# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook demoing Structured Streaming

# COMMAND ----------

from pyspark.sql.types import *

# We need the schema in-advance for streaming:  infer-schema isn't supported for streaming
# This is good practice in general as it is more efficient
dropSchema = StructType([ StructField("name", StringType(), True), StructField("action", StringType(), True) ])

# COMMAND ----------

# MAGIC %md
# MAGIC We will create a folder in DBFS (to sidestep offline authentication)

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC mkdirs action-drop

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# We define a dataframe connected to a lake folder
actionDrop = (
  spark
    .readStream
    .schema(dropSchema)               # Set the schema of the parquet data
    .option("maxFilesPerTrigger", 1)  # Treat a sequence of files as a stream by picking one file at a time
    .option("header", "true")         # CSV headers
    .csv('/action-drop')
)

# COMMAND ----------

# Notice the data frame is streaming
actionDrop.isStreaming

# COMMAND ----------

#  Integration with Spark SQL:  we create a SQL view pointing to the streaming DF
actionDrop.createOrReplaceTempView("actionDrop")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP view IF EXISTS dropCount

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's create a query connected to that streaming dataframe
# MAGIC CREATE TEMPORARY VIEW dropCount
# MAGIC     AS
# MAGIC       SELECT
# MAGIC       COUNT(*) AS entries,
# MAGIC       name
# MAGIC       FROM actionDrop
# MAGIC       GROUP BY name
# MAGIC       ORDER BY entries DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dropCount

# COMMAND ----------

# MAGIC %md
# MAGIC Let's copy files over

# COMMAND ----------

# MAGIC %fs
# MAGIC cp -r /mnt/destination/action-drop /action-drop

# COMMAND ----------

# MAGIC %fs
# MAGIC ls action-drop

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r /action-drop/

# COMMAND ----------

