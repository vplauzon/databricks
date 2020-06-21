# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Notebook demoing Delta Lake in spark

# COMMAND ----------

#  Let's read an hour worth of data
sample = spark.read.parquet('/mnt/source/wikipedia/year=2020/month=05/day=05/hour=04/part-merged.snappy.parquet')

# COMMAND ----------

#  Integration with Spark SQL
sample.createOrReplaceTempView("sampleView")

# COMMAND ----------

# MAGIC %sql
# MAGIC --  Drop the table before creating it
# MAGIC DROP TABLE IF EXISTS sampleDelta

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's create a delta lake table from that sample
# MAGIC -- See https://docs.delta.io/latest/delta-batch.html
# MAGIC CREATE TABLE sampleDelta
# MAGIC   USING DELTA
# MAGIC   LOCATION '/mnt/destination/delta/sample'
# MAGIC   COMMENT 'Sample delta lake table'
# MAGIC   AS
# MAGIC     SELECT *
# MAGIC     FROM sampleView

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Look at the data
# MAGIC SELECT *
# MAGIC FROM sampleDelta
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC -- We can INSERT / UPDATE / MERGE / DELETE data in Delta
# MAGIC DELETE FROM sampleDelta
# MAGIC WHERE requests==2
# MAGIC -- Look at the blob representation afterwards

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Look at the data again
# MAGIC SELECT *
# MAGIC FROM sampleDelta
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's look at the table history
# MAGIC DESCRIBE HISTORY sampleDelta

# COMMAND ----------

# Let's look if the data from the original version is still there
sampleDeltaV0 = spark.read.format("delta").option("versionAsOf", 0).load("/mnt/destination/delta/sample")
display(sampleDeltaV0.limit(5))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Doing something similar in SQL
# MAGIC SELECT *
# MAGIC FROM sampleDelta VERSION AS OF 0
# MAGIC -- Learn more about time travel:  https://databricks.com/blog/2019/02/04/introducing-delta-time-travel-for-large-scale-data-lakes.html

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM sampleDelta

# COMMAND ----------

# MAGIC %fs ls 

# COMMAND ----------

