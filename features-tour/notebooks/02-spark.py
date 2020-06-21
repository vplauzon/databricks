# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Notebook demoing spark
# MAGIC 
# MAGIC This is a markdown cell.

# COMMAND ----------

#  Simple Python
1+1

# COMMAND ----------

#  Declare the path to a sample blob
sampleBlobPath = '/mnt/source/wikipedia/year=2020/month=05/day=05/hour=04/part-merged.snappy.parquet'

# COMMAND ----------

#  Read the parquet blob
sample = spark.read.parquet(sampleBlobPath)
#  Here we leverage passthrough authentication to the blob storage

# COMMAND ----------

#  What is "sample" object?
sample
#  A Data Frame:  kind of a lazy loaded readonly table pointing to some storage

# COMMAND ----------

#  Let's look at the top of the blob
display(sample)
#  We can notice this job takes longer than the loading job ; why?  Lazy loading