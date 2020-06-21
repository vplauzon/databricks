# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Transformation job

# COMMAND ----------

#  Now, let's read a "partitioned table"
wikipedia = spark.read.parquet('/mnt/source/wikipedia/year=2020/month=05')
#  Observe it takes longer

# COMMAND ----------

#  What is the wikipedia object?
wikipedia

# COMMAND ----------

# Look at the data
display(wikipedia)
# Observe it takes extra column 'day' & 'hour' from the folder structure, but not 'year' nor 'month'

# COMMAND ----------

#  Integration with Spark SQL:  we create a SQL view pointing to those files
wikipedia.createOrReplaceTempView("wikipedia")

# COMMAND ----------

#  We can now do a SQL query
display(spark.sql("""SELECT *
  FROM wikipedia
  LIMIT 10"""))

# COMMAND ----------

# What is that query?
spark.sql("SELECT * FROM wikipedia")
# Another dataframe

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SQL cell:  let's do the same query in SQL now
# MAGIC SELECT *
# MAGIC FROM wikipedia
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's do a query filtering on partitions (time)
# MAGIC SELECT *
# MAGIC FROM wikipedia
# MAGIC -- We clip along partition boundary:  this allows Spark to only look at few files and hence be fast
# MAGIC WHERE day>12 AND hour==12
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's do a query filtering on partitions (time)
# MAGIC SELECT *
# MAGIC FROM wikipedia
# MAGIC -- Here we do not go along the partition line as they are decoupled from datetime ; this requires looking at more blobs and be slower
# MAGIC WHERE datetime>=to_date("12/5/2020", "dd/MM/yyyy")
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- How many rows do we have here?
# MAGIC SELECT COUNT(*)
# MAGIC FROM wikipedia
# MAGIC -- ~930M rows

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's count the number of distinct pages
# MAGIC SELECT COUNT(*)
# MAGIC FROM
# MAGIC (
# MAGIC   SELECT DISTINCT page
# MAGIC   FROM wikipedia
# MAGIC )
# MAGIC -- ~30M pages

# COMMAND ----------

# MAGIC %sql
# MAGIC --  Drop the table before creating it
# MAGIC DROP TABLE IF EXISTS pageCountOnWikipedia

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's aggregate the data and output the aggregation to the destination lake
# MAGIC -- See https://docs.databricks.com/spark/latest/spark-sql/language-manual/create-table.html
# MAGIC CREATE TABLE pageCountOnWikipedia
# MAGIC   USING PARQUET
# MAGIC   OPTIONS ('compression'='snappy')
# MAGIC   -- See https://kontext.tech/column/spark/296/data-partitioning-in-spark-pyspark-in-depth-walkthrough
# MAGIC   -- for details about partitions
# MAGIC   --PARTITIONED BY (day)
# MAGIC   LOCATION '/mnt/destination/pageCountOnWikipedia5/out/'
# MAGIC   COMMENT 'Wikipedia aggreation'
# MAGIC   AS
# MAGIC     SELECT page, COUNT(*) AS entries
# MAGIC     FROM wikipedia
# MAGIC     GROUP BY page
# MAGIC     ORDER BY entries DESC