# Databricks notebook source
# MAGIC %md
# MAGIC # Data Reading

# COMMAND ----------

df= spark.read.format("parquet")\
    .option('inferschema', "true")\
    .load("abfss://bronze@carrohithdatalake.dfs.core.windows.net/rawdata")


# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # data tranformation
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import StringType

# COMMAND ----------


df=df.withColumn('model_catogery',split(col('Model_ID'),'-')[0])

# COMMAND ----------

df.display()

# COMMAND ----------




# COMMAND ----------

df=df.withColumn('Units_Sold',col('Units_Sold').cast(StringType()))

# COMMAND ----------

df=df.withColumn('RevPerUnit',col('Revenue')/col('Units_Sold'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # data writing

# COMMAND ----------

df.write.format('parquet') \
    .mode('append') \
    .option('path', 'abfss://silver@carrohithdatalake.dfs.core.windows.net/carsales') \
    .save()


# COMMAND ----------

# MAGIC %md
# MAGIC # querying silver data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://silver@carrohithdatalake.dfs.core.windows.net/carsales`

# COMMAND ----------

# MAGIC %md
# MAGIC # Ad-Hoc
# MAGIC  

# COMMAND ----------

df.display()

# COMMAND ----------

df.groupBy('year', 'BranchName') \
  .agg(sum('units_sold').alias('total_units_sold')) \
  .orderBy('year', 'total_units_sold', ascending=[1, 0]) \
  .display()


# COMMAND ----------

# MAGIC %md
# MAGIC # Data Writing

# COMMAND ----------

df.write.format('Parquet')\
    .mode('overwrite')\
    .option('path','abfss://silver@carrohithdatalake.dfs.core.windows.net/carsales')\
    .save()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://silver@carrohithdatalake.dfs.core.windows.net/carsales`;