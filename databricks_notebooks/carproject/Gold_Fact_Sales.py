# Databricks notebook source
# MAGIC %md
# MAGIC ### CREATE FACT TABLE

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading Silver Data **

# COMMAND ----------


df_silver = spark.sql("select * from parquet.`abfss://silver@carrohithdatalake.dfs.core.windows.net/carsales`")

# COMMAND ----------

df_silver.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **reading all the dimensions**

# COMMAND ----------

df_dealer = spark.sql("select * from cars_catalog.gold.dim_dealer")
df_branch = spark.sql("select * from cars_catalog.gold.dim_branch")
df_model = spark.sql("select * from cars_catalog.gold.dim_model")
df_date = spark.sql("select * from cars_catalog.gold.dim_date")

# COMMAND ----------

# MAGIC %md
# MAGIC **Brining keys to the FACT TAble **

# COMMAND ----------

df_fact = df_silver.join(df_branch,df_silver['Branch_ID'] == df_branch['Branch_ID'],how='left')\
    .join(df_dealer,df_silver['Dealer_ID'] == df_dealer['Dealer_ID'],how='left')\
    .join(df_model,df_silver['Model_ID'] == df_model['model_id'],how='left')\
    .join(df_date,df_silver['Date_ID'] == df_date['Date_ID'],how='left')\
    .select(df_silver['Revenue'],df_silver['Units_Sold'],df_silver['RevPerUnit'],df_branch['dim_branch_key'],df_dealer['dim_dealer_key'],df_model['dim_model_key'],df_date['dim_date_key'])

# COMMAND ----------

df_fact.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # writing fact table
# MAGIC

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists('factsales'):
    delta_tbl = DeltaTable.forName(spark, 'cars_catalog.gold.factsales')
    delta_tbl.alias("trg").merge(df_fact.alias("src"), "trg.dim_date_key = src.dim_date_key AND trg.dim_branch_key = src.dim_branch_key AND trg.dim_dealer_key = src.dim_dealer_key AND trg.dim_model_key = src.dim_model_key")\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()




else:
    df_fact.write.format('delta')\
            .mode('overwrite')\
            .option('path','abfss://silver@carrohithdatalake.dfs.core.windows.net/factsales')\
            .saveAsTable('cars_catalog.gold.factsales')

# COMMAND ----------

# MAGIC %sql select * from cars_catalog.gold.factsales