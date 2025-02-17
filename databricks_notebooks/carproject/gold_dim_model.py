# Databricks notebook source
# MAGIC %md
# MAGIC # Create FLAG PARAMETERS

# COMMAND ----------

from pyspark.sql.functions import  *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.text("Incremental_Flag",'0')

# COMMAND ----------

Incremental_Flag = dbutils.widgets.get("Incremental_Flag")


# COMMAND ----------

# MAGIC %md
# MAGIC # Create Dimension Model

# COMMAND ----------

df_src = spark.sql('''
select Distinct(Model_ID) as Model_ID,model_catogery from parquet.`abfss://silver@carrohithdatalake.dfs.core.windows.net/carsales`
''');
df_src.display()

# COMMAND ----------

df_src.count()

# COMMAND ----------

# MAGIC %md
# MAGIC # Dim_Model_Sink Intitial and Incremental

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_model'):
    df_sink = spark.sql('''
    select null as dim_model_key, Model_ID, model_catogery 
    from cars_catalog.gold.dim_model
    ''')
else:
    df_sink = spark.sql('''
    select 1 as dim_model_key, Model_ID, model_catogery 
    from parquet.`abfss://silver@carrohithdatalake.dfs.core.windows.net/carsales`
    where 1=0
    ''')

display(df_sink)

# COMMAND ----------

# MAGIC %md
# MAGIC # Filtering New Records and Old Records

# COMMAND ----------

df_filter = df_src.join(
    df_sink,
    df_src['Model_ID'] == df_sink['Model_ID'],
    'left'
).select(
    df_src['Model_ID'],
    df_src['model_catogery'],
    df_sink['dim_model_key']
).dropDuplicates()
df_filter.display()

# COMMAND ----------

df_filter_old = df_filter.filter(col('dim_model_key').isNotNull())

# COMMAND ----------

df_filter_new = df_filter.filter(col('dim_model_key').isNull()).select(df_src['model_id'],df_src['model_catogery'])

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### create surrogate key

# COMMAND ----------

# MAGIC %md
# MAGIC **fetch the max surrogate key from the table**

# COMMAND ----------

if Incremental_Flag == '0':
    max_val=1
else:
    max_val_df=spark.sql("select max(dim_model_key) from cars_catalog.gold.dim_model")
    max_val=max_val_df.collect()[0][0]+1

# COMMAND ----------

# MAGIC %md
# MAGIC **create surrogate key coloum and add max surrogate key**

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('dim_model_key',max_val+monotonically_increasing_id())

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create final DF =df_filter_old + df_filter_new

# COMMAND ----------

df_final = df_filter_new.union(df_filter_old)

# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SCD TYPE - 1 (UPSERT)

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

#incremental_run
if spark.catalog.tableExists('cars_catalog.gold.dim_model'):
    delta_tbl = DeltaTable.forPath(spark, "abfss://gold@carrohithdatalake.dfs.core.windows.net/gold/dim_model")

    delta_tbl.alias("trg").merge(
        df_final.alias("src"), 
        "trg.dim_model_key = src.dim_model_key"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

#initial_run
else:
    df_final.write.format("delta")\
        .mode("overwrite")\
        .option("path", "abfss://gold@carrohithdatalake.dfs.core.windows.net/gold/dim_model")\
        .saveAsTable("cars_catalog.gold.dim_model")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.dim_model;

# COMMAND ----------

