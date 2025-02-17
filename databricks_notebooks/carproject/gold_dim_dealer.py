# Databricks notebook source
# MAGIC %md
# MAGIC # Create FLAG PARAMETERS

# COMMAND ----------

from pyspark.sql.functions import  *
from pyspark.sql.types import *
from pyspark.sql.functions import col

# COMMAND ----------

dbutils.widgets.text("Incremental_Flag",'0')

# COMMAND ----------

Incremental_Flag = dbutils.widgets.get("Incremental_Flag")


# COMMAND ----------

# MAGIC %md
# MAGIC # Create Dimension Model

# COMMAND ----------

# MAGIC %md
# MAGIC # fetch relative coloumns

# COMMAND ----------

df_src = spark.sql('''
select distinct(Dealer_ID) as Dealer_ID,DealerName 
from parquet.`abfss://silver@carrohithdatalake.dfs.core.windows.net/carsales`
''');
df_src.display()

# COMMAND ----------

df_src.count()

# COMMAND ----------

# MAGIC %md
# MAGIC # Dim_Model_Sink Intitial and Incremental(just bring thne schema if TAble not exists)

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_dealer'):

    df_sink = spark.sql('''
    select  dim_dealer_key,Dealer_ID,DealerName
    from cars_catalog.gold.dim_dealer
    ''')
else:
    df_sink = spark.sql('''
    select 1 as dim_dealer_key,Dealer_ID,DealerName
    from parquet.`abfss://silver@carrohithdatalake.dfs.core.windows.net/carsales`
    where 1=0
    ''')

# COMMAND ----------

display(df_sink)

# COMMAND ----------

# MAGIC %md
# MAGIC # Filtering New Records and Old Records

# COMMAND ----------

df_filter = df_src.join(
    df_sink,
    df_src['Dealer_ID'] == df_sink['Dealer_ID'],
    'left'
).select(
    df_src['Dealer_ID'],
    df_src['DealerName'],
    df_sink['dim_dealer_key']
)

# COMMAND ----------

df_filter.display()

# COMMAND ----------

df_filter_old = df_filter.filter(col('dim_dealer_key').isNotNull())

# COMMAND ----------

df_filter_new = df_filter.filter(col('dim_dealer_key').isNull()).select(
    df_src['Dealer_ID'],
    df_src['DealerName']
)


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
    max_val_df=spark.sql("select max(dim_dealer_key) from cars_catalog.gold.dim_dealer")
    max_val=max_val_df.collect()[0][0]+1

# COMMAND ----------

# MAGIC %md
# MAGIC **create surrogate key coloum and add max surrogate key**

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('dim_dealer_key',max_val+monotonically_increasing_id())

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
if spark.catalog.tableExists('cars_catalog.gold.dim_dealer'):
    delta_tbl = DeltaTable.forPath(spark, "abfss://gold@carrohithdatalake.dfs.core.windows.net/gold/dim_dealer")

    delta_tbl.alias("trg").merge(
        df_final.alias("src"), 
        "trg.dim_dealer_key = src.dim_dealer_key"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

#initial_run
else:
    df_final.write.format("delta")\
        .mode("overwrite")\
        .option("path", "abfss://gold@carrohithdatalake.dfs.core.windows.net/gold/dim_dealer")\
        .saveAsTable("cars_catalog.gold.dim_dealer")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.dim_dealer;

# COMMAND ----------

