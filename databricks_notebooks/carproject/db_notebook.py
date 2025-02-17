# Databricks notebook source
# MAGIC %md
# MAGIC # Create catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog cars_catalog;

# COMMAND ----------

# MAGIC %md
# MAGIC # create schema
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema cars_catalog.silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema cars_catalog.gold;