# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC create schema if not exists raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create volume raw.raw_flight_data

# COMMAND ----------

volume_path="/Volumes/workspace/raw/raw_flight_data"

# COMMAND ----------

dbutils.fs.mkdirs(f"{volume_path}/bookings")

# COMMAND ----------

dbutils.fs.mkdirs(f"{volume_path}/flights")
dbutils.fs.mkdirs(f"{volume_path}/passengers")
dbutils.fs.mkdirs(f"{volume_path}/airports")

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists bronze;
# MAGIC create schema if not exists silver;
# MAGIC create schema if not exists gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC create volume bronze.bronze_volume;
# MAGIC create volume silver.silver_volume;
# MAGIC create volume gold.gold_volume;