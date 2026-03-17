# Databricks notebook source
dbutils.widgets.text("src_folder","")

# COMMAND ----------

src_folder_value=dbutils.widgets.get("src_folder")

# COMMAND ----------

source_path=f"/Volumes/workspace/raw/raw_flight_data/{src_folder_value}"
checkpoint_path=f"/Volumes/workspace/raw/raw_flight_data/{src_folder_value}/checkpoint"
bronze_table_name=f"bronze.{src_folder_value}"

# COMMAND ----------

print(f"---Ingestion Parameters---")
print(f"source_folder:{src_folder_value}")
print(f"source_path:{source_path}")
print(f"checkpoint path:{checkpoint_path}")
print(f"bronze_table:{bronze_table_name}")

# COMMAND ----------

df=spark.readStream.format("cloudFiles").option("cloudFiles.format","csv").option("cloudFiles.schemaLocation",checkpoint_path).option("cloudFiles.schemaEvolutionMode","rescue").option("header","True").load(source_path)

# COMMAND ----------

df.writeStream.format("delta").outputMode("append").trigger(once=True).option("checkpointLocation",checkpoint_path).option("path",f"/Volumes/workspace/bronze/bronze_volume/{src_folder_value}").start()
