# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC select * from delta.`/Volumes/workspace/bronze/bronze_volume/airports/`

# COMMAND ----------

from pyspark.sql.functions import col,to_date,current_timestamp
from pyspark.sql.types import DecimalType

# COMMAND ----------

bronze_booking_df=spark.read.format("delta").load("/Volumes/workspace/bronze/bronze_volume/bookings/")

# COMMAND ----------

bronze_booking_df.display()

# COMMAND ----------

bronze_booking_df.printSchema()

# COMMAND ----------

silver_df = bronze_booking_df.withColumn("amount", col("amount").cast(DecimalType(10,2)))\
    .withColumn("booking_date", to_date(col("booking_date"), "yyyy-MM-dd"))\
    .withColumn("modified_date", current_timestamp())\
    .drop("_rescued_data")

display(silver_df)



# COMMAND ----------
