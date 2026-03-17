# Databricks notebook source
src_array=[
    {"src_folder":"airports"},
    {"src_folder":"flights"},
    {"src_folder":"passengers"},
    {"src_folder":"bookings"}
]

# COMMAND ----------

dbutils.jobs.taskValues.set(key="source_list",value=src_array)
