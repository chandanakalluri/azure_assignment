# Databricks notebook source
from pyspark.sql.functions import split, when, col, to_date

# COMMAND ----------

raw_customer_df = spark.read.csv('dbfs:/mnt/Bronze/sales_view/customer/20240105_sales_customer.csv', header=True, inferSchema=True)


# COMMAND ----------

renamed_customer_df = toSnakeCase(raw_customer_df)