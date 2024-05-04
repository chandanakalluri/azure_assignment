# Databricks notebook source
# MAGIC %run "/Workspace/bronze_to_silver/assesment/bronze_to_silver/Utils"

# COMMAND ----------

raw_sales_df = spark.read.csv('dbfs:/mnt/Bronze/sales_view/sales/20240107_sales_data.csv', header=True, inferSchema=True)

# COMMAND ----------

renamed_sales_df = toSnakeCase(raw_sales_df)

# COMMAND ----------

writeTo = f'dbfs:/mnt/Silver/sales_view/customer_sales'
write_delta_upsert(renamed_sales_df, writeTo)