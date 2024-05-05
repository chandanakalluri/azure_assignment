# Databricks notebook source
# MAGIC %run "/Workspace/bronze_to_silver/assesment/bronze_to_silver/Utils"

# COMMAND ----------

from pyspark.sql.functions import when, col

# COMMAND ----------

raw_producet_df = spark.read.csv('dbfs:/mnt/Bronze/sales_view/product', header=True, inferSchema=True)

# COMMAND ----------

renamed_product_df = toSnakeCase(raw_producet_df)

# COMMAND ----------

sub_category_df = renamed_product_df.withColumn("sub_category", when(col('category_id') == 1, "phone")\
        .when(col('category_id') == 2 , "laptop")\
        .when(col('category_id') == 3, "playstation")\
        .when(col('category_id') == 4, "e-device"))


# COMMAND ----------

writeTo = f'dbfs:/mnt/silver/sales_view/product'
write_delta_upsert(sub_category_df, writeTo)