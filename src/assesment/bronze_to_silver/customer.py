# Databricks notebook source
# MAGIC %run "/Workspace/bronze_to_silver/assesment/bronze_to_silver/Utils"

# COMMAND ----------

from pyspark.sql.functions import split, when, col, to_date
#change_snake

# COMMAND ----------

raw_customer_df = spark.read.csv('dbfs:/mnt/Bronze/20240107_sales_customer.csv', header=True, inferSchema=True)



# COMMAND ----------

renamed_customer_df = toSnakeCase(raw_customer_df)

# COMMAND ----------

splited_name_df = renamed_customer_df.withColumn('first_name', split(renamed_customer_df.name, " ")[0])\
    .withColumn('last_name', split(renamed_customer_df.name, " ")[1]).drop(renamed_customer_df.name)


# COMMAND ----------

extract_domain_df = splited_name_df.withColumn("tempdomain", split(splited_name_df.email_id, "@")[1]).drop(splited_name_df.email_id)
extract_domain_df = extract_domain_df.withColumn('domain', split(extract_domain_df.tempdomain, '\.')[0]).drop(extract_domain_df.tempdomain)
