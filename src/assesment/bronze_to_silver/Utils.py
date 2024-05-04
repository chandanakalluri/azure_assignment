# Databricks notebook source
#Mount bronze blob storage using dbutils
'''storage_account_name = "chandanastorage"
container_name = "bronze"
mount_point = "/mnt/Bronze"
AccessKey ='r1vAaWnGmV3A/kASoAmAsmrCW6WCzN/rrBeyBYwz7wnQqe9QD6YNL7yY4kULvzdEV5K6wgbFc25a+AStRoLP6A=='

dbutils.fs.mount(
  source=f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
  mount_point=mount_point,
  extra_configs={
    f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": AccessKey
  }
)'''


# COMMAND ----------

'''storage_account_name = "chandanastorage"
container_name = "bronze"
mount_point = "/mnt/Silver"
AccessKey ='r1vAaWnGmV3A/kASoAmAsmrCW6WCzN/rrBeyBYwz7wnQqe9QD6YNL7yY4kULvzdEV5K6wgbFc25a+AStRoLP6A=='

dbutils.fs.mount(
  source=f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
  mount_point=mount_point,
  extra_configs={
    f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": AccessKey
  }
)'''

# COMMAND ----------

'''storage_account_name = "chandanastorage"
container_name = "bronze"
mount_point = "/mnt/Gold"
AccessKey ='r1vAaWnGmV3A/kASoAmAsmrCW6WCzN/rrBeyBYwz7wnQqe9QD6YNL7yY4kULvzdEV5K6wgbFc25a+AStRoLP6A=='

dbutils.fs.mount(
  source=f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
  mount_point=mount_point,
  extra_configs={
    f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": AccessKey
  }
)'''

# COMMAND ----------

from pyspark.sql.functions import udf
def toSnakeCase(df):
    for column in df.columns:
        snake_case_col = ''
        for char in column:
            if char ==' ':
                snake_case_col += '_'
            else:
                snake_case_col += char.lower()
        df = df.withColumnRenamed(column, snake_case_col)
    return df

udf(toSnakeCase)



# COMMAND ----------

def write_delta_upsert(df, delta_path):
    df.write.format("delta").mode("overwrite").save(delta_path)

# COMMAND ----------

def read_delta_file(delta_path):
    df = spark.read.format("delta").load(delta_path)
    return df
udf(read_delta_file)