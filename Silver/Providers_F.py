# Databricks notebook source
from pyspark.sql import SparkSession, functions as f

#Reading Hospital A departments data 
df_hosa=spark.read.parquet("/mnt/bronze/hosa/providers")

#Reading Hospital B departments data 
df_hosb=spark.read.parquet("/mnt/bronze/hosb/providers")

#union two departments dataframes
df_merged = df_hosa.unionByName(df_hosb)
display(df_merged.limit(1))

df_merged.createOrReplaceTempView("providers")


# COMMAND ----------

# %sql
# CREATE TABLE IF NOT EXISTS silver.providers (
# ProviderID string,
# FirstName string,
# LastName string,
# Specialization string,
# DeptID string,
# NPI long,
# datasource string,
# is_quarantined boolean
# )
# USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table silver.providers

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.providers;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into silver.providers
# MAGIC select ProviderID,
# MAGIC        FirstName,
# MAGIC        LastName,
# MAGIC        Specialization,
# MAGIC        DeptID,
# MAGIC        cast(NPI as long) NPI,
# MAGIC        datasource,
# MAGIC        case when ProviderID is null or DeptID is null then true else false end is_quarantined
# MAGIC        from providers

# COMMAND ----------


