# Databricks notebook source
from pyspark.sql import functions as f

# Reading Hospital A departments data 
df_hosa = spark.read.parquet("/mnt/bronze/hosa/departments")

# Reading Hospital B departments data 
df_hosb = spark.read.parquet("/mnt/bronze/hosb/departments")

# Union two departments dataframes
df_merged = df_hosa.unionByName(df_hosb)

# Create the dept_id column and rename deptid to src_dept_id

# df_merged = df_merged.withColumn("SRC_Dept_id", f.col("deptid")) \
#                       .withColumn("Dept_id", f.concat(f.col("deptid"),f.lit('_') ,f.col("datasource")))
df_merged = df_merged.withColumn("SRC_Dept_id", f.col("deptid")) \
                     .withColumn("Dept_id", f.concat_ws('_',f.col("deptid"), f.col("datasource")))\
                     .drop("DeptID")


df_merged.createOrReplaceTempView("departments")

#display(df_merged.limit(1))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Database silver
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver.departments (
# MAGIC Dept_Id string,
# MAGIC SRC_Dept_Id string,
# MAGIC Name string,
# MAGIC datasource string,
# MAGIC is_quarantined boolean
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql 
# MAGIC truncate table silver.departments

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into silver.departments
# MAGIC select Dept_Id,
# MAGIC        SRC_Dept_Id,
# MAGIC        Name,
# MAGIC        datasource,
# MAGIC        case when SRC_Dept_Id is null or Name is null then true else false end as is_quarantined
# MAGIC  from departments;
# MAGIC -- select * from departments limit 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.departments

# COMMAND ----------


