# Databricks notebook source
# MAGIC %sql
# MAGIC create database audit;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS audit.load_logs (
# MAGIC     id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     data_source STRING,
# MAGIC     tablename STRING,
# MAGIC     numberofrowscopied INT,
# MAGIC     watermarkcolumnname STRING,
# MAGIC     loaddate TIMESTAMP
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from audit.load_logs
# MAGIC
# MAGIC -- select coalesce(cast(max(loaddate) as date),'1900-01-01') as last_fetched_date  from audit.load_logs where data_source='hos-b' and tablename='dbo.providers'
# MAGIC
# MAGIC -- select coalesce(cast(max(loaddate) as date),'1900 -01-01') as last_fetched_date from audit.load_logs where data_source='hos-b' and tablename ='dbo.encounters'
# MAGIC
# MAGIC -- select * from audit.load_logs where watermarkcolumnname
# MAGIC
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table audit.load_logs

# COMMAND ----------


