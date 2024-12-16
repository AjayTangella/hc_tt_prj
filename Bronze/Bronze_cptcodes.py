# Databricks notebook source
from pyspark.sql.functions import col

# Read the CSV file
cptcodes_df=spark.read.format("csv").option("header","true").load("/mnt/landing/cptcodes/*")
# display(cptcodes_df.limit(2))

# Replace whitespaces in column names with underscores and convert to lowercase
for col in cptcodes_df.columns:
  new_col=col.replace(" ","_").lower()
  cptcodes_df=cptcodes_df.withColumnRenamed(col,new_col)

# display(cptcodes_df.limit(2))





# COMMAND ----------

cptcodes_df.write.format("parquet").mode("overwrite").save("/mnt/bronze/cptcodes")
display(cptcodes_df.limit(2))

# COMMAND ----------


