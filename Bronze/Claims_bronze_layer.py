# Databricks notebook source
from pyspark.sql import SparkSession, functions as f

claims_df=spark.read.csv("/mnt/landing/claims/*.csv",header=True)

claims_df = claims_df.withColumn(
    "datasource",
    f.when(f.input_file_name().contains("hospital1"), "hosa").when(f.input_file_name().contains("hospital2"), "hosb")
     .otherwise(None)
)

display(claims_df)

# COMMAND ----------

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import lit



# # Define the filenames
# filenames = ["hospital1_claim_data.csv", "hospital2_claim_data.csv"]

# # Iterate over each filename
# for filename in filenames:
#     # Read the file from ADLS
#     claims_df = spark.read.format("csv").option("header", "true").load("/mnt/landing/claims/" + filename)
    
#     # Check the filename and create a new column
#     if "hospital1_claim_data.csv" in filename:
#         claims_df = claims_df.withColumn("datasource", lit("hosta"))
#     elif "hospital2_claim_data.csv" in filename:
#         claims_df = claims_df.withColumn("datasource", lit("hostb"))
    
#     # claims_df=claims_df.filter(f.col("datasource")=="hosta")
#     # Display the DataFrame
#     display(claims_df)
    
#     # Write the DataFrame back to ADLS or another location if needed
#     # claims_df.write.format("delta").save("/mnt/landing/processed/" + filename)


# COMMAND ----------

#load the data in bronze layer
claims_df.write.format("parquet").mode("overwrite").save("/mnt/bronze/claims/")

# COMMAND ----------


