# Databricks notebook source
# DBTITLE 1,Load CSV files with Auto Loader to bronze table
# Read CSV files using Auto Loader
df = (spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("cloudFiles.schemaLocation", "/Volumes/retail_q/volumes/blob_source/_schema")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("/Volumes/retail_q/volumes/blob_source/transactions_source/")
)

# Write to bronze table - process available data and stop
query = (df.writeStream
  .option("checkpointLocation", "/Volumes/retail_q/volumes/blob_source/_checkpoint")
  .trigger(availableNow=True)
  .toTable("retail_q.blob_bronze.transactions")
)

# Wait for the batch to complete
query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from retail_q.blob_bronze.transactions