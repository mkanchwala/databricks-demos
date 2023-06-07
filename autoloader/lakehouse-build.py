# Databricks notebook source
# MAGIC %run config

# COMMAND ----------

# DBTITLE 1,Streaming - Ingestion Bronze
bronze_df = spark.readStream.format("cloudFiles") \
 .option("cloudFiles.format", "csv") \
 .option("cloudFiles.maxFilesPerTrigger", "50") \
 .option("cloudFiles.maxBytesPerTrigger", "50m") \
 .option("cloudFiles.schemaLocation", base_checkpoint_reads_url) \
 .option("header", False) \
 .option("inferSchema", True) \
 .load(base_landing_url) \
 .withColumn("file", reverse(split(input_file_name(), "/")).getItem(0) ).repartition("file")

# COMMAND ----------

if(job_mode == 'batch'):
    bronze_df.writeStream \
      .queryName(queryName) \
      .option("checkpointLocation", base_checkpoint_writes_url) \
      .trigger(availableNow=True) \
      .foreachBatch(for_each_cdca_dataset) \
      .start()
elif(job_mode == 'stream'):
    bronze_df.writeStream \
      .queryName(queryName) \
      .option("checkpointLocation", base_checkpoint_writes_url) \
      .foreachBatch(for_each_cdca_dataset) \
      .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM cloud_files_state('abfss://@.dfs.core.windows.net/cdca/checkpoints/I030/writes');
