# Databricks notebook source
# All imports related to the basic python operations

from datetime import datetime

#All libary related to the job
from delta.tables import *
from pandas.core.frame import DataFrame
from pyspark.sql import DataFrame
from pyspark.sql.functions import input_file_name, split, reverse, col

# COMMAND ----------

# Setting Job Parameters -> Get them from KeyVault, already added, fix connection
job_mode = dbutils.widgets.get("mode")
landing_container = ""
checkpoint_container = ""
landing_storage_account = ""
schema = dbutils.widgets.get("schema")
date = datetime.today().strftime('%Y/%m/%d')
queryName = "CDCA-{}-Bronze".format(schema)
base_landing_url = "abfss://{}@{}.dfs.core.windows.net//landing/cdca/{}/{}/".format(landing_container, landing_storage_account, schema, date)
base_checkpoint_reads_url = "abfss://{}@{}.dfs.core.windows.net/cdca/checkpoints/{}/reads".format(checkpoint_container, landing_storage_account, schema)
base_checkpoint_writes_url = "abfss://{}@{}.dfs.core.windows.net/cdca/checkpoints/{}/writes".format(checkpoint_container, landing_storage_account, schema)
unity_catalog = ""
bronze_schema = ""

# COMMAND ----------

#spark.conf.set("spark.databricks.io.cache.enabled", True)
#spark.conf.set("spark.databricks.io.cache.maxMetaDataCache", "2g")
#spark.conf.set("spark.databricks.io.cache.compression.enabled", True)
#
#spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", True)
#spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", True)
#
spark.conf.set("spark.sql.shuffle.partitions", "64")
spark.conf.set("spark.default.parallelism", "128")
spark.conf.set("spark.sql.adaptive.enabled",True)
spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")
spark.conf.set("spark.databricks.optimizer.adaptive.enabled", True)
spark.conf.set("spark.databricks.adaptive.autoOptimizeShuffle.enabled", True)
spark.conf.set("spark.sql.streaming.noDataMicroBatches.enabled", False)

# COMMAND ----------

def for_each_cdca_dataset(bronze_df, epoch_id):
    """
        Runs each batch of the Streaming dataset and splits them into multiple bronze tables.

        Parameters
        ----------
        bronze_df : spark.DataFrame
            RAW NETA file records which is pipe delimited and has to be flattened
        epoch_id : Epoch for the current batch

        Returns
        -------
        None : Writes to the multiple tables
    """
    final_tables = flatten(spark, bronze_df.toPandas())
    for table_name, data in final_tables.items():
        if table_name == 'GP9':
            table = "{}.{}.".format(unity_catalog, bronze_schema)
        elif table_name == 'EPD':
            table = "{}.{}.".format(unity_catalog, bronze_schema)
        elif table_name == 'IPD':
            table = "{}.{}.".format(unity_catalog, bronze_schema)
        elif table_name == 'ABP':
            table = "{}.{}.".format(unity_catalog, bronze_schema)
        data.write.mode('append').format("delta").saveAsTable(table)
