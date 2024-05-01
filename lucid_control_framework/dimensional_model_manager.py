#!/usr/bin/env python
# coding: utf-8

# ## Lucid_DimensionalModelManager
# 
# 
# 

# In[ ]:


import logging
import json
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# In[ ]:


class DimensionalModelManager:
    def __init__(self, spark):
        self.spark = spark

    def load_model_config(self, modeling_config):
        try:
            schema = StructType([
                StructField("notebook_name", StringType(), True),
                StructField("notebook_path", StringType(), True),
                StructField("dependencies", StringType(), True),
                StructField("load_group", IntegerType(), True),
                StructField("active", IntegerType(), True)
            ])

            df = self.spark.createDataFrame(modeling_config, schema)
            df.write.format("delta").mode("append").saveAsTable("Control.dimensional_model_config")
            logger.info("Model config loaded to delta successfully.")
        except Exception as e:
            logger.error(f"Failed to create and load model config: {e}")
            raise

    def build_dag(self, load_group):
        try:
            df_control = self.spark.table("Control.dimensional_model_config").filter((col("active") == 1) & (col("load_group") == load_group))
            control_list = map(lambda row: row.asDict(), df_control.collect())
            DAG = {"activities": []}
            for control in control_list:
                activity = {
                    "name": control["notebook_name"],
                    "path": control["notebook_path"],
                    "dependencies": json.loads(control["dependencies"]) if control["dependencies"] else []
                }
                DAG["activities"].append(activity)
            logger.info("DAG built successfully.")
            return DAG
        except Exception as e:
            logger.error(f"Failed to build DAG: {e}")
            raise

    def log_execution(self, execution_results):
        try:
            # Define the schema for the log DataFrame
            schema = StructType([
                StructField("notebook_name", StringType(), True),
                StructField("execution_status", StringType(), True),
                StructField("exception", StringType(), True),
                StructField("execution_timestamp", TimestampType(), True)
            ])

            # Create a list to store the log records
            logs = []

            # Get the current time
            current_time = datetime.now()

            # Iterate over the execution results and create a log record for each one
            for notebook_name, result in execution_results.items():
                log = {
                    "notebook_name": notebook_name,
                    "execution_status": "Failed" if result["exception"] else "Success",
                    "exception": str(result["exception"]) if result["exception"] else None,
                    "execution_timestamp": current_time
                }
                logs.append(log)

            # Create a DataFrame from the logs
            df = self.spark.createDataFrame(logs, schema)

            # Write the DataFrame to the delta layer
            df.write.format("delta").mode("append").saveAsTable("Control.dimensional_model_log")

            logger.info("Execution log saved successfully.")
        except Exception as e:
            logger.error(f"Failed to save execution log: {e}")
            raise

