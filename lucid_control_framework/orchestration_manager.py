from lucid_control_framework.dataframe_transformation_manager import DataFrameTransformationManager
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col
from datetime import datetime
import logging
import json

class OrchestrationManager:
    """
    This class manages the orchestration for data processing.
    It provides methods to load orchestration configurations, build a Directed Acyclic Graph (DAG) for data processing,
    and log the execution results.
    """

    def __init__(self, spark, logger=None):
        """
        Initialize the OrchestrationManager with a SparkSession and an optional logger.
        
        :param spark: The SparkSession to use for data processing.
        :param logger: An optional logger for logging messages. If not provided, a default logger is used.
        """
        self.spark = spark
        self.logger = logger if logger else logging.getLogger(__name__)
        self.transform_manager = DataFrameTransformationManager(self.spark)

    def load_orchestration_config(self, orchestration_config: list):
        """
        Load the orchestration configuration into a DataFrame and save it as a delta table.
        
        :param orchestration_config: A list of dictionaries representing the orchestration configuration.
        """
        try:
            # Define the schema for the model config DataFrame
            schema = StructType([
                StructField("notebook_name", StringType(), True),
                StructField("notebook_path", StringType(), True),
                StructField("dependencies", StringType(), True),
                StructField("parameters", StringType(), True),
                StructField("timeout_per_cell_seconds", IntegerType(), True),
                StructField("retry_attempts", IntegerType(), True),
                StructField("interval_between_retry_attempt_seconds", IntegerType(), True),
                StructField("active", IntegerType(), True),
                StructField("process_group", IntegerType(), True)
            ])

            # Create a DataFrame from the model config
            df = self.spark.createDataFrame(orchestration_config, schema)

            # Add key to the model config DataFrame using transformer
            df = self.transform_manager.stage_dataframe_with_keys('Control.orchestration_config', df, df.columns, 'orchestration_config_key')

            # Write the DataFrame to the delta layer
            df.write.format('delta').mode('append').saveAsTable('Control.orchestration_config')
            self.logger.info('Model config loaded to delta successfully.')
        except Exception as e:
            self.logger.error(f'Failed to create and load model config: {e}')
            raise

    def build_dag(self, process_group: int) -> dict:
        """
        Build a Directed Acyclic Graph (DAG) for data processing based on the orchestration configuration.
        
        :param load_group: The load group to use for building the DAG.
        :return: A dictionary representing the DAG.
        """
        try:
            # Fetch the orchestration configurations from the control table
            df_control = self.spark.table("Control.orchestration_config").filter(
                (col("active") == 1) & (col("process_group") == process_group)
            )
            control_list = map(lambda row: row.asDict(), df_control.collect())
            DAG = {"activities": []}
            
            # Iterate through each control entry and construct the DAG activities
            for control in control_list:
                
                # Ensure parameters are correctly converted from JSON string to a dictionary
                args = json.loads(control["parameters"]) if control["parameters"] and control["parameters"] != 'null' else {}
                
                # Prepare the activity dictionary using correct keys for runMultiple
                activity = {
                    "name": control["notebook_name"],
                    "path": control["notebook_path"],
                    "timeoutPerCellInSeconds": control["timeout_per_cell_seconds"],
                    "args": args,
                    "retry": control["retry_attempts"],
                    "retryIntervalInSeconds": control["interval_between_retry_attempt_seconds"],
                    "dependencies": json.loads(control["dependencies"]) if control["dependencies"] else []
                }
                DAG["activities"].append(activity)
            return DAG
        except Exception as e:
            self.logger.error(f"Failed to build the DAG: {e}")
            raise

    def log_orchestration_execution(self, execution_results: dict):
        """
        Log the execution results into a DataFrame and save it as a delta table.
        
        :param execution_results: A dictionary representing the execution results.
        """
        try:
            # Define the schema for the log DataFrame
            schema = StructType([
                StructField("notebook_name", StringType(), True),
                StructField("execution_status", StringType(), True),
                StructField("exception", StringType(), True),
                StructField("log_timestamp", TimestampType(), True)
            ])

            # Create a list to store the log records
            logs = []

            # Get the current time
            current_time = datetime.now()

            # Iterate over the execution results and create a log record for each one
            for notebook_name, result in execution_results.items():
                exception_message = str(result["exception"]) if result["exception"] else None
                # Truncate the exception message to 4000 characters
                if exception_message and len(exception_message) > 4000:
                    exception_message = exception_message[:4000]
                log = {
                    "notebook_name": notebook_name,
                    "execution_status": "Failed" if result["exception"] else "Success",
                    "exception": exception_message,
                    "log_timestamp": current_time
                }
                logs.append(log)

            # Create a DataFrame from the logs
            df = self.spark.createDataFrame(logs, schema)

            # Add key to the log DataFrame using transform manager
            df = self.transform_manager.stage_dataframe_with_keys('Control.orchestration_log', df, df.columns, 'orchestration_log_key')
            
            # Write the DataFrame to the delta layer
            df.write.format("delta").mode("append").saveAsTable("Control.orchestration_log")
        except Exception as e:
            self.logger.error(f"Failed to save execution log: {e}")
            raise