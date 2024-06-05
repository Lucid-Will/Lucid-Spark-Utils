from lucid_control_framework.transformation_manager import TransformationManager
from lucid_control_framework.upsert_strategy.upsert_handler import UpsertHandler
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col, current_timestamp
from delta.tables import DeltaTable
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
        self.transform_manager = TransformationManager(self.spark, self.logger)
        self.upsert_manager = UpsertHandler(self.spark, self.logger)

    def load_orchestration_config(self, control_storage_container_endpoint: str, control_table_name: str, orchestration_config: list):
        """
        Load the orchestration configuration into a DataFrame and save it as a delta table.
        
        :param control_storage_container_endpoint: The endpoint of the storage container where the orchestration configuration is stored.
        :param control_table_name: The name of the control table.
        :param orchestration_config: A list of dictionaries representing the orchestration configuration.

        Example = [
            {
                'notebook_name': 'Notebook1',
                'notebook_path': '/path/to/notebook1',
                'dependencies': '["Notebook2", "Notebook3"]',
                'parameters': '{"param1": "value1", "param2": "value2"}',
                'timeout_per_cell_seconds': 600,
                'retry_attempts': 3,
                'interval_between_retry_attempt_seconds': 60,
                'active': 1,
                'process_group': 1
            },
            {
                'notebook_name': 'Notebook2',
                'notebook_path': '/path/to/notebook2',
                'dependencies': '["Notebook3"]',
                'parameters': '{"param1": "value1", "param2": "value2"}',
                'timeout_per_cell_seconds': 300,
                'retry_attempts': 2,
                'interval_between_retry_attempt_seconds': 30,
                'active': 1,
                'process_group': 1
            }
        ]
        """
        try:
            # Set path to the orchestration config table
            table_path = f"{control_storage_container_endpoint}/Tables/{control_table_name}"
            
            # Attempt to access the Delta table
            try:
                DeltaTable.forPath(self.spark, table_path)
            except Exception as e:
                self.logger.info('Table does not exist, creating it.')

                # Create the table with the correct schema
                self.spark.sql(f"""
                    CREATE TABLE delta.`{table_path}` (
                        orchestration_config_key INT,
                        notebook_name VARCHAR(4000),
                        notebook_path VARCHAR(4000),
                        dependencies VARCHAR(4000),
                        parameters VARCHAR(4000),
                        timeout_per_cell_seconds INT,
                        retry_attempts INT,
                        interval_between_retry_attempt_seconds INT,
                        active INT,
                        process_group INT,
                        inserted_date_time TIMESTAMP,
                        updated_date_time TIMESTAMP
                    ) USING delta;
                """)
                self.logger.info(f"Log table {control_table_name} has been created.")
            
            # Define the schema for the model config DataFrame
            schema = StructType([
                StructField('notebook_name', StringType(), True),
                StructField('notebook_path', StringType(), True),
                StructField('dependencies', StringType(), True),
                StructField('parameters', StringType(), True),
                StructField('timeout_per_cell_seconds', IntegerType(), True),
                StructField('retry_attempts', IntegerType(), True),
                StructField('interval_between_retry_attempt_seconds', IntegerType(), True),
                StructField('active', IntegerType(), True),
                StructField('process_group', IntegerType(), True)
            ])

            # Create a DataFrame from the model config
            df = self.spark.createDataFrame(orchestration_config, schema)

            # Match columns
            match_key_columns = ['notebook_name', 'process_group']

            # Build table config
            table_config = [
                {
                    'table_name': control_table_name,
                    'dataframe': df,
                    'match_keys': match_key_columns,
                    'upsert_type': 'generic',
                    'skey_column': 'orchestration_config_key'
                }
            ]

            # Upsert table
            self.upsert_manager.upsert_data_concurrently(table_config)

            self.logger.info('Model config loaded to delta successfully.')
        except Exception as e:
            self.logger.error(f'Failed to create and load model config: {e}')
            raise e

    def build_dag(self, control_storage_container_endpoint: str, control_table_name: str, process_group: int) -> dict:
        """
        Build a Directed Acyclic Graph (DAG) for data processing based on the orchestration configuration.
        
        :param control_storage_container_endpoint: The endpoint of the storage container where the orchestration configuration is stored.
        :param control_table_name: The name of the control table.
        :param process_group: The load group to use for building the DAG.
        :return: A dictionary representing the DAG.

        Example:
        process_group = 1
        dag = OrchestrationManager.build_dag(control_storage_container_endpoint, process_group)
        """
        try:
            # Set the path to the orchestration config table
            table_path = f'{control_storage_container_endpoint}/Tables/{control_table_name}'

            # Fetch the orchestration configurations from the control table
            df_control = self.spark.load(table_path).filter(
                (col('active') == 1) & (col('process_group') == process_group)
            )
            control_list = map(lambda row: row.asDict(), df_control.collect())
            DAG = {'activities': []}

            # Iterate through each control entry and construct the DAG activities
            for control in control_list:

                # Ensure parameters are correctly converted from JSON string to a dictionary
                args = json.loads(control['parameters']) if control['parameters'] and control['parameters'] != 'null' else {}

                # Prepare the activity dictionary using correct keys for runMultiple
                activity = {
                    'name': control['notebook_name'],
                    'path': control['notebook_path'],
                    'timeoutPerCellInSeconds': control['timeout_per_cell_seconds'],
                    'args': args,
                    'retry': control['retry_attempts'],
                    'retryIntervalInSeconds': control['interval_between_retry_attempt_seconds'],
                    'dependencies': json.loads(control['dependencies']) if control['dependencies'] else []
                }
                DAG['activities'].append(activity)
            return DAG
        except Exception as e:
            self.logger.error(f'Failed to build the DAG: {e}')
            raise

    def log_orchestration_execution(self, control_storage_container_endpoint: str, log_table_name: str, execution_results: dict):
        """
        Log the execution results into a DataFrame and save it as a delta table.
        
        :param control_storage_container_endpoint: The endpoint of the storage container where the orchestration log is stored.
        :param log_table_name: The name of the control table.
        :param execution_results: A dictionary representing the execution results.

        Example:
        log_orchestration_execution(control_storage_container_endpoint, log_table_name, execution_results)
        """
        try:
            # Define the schema for the log DataFrame
            schema = StructType([
                StructField('notebook_name', StringType(), True),
                StructField('execution_status', StringType(), True),
                StructField('exception', StringType(), True),
                StructField('log_timestamp', TimestampType(), True)
            ])

            # Create a list to store the log records
            logs = []

            # Get the current time
            current_time = current_timestamp()

            # Iterate over the execution results and create a log record for each one
            for notebook_name, result in execution_results.items():
                exception_message = str(result['exception']) if result['exception'] else None
                # Truncate the exception message to 4000 characters
                if exception_message and len(exception_message) > 4000:
                    exception_message = exception_message[:4000]
                log = {
                    'notebook_name': notebook_name,
                    'execution_status': 'Failed' if result['exception'] else 'Success',
                    'exception': exception_message,
                    'log_timestamp': current_time
                }
                logs.append(log)

            # Create a DataFrame from the logs
            df = self.spark.createDataFrame(logs, schema)

            # Set log table
            log_table_name = 'orchestration_log'

            # Set path to the log table
            table_path = f'{control_storage_container_endpoint}/Tables/{log_table_name}'
            
            # Check if the log table exists and create it if it doesn't
            try:
                DeltaTable.forPath(self.spark, table_path)
            except Exception as e:
                self.logger.info('Table does not exist, creating it.')

                # Create logging table
                self.spark.sql(f'''
                    CREATE TABLE delta.`{table_path}` (
                        orchestration_log_key INT,
                        notebook_name VARCHAR(4000),
                        execution_status VARCHAR(4000),
                        exception VARCHAR(4000),
                        log_timestamp TIMESTAMP
                    ) USING delta
                ''')
                self.logger.info(f'Log table {log_table_name} has been created.')

            # Add key to the log DataFrame using transform manager
            df = self.transform_manager.stage_dataframe_with_keys(control_storage_container_endpoint, log_table_name, df, 'orchestration_log_key')
            
            # Write the DataFrame to the delta layer
            df.write.format('delta').mode('append').save(table_path)
        except Exception as e:
            self.logger.error(f'Failed to save execution log: {e}')
            raise