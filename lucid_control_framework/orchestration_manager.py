from lucid_control_framework.transformation_manager import TransformationManager
from lucid_control_framework.upsert_strategy.upsert_handler import UpsertHandler
from lucid_control_framework.delta_table_manager import DeltaTableManager
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col
from delta.tables import DeltaTable
from notebookutils import mssparkutils
from typing import Optional
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
        self.table_manager = DeltaTableManager(self.spark, self.logger)

    def load_orchestration_config(
            self,
            control_table_name: str,
            orchestration_config: list,
            write_method: str = 'catalog',
            control_storage_container_endpoint: Optional[str] = None
        ):
        """
        Load the orchestration configuration into a DataFrame and save it as a Delta table.

        :param control_table_name: The name of the control table.
        :param orchestration_config: A list of dictionaries representing the orchestration configuration.
        :param write_method: The method to use for writing the table. Should be either 'catalog' or 'path'. Defaults to 'catalog'.
        :param control_storage_container_endpoint: The endpoint of the storage container where the orchestration configuration is stored. Optional and defaults to None.

        This function performs the following steps:
        1. Attempts to access the Delta table using the specified write method.
        2. If the table does not exist, it creates the table with the necessary schema.
        3. Defines the schema for the orchestration configuration DataFrame.
        4. Creates a DataFrame from the orchestration configuration list.
        5. Specifies the composite columns and primary key column.
        6. Builds the table configuration for upserting data.
        7. Calls the upsert manager to perform the upsert operation concurrently.
        8. Logs the successful loading of the orchestration configuration.

        Example:
            orchestration_config = [
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
            load_orchestration_config(control_table_name, orchestration_config)
        """
        try:
            # Attempt to access the Delta table
            try:
                if write_method == 'catalog':
                    DeltaTable.forName(self.spark, control_table_name)
                elif write_method == 'path':
                    DeltaTable.forPath(self.spark, f'{control_storage_container_endpoint}/Tables/{control_table_name}')
                else:
                    raise ValueError(f'Invalid write method: {write_method}')
            except Exception as e:
                self.logger.info('Table does not exist, creating it.')

                # Construct log table path
                if write_method == 'catalog':
                    create_table = f'CREATE TABLE {control_table_name}'
                elif write_method == 'path':
                    control_table_path = f'{control_storage_container_endpoint}/Tables/{control_table_name}'
                    create_table = f'CREATE TABLE delta.`{control_table_path}`'
                else:
                    raise ValueError(f'Invalid write method: {write_method}')
                    
                # Create logging table
                self.spark.sql(f"""
                    {create_table} (
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
            composite_columns = ['notebook_name', 'process_group']

            # Primary key column
            primary_key_column = 'orchestration_config_key'

            # Build table config
            table_config = [
                {
                    'table_name': control_table_name,
                    'dataframe': df,
                    'composite_columns': composite_columns,
                    'upsert_type': 'generic',
                    'primary_key_column': primary_key_column
                }
            ]

            # Upsert table
            self.upsert_manager.upsert_data_concurrently(table_config)

            self.logger.info('Model config loaded to delta successfully.')
        except Exception as e:
            self.logger.error(f'Failed to create and load model config: {e}')
            raise e

    def build_and_execute_dag(
            self,
            control_table_name: str,
            process_group: int,
            read_method: str = 'catalog',
            control_storage_container_endpoint: Optional[str] = None,
        ) -> dict:
        """
        Build a Directed Acyclic Graph (DAG) for data processing based on the orchestration configuration.

        :param control_table_name: The name of the control table.
        :param process_group: The load group to use for building the DAG.
        :param read_method: The method to use for reading the table. Can be either 'path' or 'catalog'.
        :param control_storage_container_endpoint: The endpoint of the storage container where the orchestration configuration is stored.

        :return: A dictionary representing the DAG.

        This function performs the following steps:
        1. Fetches the orchestration configurations from the control table.
        2. Filters the configurations based on the active status and process group.
        3. Constructs a list of control entries as dictionaries.
        4. Iterates through each control entry to construct the DAG activities.
        5. Converts parameters from JSON strings to dictionaries.
        6. Prepares the activity dictionary with necessary keys for runMultiple.
        7. Appends the activity to the DAG.
        8. Executes the DAG using mssparkutils.notebook.runMultiple.
        9. Returns the execution results.

        Example:
            process_group = 1
            dag = OrchestrationManager.build_dag(control_storage_container_endpoint, process_group)
        """
        try:
            # Fetch the orchestration configurations from the control table
            df_control = self.table_manager.read_delta_table(control_table_name, control_storage_container_endpoint, read_method).filter(
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

            # Execute the DAG
            execution_results = mssparkutils.notebook.runMultiple(DAG)
            
            return execution_results
        except Exception as e:
            self.logger.error(f'Failed to build the DAG: {e}')
            raise

    def log_orchestration_execution(
            self,
            log_table_name: str,
            execution_results: dict,
            write_method: str = 'catalog',
            control_storage_container_endpoint: Optional[str] = None,
        ):
        """
        Log the execution results into a DataFrame and save it as a Delta table.

        :param log_table_name: The name of the control table.
        :param execution_results: A dictionary representing the execution results.
        :param write_method: The method to use for writing the table. Can be either 'path' or 'catalog'.
        :param control_storage_container_endpoint: The endpoint of the storage container where the orchestration log is stored.

        This function performs the following steps:
        1. Defines the schema for the log DataFrame.
        2. Creates a list to store the log records.
        3. Iterates over the execution results and creates a log record for each one.
        4. Truncates the exception message to 4000 characters if necessary.
        5. Creates a DataFrame from the logs.
        6. Checks if the log table exists and creates it if it doesn't.
        7. Constructs the log table path and creates the table with the necessary schema if it does not exist.
        8. Defines the primary key column for the log table.
        9. Builds the table configuration for upserting data.
        10. Calls the upsert manager to perform the upsert operation concurrently.
        11. Logs the successful saving of the execution log.

        Example:
            log_orchestration_execution(log_table_name, execution_results, write_method, control_storage_container_endpoint)
        """
        try:
            # Define the schema for the log DataFrame
            schema = StructType([
                StructField('notebook_name', StringType(), True),
                StructField('execution_status', StringType(), True),
                StructField('exception', StringType(), True)
            ])

            # Create a list to store the log records
            logs = []

            # Iterate over the execution results and create a log record for each one
            for notebook_name, result in execution_results.items():
                exception_message = str(result['exception']) if result['exception'] else None
                # Truncate the exception message to 4000 characters
                if exception_message and len(exception_message) > 4000:
                    exception_message = exception_message[:4000]
                log = {
                    'notebook_name': notebook_name,
                    'execution_status': 'Failed' if result['exception'] else 'Success',
                    'exception': exception_message
                }
                logs.append(log)

            # Create a DataFrame from the logs
            df = self.spark.createDataFrame(logs, schema)

            # Check if the log table exists and create it if it doesn't
            try:
                if write_method == 'catalog':
                    DeltaTable.forName(self.spark, log_table_name)
                elif write_method == 'path':
                    DeltaTable.forPath = f'{control_storage_container_endpoint}/Tables/{log_table_name}'
            except Exception as e:
                self.logger.info('Table does not exist, creating it.')

                # Construct log table path
                if write_method == 'catalog':
                    create_table = f'CREATE TABLE {log_table_name}'
                elif write_method == 'path':
                    log_table_path = f'{control_storage_container_endpoint}/Tables/{log_table_name}'
                    create_table = f'CREATE TABLE delta.`{log_table_path}`'
                else:
                    raise ValueError(f'Invalid write method: {write_method}')

                # Create logging table
                self.spark.sql(f"""
                        {create_table} (
                        orchestration_log_key INT,
                        notebook_name VARCHAR(4000),
                        execution_status VARCHAR(4000),
                        exception VARCHAR(4000),
                        inserted_date_time TIMESTAMP,
                        updated_date_time TIMESTAMP
                    ) USING delta
                """)
                self.logger.info(f'Log table {log_table_name} has been created.')

            
            # Primary key column
            primary_key_column = 'orchestration_log_key'

            # Build table config
            table_config = [
                {
                    'table_name': log_table_name,
                    'dataframe': df,
                    'composite_columns': None,
                    'upsert_type': 'generic',
                    'primary_key_column': primary_key_column
                }
            ]

            # Upsert table
            self.upsert_manager.upsert_data_concurrently(table_config)
            
        except Exception as e:
            self.logger.error(f'Failed to save execution log: {e}')
            raise