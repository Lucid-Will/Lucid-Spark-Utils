import logging
from typing import List, Dict, Optional, Tuple, Callable
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from .utility_manager import UtilityManager
from .orchestration_manager import OrchestrationManager
from .upsert_strategy.upsert_handler import UpsertHandler
from .semantic_model_manager import SemanticModelManager
from .data_validation_manager import Validation
from .transformation_manager import TransformationManager
from .file_manager import FileManager
from .delta_table_manager import DeltaTableManager

class LucidUtils:
    """
    Provides utility functions for managing and manipulating data in Lucid Control Framework.

    :param spark: The SparkSession object.
    """

    def __init__(self):
        """
        Initializes the LucidUtils class.

        :param spark: The SparkSession object.
        """

        # Initialize the SparkSession object
        self.spark = SparkSession.builder.getOrCreate()
        
        # Set up logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

        # Initialize the Lucid Support Modules
        self.utility_manager = UtilityManager()
        self.upsert_handler = UpsertHandler(self.spark, self.logger)
        self.orchestration_manager = OrchestrationManager(self.spark, self.logger)
        self.semantic_model_manager = SemanticModelManager(self.spark, self.logger)
        self.validation = Validation(self.spark, self.logger)
        self.dataframe_transformation_manager = TransformationManager(self.spark, self.logger)
        self.file_manager = FileManager(self.spark, self.logger)
        self.table_manager = DeltaTableManager(self.spark, self.logger)

    def get_secret_value_as_user(
            self,
            key_vault_name: str, 
            secret_name: str
        ) -> str:
        """
        Retrieves a secret value from Azure Key Vault as a user.

        :param key_vault_name: The name of the key vault.
        :param secret_name: The name of the secret within the key vault.
        :param logger: The logger to use. If not provided, a default logger will be used.

        :return: The value of the secret retrieved from Azure Key Vault.

        :raises Exception: If there's a problem retrieving the secret from Azure Key Vault.

        This function performs the following steps:
        1. Constructs the vault URL using the key_vault_name.
        2. Tries to retrieve the secret from the key vault using the secret_name.
        3. Logs and raises any exceptions that occur during the secret retrieval process.

        Example:
            key_vault_name = "my-key-vault"
            secret_name = "my-secret"
            secret_value = UtilityManager.get_secret_value_as_user(key_vault_name, secret_name)
        """
        self.logger.info(f"Getting secret value as user")
        return self.utility_manager.get_secret_value_as_user(key_vault_name, secret_name)
    
    def get_secret_value_as_managed_identity(
            self,
            key_vault_name: str, 
            secret_name: str, 
            managed_identity_name: str
        ) -> str:
        """
        Retrieves a secret value from Azure Key Vault using a managed identity.

        :param key_vault_name: The name of the key vault.
        :param secret_name: The name of the secret within the key vault.
        :param managed_identity_name: The name of the managed identity resource to use for secret retrieval.
        :param logger: The logger to use. If not provided, a default logger will be used.

        :return: The value of the secret retrieved from Azure Key Vault.

        :raises Exception: If there's a problem retrieving the secret from Azure Key Vault.

        This function performs the following steps:
        1. Constructs the vault URL using the key_vault_name.
        2. Tries to retrieve the secret from the key vault using the secret_name and managed_identity_name.
        3. Logs and raises any exceptions that occur during the secret retrieval process.

        Example:
            key_vault_name = "my-key-vault"
            secret_name = "my-secret"
            managed_identity_name = "my-managed-identity"
            secret_value = UtilityManager.get_secret_value_as_managed_identity(key_vault_name, secret_name, managed_identity_name)
        """
        self.logger.info(f"Getting secret value as managed identity.")
        return self.utility_manager.get_secret_value_as_managed_identity(key_vault_name, secret_name, managed_identity_name)
    
    def read_file(
            self, 
            file_path: str, 
            file_format: str = 'csv'
        ) -> DataFrame:
        """
        Reads a file from the storage account.

        :param file_path: The path of the file.
        :param file_format: The format of the file. Can be 'csv', 'json', 'parquet', etc.

        :return: The DataFrame read from the file.

        Raises:
            ValueError: If an invalid file format is provided.
            Exception: If any error occurs during file reading.

        This function performs the following steps:
        1. Checks the file format and reads the file accordingly:
            - For CSV format, reads the file with headers and infers the schema.
            - For JSON format, reads the file as a JSON file.
            - For Parquet format, reads the file as a Parquet file.
        2. Raises a ValueError if an invalid file format is provided.
        3. Logs and raises any exceptions that occur during file reading.

        Example:
            file_path = 'abfss://workspaceid.dfs.core.windows.net/data/Files/file1.csv'
            file_format = 'csv'
            dataframe = read_file(file_path, file_format)
        """
        self.logger.info(f"Reading file")
        return self.file_manager.read_file(file_path, file_format)
    
    def read_files_concurrently(
            self, 
            file_paths: list, 
            file_format: str = 'csv'
        ) -> list:
        """
        Reads multiple files from the storage account concurrently.

        :param file_paths: A list of file paths.
        :param file_format: The format of the files. Can be 'csv', 'json', 'parquet', etc.

        :return: A list of DataFrames read from the files.

        Raises:
            Exception: If any error occurs during file reading.

        This function performs the following steps:
        1. Defines a nested function to read a single file.
        2. Determines the maximum number of worker threads based on the number of file paths and available CPUs.
        3. Uses ThreadPoolExecutor to read files concurrently.
        4. Submits tasks to the executor for each file path.
        5. Processes the results as they complete.
        6. Collects the DataFrames from the completed tasks.
        7. Logs any errors that occur during file reading.

        Example:
            file_paths = [
                'abfss://workspaceid.dfs.core.windows.net/data/Files/file1.csv',
                'abfss://workspaceid.dfs.core.windows.net/data/Files/file2.csv'
            ]
            file_format = 'csv'
            dataframes = read_files_concurrently(file_paths, file_format)
        """
        self.logger.info(f"Reading files concurrently")
        return self.file_manager.read_files_concurrently(file_paths, file_format)
    
    def write_file(
            self, 
            dataframe: DataFrame, 
            file_name: str, 
            storage_container_endpoint: str, 
            file_format: str = 'parquet'
        ):
        """
        Writes a DataFrame to a file in the storage account.

        :param dataframe: The DataFrame to write.
        :param file_name: The name of the file to write.
        :param storage_container_endpoint: The endpoint of the storage container.
        :param file_format: The format of the file. Can be 'csv', 'json', 'parquet', etc.

        Raises:
            ValueError: If an invalid file format is provided.
            Exception: If any error occurs during file writing.

        This function performs the following steps:
        1. Checks the file format and prepares the DataFrame accordingly:
            - For CSV and JSON formats, converts timestamp fields to strings to avoid issues during writing.
        2. Temporarily disables Arrow optimization for writing to CSV or JSON formats.
        3. Writes the DataFrame to the specified file format:
            - Converts the Spark DataFrame to a Pandas DataFrame for CSV and JSON formats and writes the file.
            - Directly writes the Spark DataFrame for Parquet format.
        4. Re-enables Arrow optimization after writing the file.
        5. Logs and raises any exceptions that occur during the file writing process.

        Example:
            dataframe = df_data
            file_name = 'file1.parquet'
            storage_container_endpoint = 'abfss://workspaceid@onelake.dfs.fabric.microsoft.com/lakehouseid'
            file_format = 'parquet'
            write_file(dataframe, file_name, storage_container_endpoint, file_format)
        """
        self.logger.info(f"Writing file")
        return self.file_manager.write_file(dataframe, file_name, storage_container_endpoint, file_format)

    def write_files_concurrently(
            self, 
            dataframe_list: List[DataFrame], 
            file_names: List[str], 
            storage_container_endpoint: str, 
            file_format: str = 'parquet'
        ):
        """
        Writes multiple DataFrames to files in the storage account concurrently.

        :param dataframe_list: A list of DataFrames to write.
        :param file_names: A list of file names.
        :param storage_container_endpoint: The storage container endpoint.
        :param file_format: The format of the files. Can be 'csv', 'json', 'parquet', etc.

        :return: None. The function writes the DataFrames to files.

        Raises:
            ValueError: If the lengths of dataframe_list and file_names do not match.
            Exception: If any error occurs during file writing.

        This function performs the following steps:
        1. Defines a nested function to write a single DataFrame to a file.
        2. Determines the maximum number of worker threads based on the number of DataFrames and available CPUs.
        3. Uses ThreadPoolExecutor to write files concurrently.
        4. Submits tasks to the executor for each DataFrame and file name pair.
        5. Processes the results as they complete.
        6. Logs any errors that occur during file writing.

        Example:
            dataframe_list = [
                spark.createDataFrame([(1, "John", "Doe"), (2, "Jane", "Doe")], ["ID", "First Name", "Last Name"]),
                spark.createDataFrame([(3, "Jim", "Smith"), (4, "Jill", "Smith")], ["ID", "First Name", "Last Name"])
            ]
            file_names = ['file1.parquet', 'file2.parquet']
            storage_container_endpoint = 'abfss://workspaceid@onelake.dfs.fabric.microsoft.com/lakehouseid'
            file_format = 'parquet'
            write_files_concurrently(dataframe_list, file_names, storage_container_endpoint, file_format)
        """
        self.logger.info(f"Writing files concurrently")
        return self.file_manager.write_files_concurrently(dataframe_list, file_names, storage_container_endpoint, file_format)
    
    def read_delta_table(
            self, 
            table_name: str, 
            storage_container_endpoint: Optional[str] = None, 
            read_method: str = 'catalog'
        ) -> DataFrame:
        """
        Reads a Delta table into a DataFrame.

        :param table_name: The name of the Delta table.
        :param storage_container_endpoint: The storage container endpoint. Required if read_method is 'path'.
        :param read_method: The method to use for reading the table. Can be either 'path' or 'catalog'.

        :return: The DataFrame representing the Delta table.

        :raises Exception: If there's a problem reading the Delta table.
        :raises ValueError: If an invalid read_method is provided.

        This function performs the following steps:
        1. Checks the read method and reads the Delta table accordingly:
            - For 'catalog' read method, reads the table using the catalog method.
            - For 'path' read method, reads the table from the specified storage container endpoint.
        2. Raises a ValueError if an invalid read method is provided.
        3. Logs and raises any exceptions that occur during the Delta table reading process.

        Example:
            table_name = 'my_table'
            storage_container_endpoint = 'abfss://workspaceid@onelake.dfs.fabric.microsoft.com/lakehouseid'
            read_method = 'path'
            df = delta_table_reader.read_delta_table(table_name, storage_container_endpoint, read_method)
        """
        self.logger.info(f"Reading Delta table")
        return self.table_manager.read_delta_table(table_name, storage_container_endpoint, read_method)
    
    def read_delta_tables_concurrently(
            self, 
            table_names: List[str], 
            storage_container_endpoint: Optional[str] = None, 
            read_method: str = 'catalog'
        ) -> Dict[str, DataFrame]:
        """
        Reads multiple Delta tables into DataFrames concurrently.

        :param table_names: A list of Delta table names.
        :param storage_container_endpoint: The storage container endpoint. Required if read_method is 'path'.
        :param read_method: The method to use for reading the table. Can be either 'path' or 'catalog'.

        :return: A dictionary mapping table names to DataFrames.

        :raises Exception: If there's a problem reading the Delta tables.

        This function performs the following steps:
        1. Defines a nested function to read a single Delta table.
        2. Determines the maximum number of worker threads based on the number of table names and available CPUs.
        3. Uses ThreadPoolExecutor to read tables concurrently.
        4. Submits tasks to the executor for each table name.
        5. Processes the results as they complete.
        6. Collects the DataFrames from the completed tasks and stores them in a dictionary.
        7. Logs any errors that occur during table reading.

        Example:
            table_names = ['table1', 'table2']
            storage_container_endpoint = 'abfss://workspaceid@onelake.dfs.fabric.microsoft.com/lakehouseid'
            read_method = 'path'
            df_tables = delta_table_reader.read_delta_tables_concurrently(table_names, storage_container_endpoint, read_method)
        """
        self.logger.info(f"Reading Delta tables concurrently")
        return self.table_manager.read_delta_tables_concurrently(table_names, storage_container_endpoint, read_method)
    
    def write_delta_table(
            self, 
            dataframe: DataFrame, 
            table_name: str, 
            storage_container_endpoint: Optional[str] = None, 
            write_method: str = 'catalog', 
            write_mode: str = 'overwrite', 
            merge_schema: str = 'true'
        ) -> None:
        """
        Writes a DataFrame to a Delta table.

        :param dataframe: The DataFrame to write.
        :param table_name: The name of the Delta table.
        :param storage_container_endpoint: The storage container endpoint. Required if write_method is 'path'.
        :param write_method: The method to use for writing the table. Can be either 'path' or 'catalog'.
        :param write_mode: The mode to use for writing the table. Can be 'overwrite', 'append', 'ignore', 'error', or 'overwritePartitions'. Default is 'overwrite'.
        :param merge_schema: Whether to merge the schema of the DataFrame with the schema of the Delta table. Default is 'true'.

        :raises Exception: If there's a problem writing the Delta table.
        :raises ValueError: If an invalid write_method is provided or if storage_container_endpoint is not provided when using 'path' write method.

        This function performs the following steps:
        1. Checks the write method and writes the DataFrame to the Delta table accordingly:
            - For 'catalog' write method, writes the table using the catalog method.
            - For 'path' write method, writes the table to the specified storage container endpoint.
        2. Raises a ValueError if the storage_container_endpoint is not provided when using the 'path' write method.
        3. Sets the write mode and merge schema options for the Delta table write operation.
        4. Logs and raises any exceptions that occur during the Delta table writing process.

        Example:
            dataframe = df_data
            table_name = 'my_table'
            storage_container_endpoint = 'abfss://workspaceid.dfs.core.windows.net/data'
            write_method = 'path'
            write_mode = 'overwrite'
            merge_schema = 'true'
            delta_table_writer.write_delta_table(dataframe, table_name, storage_container_endpoint, write_method)
        """
        self.logger.info(f"Writing Delta table")
        return self.table_manager.write_delta_table(dataframe, table_name, storage_container_endpoint, write_method, write_mode, merge_schema)
    
    def stage_dataframe_with_keys(
            self, 
            target_table: str, 
            dataframe: DataFrame, 
            primary_key_column: Optional[str] = None, 
            composite_key_column: Optional[str] = None, 
            match_key_columns: Optional[List[str]] = None, 
            read_method: str = 'catalog',
            target_table_storage_container_endpoint: Optional[str] = None 
        ) -> Optional[DataFrame]:
        """
        Transforms a DataFrame by adding a new column with an integer hash based on specified key columns.
        It also adds a surrogate key column with values starting from the maximum key in the target table plus one.

        :param target_table: The target table to check for the maximum key.
        :param dataframe: The source DataFrame.
        :param primary_key_column: The name of the new surrogate key column to be added.
        :param composite_key_column: The name of the new natural key column to be added.
        :param composite_columns: List of columns to use for hash generation.
        :param read_method: The method to use for reading the target table. Can be either 'path' or 'catalog'.
        :param target_table_storage_container_endpoint: The storage container endpoint for the target table. Required if read_method is 'path'.

        :return: Transformed DataFrame with the new columns added, if specified.

        :raises ValueError: If an invalid read method is provided.
        :raises Exception: If there's a problem transforming the DataFrame.

        This function performs the following steps:
        1. Gets the column names from the DataFrame.
        2. Returns the DataFrame unchanged if no primary key column is specified.
        3. Initializes the maximum key value to 0.
        4. Selects the specified columns from the DataFrame.
        5. If composite columns are specified, generates an integer hash based on the composite columns and adds it as the new composite key column.
        6. Attempts to access the Delta table using the specified read method:
            - For 'catalog' read method, uses the catalog method to check for the table.
            - For 'path' read method, uses the specified storage container endpoint to check for the table.
        7. Retrieves the maximum key value from the target table if it exists.
        8. Creates a window specification for adding the surrogate key column.
        9. Adds the surrogate key column to the DataFrame with values starting from the maximum key value plus one.
        10. Returns the transformed DataFrame.
        11. Logs and raises any exceptions that occur during the process.

        Example:
            df = spark.createDataFrame([(1, "John", "Doe"), (2, "Jane", "Doe")], ["ID", "First Name", "Last Name"])
            df_transformed = stage_dataframe_with_keys("target_table", df, primary_key_column="skey", composite_key_column="nkey", composite_columns=["ID", "First Name"], read_method="catalog")
        """
        self.logger.info(f"Staging DataFrame with keys")
        return self.dataframe_transformation_manager.stage_dataframe_with_keys(target_table, dataframe, primary_key_column, composite_key_column, match_key_columns, read_method, target_table_storage_container_endpoint)

    def execute_transformations_concurrently(
            self,
            transformations: List[Tuple[Callable, Tuple]]
    ) -> List:
        """
        Executes multiple DataFrame transformation tasks concurrently, improving performance on multi-core systems.

        :param transformations: A list of tuples, where each tuple contains a transformation function
                                and its corresponding arguments.

        :return: A list of results from each transformation task, executed concurrently.

        :raises Exception: If there's a problem executing the transformations concurrently.

        This function performs the following steps:
        1. Determines the number of workers based on the number of tasks and available CPU cores.
        2. Uses ThreadPoolExecutor to execute transformation tasks concurrently.
        3. Submits all transformation tasks to the executor.
        4. Waits for all futures to complete and gathers results.
        5. Returns the list of results from each transformation task.
        6. Logs and raises any exceptions that occur during concurrent execution.

        Example:
            transformations = [
                (self.stage_dataframe_with_keys, (target_table1, df1, "skey", "nkey", ["ID", "First Name"], "catalog")),
                (self.stage_dataframe_with_keys, (target_table2, df2, "skey", "nkey", ["ID", "First Name"], "catalog"))
            ]
            results = self.execute_transformations_concurrently(transformations)
        """
        self.logger.info(f"Executing transformations concurrently")
        return self.dataframe_transformation_manager.execute_transformations_concurrently(transformations)

    def upsert_data_concurrently(
            self, 
            table_configs: List[Dict[str, str]], 
            storage_container_endpoint: Optional[str] = None, 
            write_method: str = 'catalog'
        ) -> None:
        """
        Performs upsert operations concurrently on multiple tables based on the provided configurations.

        :param table_configs: A list of dictionaries, each representing a table configuration. Each dictionary should include:
            'table_name': The name of the table to upsert.
            'upsert_type': The type of upsert operation to perform. Options are 'scd1' and 'scd2'.
            'primary_key': A list of columns that form the primary key.
            'composite_columns': A list of columns to use for creating a composite key.
        :param storage_container_endpoint: The endpoint of the storage container if using 'path' write method. This is optional and defaults to None.
        :param write_method: The method to use for saving the table. This should be either 'path' or 'catalog'. Defaults to 'catalog'.

        :return: None. The function performs upsert operations on the tables concurrently.

        Raises:
            Exception: If any error occurs during the upsert operation.

        Example:
        table_configs = [
            {
                'table_name': 'table1',
                'upsert_type': 'scd2',
                'primary_key': ['id'],
                'composite_columns': ['id', 'name', 'age']
            },
            {
                'table_name': 'table2',
                'upsert_type': 'scd1',
                'primary_key': ['id'],
                'composite_columns': ['id', 'name', 'age']
            }
        ]
        upsert_data_concurrently(table_configs)

        This function performs the following steps:
        1. For each table configuration, it gets the strategy for the specified upsert type or uses the 'scd1' strategy by default.
        2. It logs a message indicating that the upsert operation is starting.
        3. It performs the upsert operation.
        4. It logs a message indicating that the upsert operation is complete.
        5. If any error occurs during the upsert operation, it logs an error message.
        6. It sets the thread pool size based on the number of tables and available CPUs.
        7. It creates a ThreadPoolExecutor and submits the upsert_table function for each table configuration.
        8. It waits for all futures to complete.
        """
        self.logger.info(f"Upserting data concurrently")
        return self.upsert_handler.upsert_data_concurrently(table_configs, storage_container_endpoint, write_method)
    
    def log_table_validation(
            self,
            target_table_name: str,
            log_table_name: str,
            read_method: str = 'catalog',
            write_method: str = 'catalog',
            target_storage_container_endpoint: Optional[str] = None,
            log_storage_container_endpoint: Optional[str] = None,
            primary_key_column: Optional[str] = None,
            stage_count: Optional[int] = None,
            invalid_count: Optional[int] = None,
            duplicate_count: Optional[int] = None,
            delete_count: Optional[int] = None
        ) -> None:
        """
        Log the validation results for a table.

        :param target_table_name: The name of the target table.
        :param log_table_name: The name of the log table.
        :param read_method: The method used to read the target table (catalog or path).
        :param write_method: The method used to write the log table (catalog or path).
        :param target_storage_container_endpoint: The storage container endpoint of the target table.
        :param log_storage_container_endpoint: The storage container endpoint of the log table.
        :param primary_key_column: The primary key column used for filtering.
        :param stage_count: The count of rows in the staging DataFrame.
        :param invalid_count: Count of invalid records.
        :param duplicate_count: Count of duplicate records.
        :param delete_count: Count of records flagged for deletion.

        :return: None. The function logs the validation results into the specified log table.

        Example:
            log_table_validation(
                "storage_container_endpoint", 
                "my_table", 
                "log_storage_container_endpoint", 
                "log_table", 
                "id", 
                100, 
                5, 
                10, 
                3
            )

        This function performs the following steps:
        1. Determines if the 'is_current' column exists in the target table for count validation.
        2. Reads the target table and filters current records if the 'is_current' column exists.
        3. Filters unknown records based on the primary key column.
        4. Captures row counts for staging, target table, invalid records, duplicate records, and delete flagged records.
        5. Defines the logging schema and creates a DataFrame with validation data.
        6. Adds a timestamp column to the validation DataFrame.
        7. Checks if the log table exists and creates it if not.
        8. Creates a surrogate key column in the validation DataFrame.
        9. Writes the validation DataFrame to the log table.
        """
        self.logger.info(f"Logging table validation")
        return self.validation.log_table_validation(target_table_name, log_table_name, read_method, write_method, target_storage_container_endpoint, log_storage_container_endpoint, primary_key_column, stage_count, invalid_count, duplicate_count, delete_count)
    
    def data_validation_check(
            self, 
            df_stage: DataFrame, 
            target_table_name: str,
            target_storage_container_endpoint: str,
            composite_columns: List[str], 
            read_method: str = 'catalog',
            write_method: str = 'catalog',
            primary_key_column: Optional[str] = None, 
            dropped_validation_columns: Optional[List[str]] = None
        ) -> None:
        """
        Identify invalid, duplicate, and delete flagged records by identifying them,
        saving them to specified paths, and returning a filtered DataFrame along with counts of invalid, duplicate, and delete flagged records.

        :param df_stage: The staging DataFrame.
        :param target_table_name: The name of the table being processed and the target table to check for delete flagged records.
        :param target_storage_container_endpoint: The endpoint for the storage account.
        :param composite_columns: List of columns to check for invalid values and duplicates and form the composite key.
        :param read_method: The method used to read the target table (catalog or path).
        :param write_method: The method used to write the log table (catalog or path).
        :param primary_key_column: The primary key column used for identifying records in the target table.
        :param dropped_validation_columns: List of columns to drop from the final DataFrame after validation.

        :return: A tuple containing the filtered DataFrame, count of invalid records, count of duplicate records, and count of delete flagged records.

        Example:
            data_validation_check(df_stage, "my_table", ["id", "name"], "mydatalake", "id", ["created_at", "updated_at"])

        This function performs the following steps:
        1. Gets the current date and time for the paths.
        2. Defines paths for invalid, duplicate, and delete flagged records.
        3. Checks each key column individually for -1 to flag invalid records.
        4. Separates invalid records and writes them to a CSV file.
        5. Groups by the key columns to identify duplicates and writes them to a CSV file.
        6. Identifies records in the target table that are flagged for deletion and writes them to a CSV file.
        7. Combines all validation DataFrames for writing to a Delta table.
        8. Writes combined validation DataFrames to a Delta table.
        9. Drops the specified validation columns from the final DataFrame and returns the final valid records.
        """
        self.logger.info(f"Starting data validation")
        return self.validation.data_validation_check(df_stage, target_table_name, target_storage_container_endpoint, composite_columns, read_method, write_method, primary_key_column, dropped_validation_columns)
    
    def hard_delete_records(
            self,
            target_table_name: str,
            primary_key_column: str,
            write_method: str = 'catalog',
            target_table_storage_container_endpoint: Optional[str] = None,
            df_delete=None
        ):
        """
        Perform hard deletes from the target table for records identified as delete.

        :param target_table_name: The name of the target table.
        :param primary_key_column: The primary key column used for identifying records in the target table.
        :param write_method: The method used to write the log table (catalog or path).
        :param target_table_storage_container_endpoint: The storage container endpoint of the target table.
        :param df_delete: DataFrame of records flagged for deletion.

        Example:
            hard_delete_records('schema.my_table', 'id', 'path', 'target_table_storage_container_endpoint', df_delete)

        This function performs the following steps:
        1. Checks if df_delete is None or empty.
        2. Converts the DataFrame of delete flagged records to a list of primary keys.
        3. Constructs the delete condition based on primary keys.
        4. Constructs the path of the target table if using the 'path' write method.
        5. Executes the delete statement on the target table.
        """
        self.logger.info(f"Hard deleting records")
        return self.validation.hard_delete_records(target_table_name, primary_key_column, write_method, target_table_storage_container_endpoint, df_delete)
    
    def soft_delete_records(
            self,
            target_table_name: str,
            primary_key_column: str,
            read_method: str = 'catalog',
            target_table_storage_container_endpoint: Optional[str] = None,
            df_delete=None
        ):
        """
        Perform soft deletes from the target table for records identified as delete flagged by setting the is_deleted column to True.
        If the is_deleted column does not exist, it will be added to the target table.

        :param target_table_storage_container_endpoint: The storage container endpoint of the target table.
        :param target_table_name: The name of the target table.
        :param primary_key_column: The primary key column used for identifying records in the target table.
        :param df_delete: DataFrame of records flagged for deletion.

        Example:
            soft_delete_records("storage_container_endpoint", "my_table", "id", df_delete)

        This function performs the following steps:
        1. Checks if df_delete is None or empty.
        2. Converts the DataFrame of delete flagged records to a list of primary keys.
        3. Reads the Delta table based on the provided read method.
        4. Checks if the is_deleted column exists in the target table.
        5. Adds the is_deleted column to the target table if it does not exist.
        6. Creates a condition for updating the is_deleted column.
        7. Updates the target table to set is_deleted to True for the identified records.
        """
        self.logger.info(f"Soft deleting records")
        return self.validation.soft_delete_records(target_table_name, primary_key_column, read_method, target_table_storage_container_endpoint, df_delete)
    
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
        self.logger.info(f"Loading orchestration configuration")
        return self.orchestration_manager.load_orchestration_config(control_table_name, orchestration_config, write_method, control_storage_container_endpoint)
    
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
        self.logger.info(f"Building DAG")
        return self.orchestration_manager.build_and_execute_dag(control_table_name, process_group, read_method, control_storage_container_endpoint)
    
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
        self.logger.info(f"Logging orchestration execution")
        return self.orchestration_manager.log_orchestration_execution(log_table_name, execution_results, write_method, control_storage_container_endpoint)
    
    def load_semantic_model_config(
            self,
            control_table_name: str,
            semantic_model_config: list,
            write_method: str = 'catalog',
            control_storage_container_endpoint: Optional[str] = None
        ):
        """
        Load the semantic model configuration into a DataFrame and save it as a Delta table.

        :param control_table_name: The name of the control table.
        :param semantic_model_config: A list of dictionaries representing the semantic model configuration.
        :param write_method: The method to use for writing the table. Can be either 'path' or 'catalog'.
        :param control_storage_container_endpoint: The endpoint of the storage container where the semantic model configuration is stored. Required if write_method is 'path'.

        :raises Exception: If there's a problem loading the semantic model configuration.
        :raises ValueError: If an invalid write method is provided.

        This function performs the following steps:
        1. Attempts to access the Delta table using the specified write method.
        2. If the table does not exist, creates the table with the necessary schema.
        3. Defines the schema for the semantic model configuration DataFrame.
        4. Creates a DataFrame from the semantic model configuration list.
        5. Specifies the composite columns and primary key column.
        6. Builds the table configuration for upserting data.
        7. Calls the upsert manager to perform the upsert operation concurrently.
        8. Logs the successful loading of the semantic model configuration.

        Example:
            semantic_model_config = [
                {
                    'semantic_model_key': 1,
                    'tenant_id': 'tenant-id',
                    'key_vault_name': 'key-vault-name',
                    'client_id': 'client-id',
                    'client_secret': 'client-secret',
                    'workspace_id': 'workspace-id',
                    'semantic_model_id': 'semantic-model-id',
                    'linked_service': 'linked-service',
                    'active': 1
                }
            ]
            load_semantic_model_config(control_table_name, semantic_model_config)
        """
        self.logger.info(f"Loading semantic model configuration")
        return self.semantic_model_manager.load_semantic_model_config(control_table_name, semantic_model_config, write_method, control_storage_container_endpoint)
    
    def get_service_principal_pbi_scope_token(
            self, tenant_id: str,
            key_vault_name: str,
            client_id: str,
            client_secret: str,
            secret_retrieval_method: str = 'user',
            managed_identity: Optional[str] = None
        ) -> str:
        """
        Retrieves an access token for a service principal using the Microsoft Authentication Library (MSAL).

        :param tenant_id: The Azure Active Directory tenant GUID.
        :param key_vault_name: The name of the Azure Key Vault containing the client ID and client secret.
        :param client_id: The name of the secret containing the client ID in Azure Key Vault.
        :param client_secret: The name of the secret containing the client secret in Azure Key Vault.
        :param secret_retrieval_method: The method to use for secret retrieval. Can be either 'user' or 'managed_identity'. Default is 'user'.
        :param managed_identity: The name of the linked service to use for secret retrieval if using 'managed_identity' method.

        :return: The access token for the service principal.

        :raises NameError: If managed_identity is not provided when secret_retrieval_method is 'managed_identity'.
        :raises Exception: If there's a problem retrieving the access token.

        This function performs the following steps:
        1. Retrieves the client ID and client secret from Azure Key Vault using the specified secret retrieval method:
            - Uses 'user' method if secret_retrieval_method is 'user'.
            - Uses 'managed_identity' method if secret_retrieval_method is 'managed_identity'.
        2. Creates a confidential client application using MSAL with the retrieved client ID and client secret.
        3. Acquires a token for the service principal with scope Power BI Service.
        4. Returns the access token.
        5. Logs and raises any exceptions that occur during the process.

        Example:
            token = get_service_principal_pbi_scope_token(
                tenant_id="my-tenant-id",
                key_vault_name="my-key-vault",
                client_id="my-client-id",
                client_secret="my-client-secret",
                secret_retrieval_method="user"
            )
        """
        self.logger.info(f"Getting service principal PBI scope token")
        return self.semantic_model_manager.get_service_principal_pbi_scope_token(tenant_id, key_vault_name, client_id, client_secret, secret_retrieval_method, managed_identity)
    
    def trigger_semantic_model_refresh(
            self, workspace_id: str,
            semantic_model_id: str,
            refresh_token: str
        ) -> None:
        """
        Triggers a refresh of a Power BI dataset.

        :param workspace_id: The ID of the Power BI workspace containing the dataset.
        :param semantic_model_id: The ID of the dataset to refresh.
        :param refresh_token: The refresh token for authentication.

        :return: None. The function logs the success or failure of the refresh operation.

        :raises Exception: If there's a problem triggering the dataset refresh.

        This function performs the following steps:
        1. Constructs the API endpoint URL for triggering the dataset refresh.
        2. Sets the headers and authentication using the provided refresh token.
        3. Sends a POST request to trigger the dataset refresh.
        4. Checks the response status code to determine if the refresh was successful.
        5. Logs a success message if the response status code is 202.
        6. Logs an error message if the response status code indicates a failure.
        7. Logs and raises any exceptions that occur during the process.

        Example:
            trigger_semantic_model_refresh("my-workspace-id", "my-dataset-id", "my-refresh-token")
        """
        self.logger.info(f"Triggering semantic model refresh")
        return self.semantic_model_manager.trigger_semantic_model_refresh(workspace_id, semantic_model_id, refresh_token)
    
    def get_semantic_model_refresh_status(
            self, workspace_id: str,
            semantic_model_id: str,
            refresh_token: str
        ) -> None:
        """
        Retrieves the refresh status of a Power BI dataset.

        :param workspace_id: The ID of the Power BI workspace containing the dataset.
        :param semantic_model_id: The ID of the dataset to refresh.
        :param refresh_token: The refresh token for authentication.

        :return: The refresh status of the dataset as a PySpark DataFrame.

        :raises Exception: If there's a problem retrieving the refresh status.

        This function performs the following steps:
        1. Constructs the API endpoint URL for retrieving the dataset refresh status.
        2. Sets the headers and authentication using the provided refresh token.
        3. Polls the API for the current state of the refresh until it is no longer "Unknown" or "InProgress".
        4. Defines the schema for the refresh status DataFrame.
        5. Converts the refresh start and end times to datetime objects.
        6. Creates a dictionary representing the refresh state.
        7. Parses the serviceExceptionJson for error details, if present.
        8. Creates a PySpark DataFrame with the defined schema from the refresh state dictionary.
        9. Returns the refresh status DataFrame.
        10. Logs and raises any exceptions that occur during the process.

        Example:
            df_refresh_status = get_semantic_model_refresh_status("my-workspace-id", "my-dataset-id", "my-refresh-token")
        """
        self.logger.info(f"Getting semantic model refresh status")
        return self.semantic_model_manager.get_semantic_model_refresh_status(workspace_id, semantic_model_id, refresh_token)
    
    def log_semantic_model_refresh_activity(
            self,
            log_table_name: str,
            refresh_state: Dict,
            write_method: str = 'catalog',
            log_table_storage_container_endpoint: Optional[str] = None,
        ) -> None:
        """
        Logs the refresh activity of a Power BI dataset.

        :param log_table_name: The name of the log table in the Azure Storage container.
        :param refresh_state: The refresh state of the dataset as a DataFrame.
        :param write_method: The method to use for writing the log table. Can be either 'path' or 'catalog'. Default is 'catalog'.
        :param log_table_storage_container_endpoint: The endpoint of the Azure Storage container where the log table is stored. Required if write_method is 'path'.

        :raises Exception: If there's a problem logging the refresh activity.
        :raises ValueError: If an invalid write method is provided.

        This function performs the following steps:
        1. Attempts to access the Delta log table using the specified write method.
        2. If the table does not exist, creates the log table with the necessary schema.
        3. Sets the composite columns and primary key column for the log table.
        4. Builds the table configuration for upserting the refresh state data.
        5. Calls the upsert manager to perform the upsert operation concurrently.
        6. Logs the successful logging of the refresh state.

        Example:
            log_dataset_refresh_activity(log_table_name, refresh_state, write_method, log_table_storage_container_endpoint)
        """
        self.logger.info(f"Logging semantic model refresh activity")
        return self.semantic_model_manager.log_semantic_model_refresh_activity(log_table_name, refresh_state, write_method, log_table_storage_container_endpoint)