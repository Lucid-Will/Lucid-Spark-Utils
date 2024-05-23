import logging
from typing import List, Dict, Optional, Tuple, Callable
from pyspark.sql import DataFrame
from .utility_manager import UtilityManager
from .upsert_strategy.upsert_handler import UpsertHandler
from .orchestration_manager import OrchestrationManager
from .semantic_model_manager import SemanticModelManager
from .data_validation_manager import Validation
from .dataframe_transformation_manager import DataFrameTransformationManager
from .file_manager import FileManager
from .delta_table_manager import DeltaTableReader

class LucidUtils:
    def __init__(self):
        # Set up logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

        self.utility_manager = UtilityManager()
        self.upsert_handler = UpsertHandler(self.logger)
        self.orchestration_manager = OrchestrationManager(self.logger)
        self.semantic_model_manager = SemanticModelManager(self.logger)
        self.validation = Validation(self.logger)
        self.dataframe_transformation_manager = DataFrameTransformationManager()
        self.file_manager = FileManager(self.logger)
        self.delta_table_reader = DeltaTableReader(self.logger)

    def get_secret_value_as_user(self, key_vault_name: str, secret_name: str) -> str:
        """
        Retrieves a secret value from Azure Key Vault.

        :param key_vault_name: The name of the key vault.
        :param secret_name: The name of the secret within the key vault.
        :return: The value of the secret retrieved from Azure Key Vault.
        """
        self.logger.info(f"Getting secret value for {key_vault_name}/{secret_name}")
        return self.utility_manager.get_secret_value_as_user(key_vault_name, secret_name)

    def upsert_data_concurrently(self, table_configs: List[Dict], storage_container_endpoint: Optional[str] = None, write_method: str = 'default') -> None:
        """
        Performs upsert operations concurrently on multiple tables based on the provided configurations.

        :param table_configs: A list of table configurations.
        :param storage_container_endpoint: The storage container endpoint (optional).
        :param write_method: The write method (default is 'default').
        """
        self.logger.info(f"Upserting data concurrently")
        return self.upsert_handler.upsert_data_concurrently(table_configs, storage_container_endpoint, write_method)
    
    def load_orchestration_config(self, orchestration_config: list) -> None:
        """
        Load the orchestration configuration into a DataFrame and save it as a delta table.

        :param orchestration_config: A list of dictionaries representing the orchestration configuration.
        """
        self.logger.info(f"Loading orchestration configuration")
        return self.orchestration_manager.load_orchestration_config(orchestration_config)
    
    def build_dag(self, process_group: int) -> dict:
        """
        Build a Directed Acyclic Graph (DAG) for data processing based on the orchestration configuration.

        :param load_group: The load group to use for building the DAG.
        :return: A dictionary representing the DAG.
        """
        self.logger.info(f"Building DAG")
        return self.orchestration_manager.build_dag(process_group)
    
    def log_orchestration_execution(self, execution_log: dict) -> None:
        """
        Log the execution results of the orchestration.

        :param execution_log: A dictionary representing the execution log.
        """
        self.logger.info(f"Logging orchestration execution")
        return self.orchestration_manager.log_execution(execution_log)
    
    def get_service_principal_pbi_scope_token(self, tenant_id: str, key_vault_name: str, client_id: str, client_secret: str, linked_service: str) -> str:
        """
        Retrieves an access token for a service principal using the Microsoft Authentication Library (MSAL).

        :param tenant_id: The Azure Active Directory tenant GUID.
        :param key_vault_name: The name of the Azure Key Vault containing the client ID and client secret.
        :param client_id: The name of the secret containing the client ID in Azure Key Vault.
        :param client_secret: The name of the secret containing the client secret in Azure Key Vault.
        :param linked_service: The name of the linked service to use for secret retrieval.
        :return: The access token for the service principal.
        """
        self.logger.info(f"Getting service principal PBI scope token")
        return self.semantic_model_manager.get_service_principal_pbi_scope_token(tenant_id, key_vault_name, client_id, client_secret, linked_service)
    
    def trigger_semantic_model_refresh(self, workspace_id: str, semantic_model_id: str, refresh_token: str) -> None:
        """
        Triggers a refresh of a Power BI dataset.

        :param workspace_id: The ID of the Power BI workspace containing the dataset.
        :param semantic_model_id: The ID of the dataset to refresh.
        :param refresh_token: The refresh token for the dataset.
        """
        self.logger.info(f"Triggering semantic model refresh")
        return self.semantic_model_manager.trigger_semantic_model_refresh(workspace_id, semantic_model_id, refresh_token)
    
    def get_semantic_model_refresh_status(self, workspace_id: str, semantic_model_id: str, refresh_token: str) -> DataFrame:
        """
        Retrieves the refresh status of a Power BI dataset.

        :param workspace_id: The ID of the Power BI workspace containing the dataset.
        :param semantic_model_id: The ID of the dataset to refresh.
        :param refresh_token: The refresh token for the dataset.
        :return: The DataFrame with refresh status of the dataset.
        """
        self.logger.info(f"Getting semantic model refresh status")
        return self.semantic_model_manager.get_semantic_model_refresh_status(workspace_id, semantic_model_id, refresh_token)
    
    def log_semantic_model_refresh_activity(self, refresh_state: str) -> None:
        """
        Logs the refresh activity of a Power BI dataset.

        :param refresh_state: The refresh state of the dataset.
        """
        self.logger.info(f"Logging semantic model refresh activity")
        return self.semantic_model_manager.log_semantic_model_refresh_activity(refresh_state)
    
    def log_table_validation(self, df_stage: DataFrame, target_table: str, target_filter: Optional[str] = None) -> None:
        """
        Log the validation results for a table.

        :param df_stage: The staging DataFrame
        :param target_table: The name of the target table
        :param target_filter: The filter condition for the target table
        """
        self.logger.info(f"Logging table validation")
        return self.validation.log_table_validation(df_stage, target_table, target_filter)
    
    def stage_dataframe_with_keys(self, target_table: str, dataframe: DataFrame, columns: List[str], skey_column: Optional[str] = None, nkey_column: Optional[str] = None, match_key_columns: Optional[List[str]] = None) -> Optional[DataFrame]:
        """
        Transforms a DataFrame by adding a new column with an integer hash based on specified key columns.
        It also adds a surrogate key column with values starting from the maximum key in the target table plus one.

        :param target_table: The target table to check for the maximum key.
        :param dataframe: The source DataFrame.
        :param columns: List of column names to include in the transformation.
        :param skey_column: The name of the new surrogate key column to be added.
        :param nkey_column: The name of the new natural key column to be added.
        :param match_key_columns: List of columns to use for hash generation.
        """
        self.logger.info(f"Staging DataFrame with keys")
        return self.dataframe_transformation_manager.stage_dataframe_with_keys(target_table, dataframe, columns, skey_column, nkey_column, match_key_columns)

    def execute_transformations_concurrently(self, transformations: List[Tuple[Callable, Tuple]]) -> List:
        """
        Execute transformations concurrently.

        :param transformations: A list of transformation functions and their arguments.
        :return: A list of results from the transformations.
        """
        self.logger.info(f"Executing transformations concurrently")
        return self.dataframe_transformation_manager.execute_transformations_concurrently(transformations)
    
    def read_file(self, file_path: str, file_format: str = 'csv') -> DataFrame:
        """
        Reads a file from the storage account.

        :param file_path: The path of the file.
        :param file_format: The format of the file. Can be 'csv', 'json', 'parquet', etc.
        :return: The DataFrame read from the file.

        Raises:
            ValueError: If an invalid file format is provided.
            Exception: If any error occurs during file reading.
        """
        self.logger.info(f"Reading file")
        return self.file_manager.read_file(file_path, file_format)
    
    def read_files_concurrently(self, file_paths: list, file_format: str = 'csv') -> list:
        """
        Reads multiple files from the storage account concurrently.

        :param file_paths: A list of file paths.
        :param file_format: The format of the files. Can be 'csv', 'json', 'parquet', etc.
        :return: A list of DataFrames read from the files.

        Raises:
            Exception: If any error occurs during file reading.
        """
        self.logger.info(f"Reading files concurrently")
        return self.file_manager.read_files_concurrently(file_paths, file_format)
    
    def write_file(self, dataframe: DataFrame, file_name: str, storage_container_endpoint: str, storage_container_name: str, file_format: str = 'parquet') -> None:
        """
        Writes a DataFrame to a file in the storage account.

        :param dataframe: The DataFrame to write.
        :param file_name: The name of the file to write.
        :param storage_container_endpoint: The endpoint of the storage container.
        :param storage_container_name: The name of the storage container.
        :param file_format: The format of the file. Can be 'csv', 'json', 'parquet', etc.

        Raises:
            Exception: If any error occurs during file writing.
        """
        self.logger.info(f"Writing file")
        return self.file_manager.write_file(dataframe, file_name, storage_container_endpoint, storage_container_name, file_format)

    def write_files_concurrently(self, dataframe_list: List[DataFrame], file_names: List[str], storage_container_endpoint: str, storage_container_name: str, file_format: str = 'parquet') -> None:
        """
        Writes multiple DataFrames to files in the storage account concurrently.

        :param dataframe_list: A list of DataFrames to write.
        :param file_names: A list of file names.
        :param storage_container_endpoint: The storage container endpoint.
        :param storage_container_name: The storage container name.
        :param file_format: The format of the files. Can be 'csv', 'json', 'parquet', etc.

        Raises:
            ValueError: If the lengths of dataframe_list and file_names do not match.
            Exception: If any error occurs during file writing.
        """
        self.logger.info(f"Writing files concurrently")
        return self.file_manager.write_files_concurrently(dataframe_list, file_names, storage_container_endpoint, storage_container_name, file_format)
    
    def read_delta_table(self, table_name: str, storage_container_endpoint: Optional[str] = None, read_method: str = 'default') -> DataFrame:
        """
        Reads a Delta table into a DataFrame.

        :param table_name: The name of the Delta table.
        :param storage_container_endpoint: The storage container endpoint. Required if read_method is 'abfss'.
        :param read_method: The method to use for reading the table. Can be either 'default' or 'abfss'.
        :return: The DataFrame representing the Delta table.

        :raises Exception: If there's a problem reading the Delta table.
        :raises ValueError: If an invalid read_method is provided.

        Example:
            table_name = 'my_table'
            storage_container_endpoint = 'https://storageaccount.blob.core.windows.net/container'
            read_method = 'default'
            df = lucid_utils.read_delta_table(table_name, storage_container_endpoint, read_method)
        """
        self.logger.info(f"Reading Delta table")
        return self.delta_table_reader.read_delta_table(table_name, storage_container_endpoint, read_method)
    
    def read_delta_tables_concurrently(self, table_names: List[str], storage_container_endpoint: Optional[str] = None, read_method: str = 'default') -> Dict[str, DataFrame]:
        """
        Reads multiple Delta tables into DataFrames concurrently.

        :param table_names: A list of Delta table names.
        :param storage_container_endpoint: The storage container endpoint. Required if read_method is 'abfss'.
        :param read_method: The method to use for reading the table. Can be either 'default' or 'abfss'.
        :return: A dictionary mapping table names to DataFrames.

        :raises Exception: If there's a problem reading the Delta tables.

        Example:
            table_names = ['table1', 'table2']
            storage_container_endpoint = 'https://storageaccount.blob.core.windows.net/container'
            read_method = 'default'
            df_tables = lucid_utils.read_delta_tables_concurrently(table_names, storage_container_endpoint, read_method)
        """
        self.logger.info(f"Reading Delta tables concurrently")
        return self.delta_table_reader.read_delta_tables_concurrently(table_names, storage_container_endpoint, read_method)