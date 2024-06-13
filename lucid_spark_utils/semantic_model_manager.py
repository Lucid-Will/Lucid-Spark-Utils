from lucid_spark_utils.transformation_manager import TransformationManager
from lucid_spark_utils.upsert_strategy.upsert_handler import UpsertHandler
from lucid_spark_utils.delta_table_manager import DeltaTableManager
from .utility_manager import UtilityManager
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType, IntegerType
from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql import Row
from typing import Optional
from pyspark.sql import DataFrame
import requests
import logging
import time
import msal
import json

class SemanticModelManager:
    """
    A class for managing Power BI semantic models.
    """

    def __init__(self, spark, logger=None):
        """
        Initializes the SemanticModelManager.

        :param spark: The SparkSession to use for data processing.
        :param logger: An optional logger for logging messages. If not provided, a default logger is used.
        """
        self.spark = spark
        self.logger = logger if logger else logging.getLogger(__name__)
        self.transform_manager = TransformationManager(self.spark, self.logger)
        self.table_manager = DeltaTableManager(self.spark, self.logger)
        self.upsert_manager = UpsertHandler(self.spark, self.logger)

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
        try:
            # Attempt to access the Delta table
            try:
                if write_method == 'catalog':
                    DeltaTable.forName(self.spark, control_table_name)
                elif write_method == 'path':
                    control_table_path = f'{control_storage_container_endpoint}/Tables/{control_table_name}'
                    DeltaTable.forPath(self.spark, control_table_path)
                else:
                    raise ValueError(f'Invalid write method: {write_method}')
            except Exception as e:
                self.logger.info('Table does not exist, creating it.')

                # Construct log table path
                if write_method == 'catalog':
                    create_table = f'CREATE TABLE {control_table_name}'
                elif write_method == 'path':
                    create_table = f'CREATE TABLE delta.`{control_table_path}`'
                else:
                    raise ValueError(f'Invalid write method: {write_method}')
                    
                # Create logging table with appropriate field lengths
                self.spark.sql(f"""
                    {create_table} (
                        semantic_model_key INT,
                        tenant_id VARCHAR(100),
                        key_vault_name VARCHAR(150),
                        client_id VARCHAR(150),
                        client_secret VARCHAR(150),
                        workspace_id VARCHAR(100),
                        semantic_model_id VARCHAR(100),
                        linked_service VARCHAR(150),
                        active INT,
                        inserted_date_time TIMESTAMP,
                        updated_date_time TIMESTAMP
                    ) USING delta;
                """)
                self.logger.info(f"Log table {control_table_name} has been created.")
            
            # Define the schema for the model config DataFrame
            schema = StructType([
                StructField('semantic_model_key', IntegerType(), True),
                StructField('tenant_id', StringType(), True),
                StructField('key_vault_name', StringType(), True),
                StructField('client_id', StringType(), True),
                StructField('client_secret', StringType(), True),
                StructField('workspace_id', StringType(), True),
                StructField('semantic_model_id', StringType(), True),
                StructField('linked_service', StringType(), True),
                StructField('active', IntegerType(), True)
            ])

            # Create a DataFrame from the model config
            df = self.spark.createDataFrame(semantic_model_config, schema)
            
            # Match columns
            composite_columns = ['workspace_id', 'semantic_model_id']

            # Primary key column
            primary_key_column = 'semantic_model_key'

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

            self.logger.info('Semantic model config loaded to delta successfully.')
        except Exception as e:
            self.logger.error(f'Failed to create and load semantic model config: {e}')
            raise e
    
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
        try:
            # Retrieve the client ID and client secret from Azure Key Vault
            if secret_retrieval_method == 'user':
                client_id = UtilityManager.get_secret_value_as_user(key_vault_name, client_id)
                client_secret = UtilityManager.get_secret_value_as_user(key_vault_name, client_secret)
            elif secret_retrieval_method == 'managed_identity':
                if managed_identity is None:
                    self.logger.error("Managed identity name is required for secret retrieval method 'managed_identity'.")
                    raise NameError("Managed identity name is required for secret retrieval method 'managed_identity'.")
                client_id = UtilityManager.get_secret_value_as_managed_identity(key_vault_name, client_id, managed_identity)
                client_secret = UtilityManager.get_secret_value_as_managed_identity(key_vault_name, client_secret, managed_identity)

            # Create a confidential client application using MSAL
            app = msal.ConfidentialClientApplication(
                client_id=client_id,
                client_credential=client_secret,
                authority=f"https://login.microsoftonline.com/{tenant_id}"
            )

            # Acquire a token for the service principal with scope Power BI Service
            result = app.acquire_token_for_client(scopes=["https://analysis.windows.net/powerbi/api/.default"])

            # Return the access token
            return result["access_token"]

        except Exception as e:
            self.logger.error(f"An unexpected error occurred in get_service_principal_token: {e}")
            raise
    
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
        try:
            # Construct the API endpoint URL
            api_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{semantic_model_id}/refreshes"

            # Set the headers and authentication
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {refresh_token}"
            }
            
            # Send a POST request to trigger the dataset refresh
            response = requests.post(api_url, headers=headers)

            # Check the response status code
            if response.status_code == 202:
                self.logger.info("Dataset refresh triggered successfully.")
            else:
                self.logger.error(f"Dataset refresh failed with status code {response.status_code}.")

        except Exception as e:
            self.logger.error(f"An unexpected error occurred in trigger_dataset_refresh: {e}")
            raise
        return response
    
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
        try:
            # Construct the API endpoint URL
            api_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{semantic_model_id}/refreshes?$top=1"

            # Set the headers and authentication
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {refresh_token}"
            }

            # Poll for current state of the refresh
            start_time = time.time()
            while True:
                response = requests.get(api_url, headers=headers)
                response_json = json.loads(response.text)
                refreshes = response_json["value"]
                if refreshes[0]["status"] == "Unknown" or refreshes[0]["status"] == "InProgress":
                    time.sleep(20)
                    if time.time() - start_time > 1200:
                        break
                else:
                    break
            
            # Define the schema
            schema = StructType([
                StructField("workspace_id", StringType(), True),
                StructField("semantic_model_id", StringType(), True),
                StructField("refresh_id", StringType(), True),
                StructField("start_time", TimestampType(), True),
                StructField("end_time", TimestampType(), True),
                StructField("duration", FloatType(), True),
                StructField("status", StringType(), True),
                StructField("error_code", StringType(), True),
                StructField("error_description", StringType(), True)
            ])

            # Convert the refresh start and end times to datetime objects
            refreshes[0]["startTime"] = datetime.strptime(refreshes[0]["startTime"], "%Y-%m-%dT%H:%M:%S.%fZ")
            refreshes[0]["endTime"] = datetime.strptime(refreshes[0]["endTime"], "%Y-%m-%dT%H:%M:%S.%fZ")

            # Create the refresh state
            refresh_state = {
                "workspace_id": workspace_id,
                "semantic_model_id": semantic_model_id,
                "refresh_id": refreshes[0]["id"],
                "start_time": refreshes[0]["startTime"],
                "end_time": refreshes[0]["endTime"],
                "duration": (refreshes[0]["endTime"] - refreshes[0]["startTime"]).total_seconds(),
                "status": refreshes[0]["status"],
                "error_code": None,
                "error_description": None
            }

            # Parse serviceExceptionJson for error details
            if "serviceExceptionJson" in refreshes[0]:
                service_exception_json = json.loads(refreshes[0]["serviceExceptionJson"])
                refresh_state["error_code"] = service_exception_json["errorCode"]
                refresh_state["error_description"] = service_exception_json["errorDescription"]

            # Create a PySpark DataFrame with the defined schema
            df_refresh_state = self.spark.createDataFrame([Row(**refresh_state)], schema=schema)

            return df_refresh_state
        except Exception as e:
            self.logger.error(f"An unexpected error occurred in get_semantic_model_refresh_status: {e}")
            raise    

    def log_semantic_model_refresh_activity(
            self,
            log_table_name: str,
            refresh_state: DataFrame,
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
        try:
             # Attempt to access the Delta table
            try:
                if write_method == 'catalog':
                    DeltaTable.forName(self.spark, log_table_name)
                elif write_method == 'path':
                    DeltaTable.forPath(self.spark, f'{log_table_storage_container_endpoint}/Tables/{log_table_name}')
                else:
                    raise ValueError(f'Invalid write method: {write_method}')
            except Exception as e:
                self.logger.info('Table does not exist, creating it.')

                # Construct log table path
                if write_method == 'catalog':
                    create_table = f'CREATE TABLE {log_table_name}'
                elif write_method == 'path':
                    control_table_path = f'{log_table_storage_container_endpoint}/Tables/{log_table_name}'
                    create_table = f'CREATE TABLE delta.`{control_table_path}`'
                else:
                    raise ValueError(f'Invalid write method: {write_method}')
                    
                # Create logging table
                self.spark.sql(f"""
                    {create_table} (
                        semantic_model_log_key INT,
                        workspace_id VARCHAR(50),
                        semantic_model_id VARCHAR(50),
                        refresh_id VARCHAR(50),
                        start_time TIMESTAMP,
                        end_time TIMESTAMP,
                        duration FLOAT,
                        status VARCHAR(50),
                        error VARCHAR(4000),
                        message VARCHAR(4000),
                        inserted_date_time TIMESTAMP,
                        updated_date_time TIMESTAMP
                    ) USING delta;
                """)
                self.logger.info(f"Log table {log_table_name} has been created.")
            
            # Set composite_columns
            composite_columns = ['workspace_id', 'semantic_model_id']

            # Primary key column
            primary_key_column = 'semantic_model_log_key'

            # Build table config
            table_config = [
                {
                    'table_name': log_table_name,
                    'dataframe': refresh_state,
                    'composite_columns': composite_columns,
                    'upsert_type': 'generic',
                    'primary_key_column': primary_key_column
                }
            ]

            # Upsert table
            self.upsert_manager.upsert_data_concurrently(table_config)
            
            self.logger.info("Refresh state logged successfully.")
        except Exception as e:
            self.logger.error(f"An unexpected error occurred in log_semantic_model_refresh_activity: {e}")
            raise