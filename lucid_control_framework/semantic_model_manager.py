from lucid_control_framework.dataframe_transformation_manager import DataFrameTransformationManager
from .utility_manager import UtilityManager
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType
from pyspark.sql.functions import concat, abs, hash, col
from datetime import datetime
from pyspark.sql import Row
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
        self.transform_manager = DataFrameTransformationManager(self.spark)

    def get_service_principal_pbi_scope_token(self, tenant_id, key_vault_name, client_id, client_secret, linked_service):
        """
        Retrieves an access token for a service principal using the Microsoft Authentication Library (MSAL).

        :param tenant_id: The Azure Active Directory tenant GUID.
        :param key_vault_name: The name of the Azure Key Vault containing the client ID and client secret.
        :param client_id: The name of the secret containing the client ID in Azure Key Vault.
        :param client_secret: The name of the secret containing the client secret in Azure Key Vault.
        :param linked_service: The name of the linked service to use for secret retrieval.
        :return: The access token for the service principal.
        """
        try:
            # Retrieve the client ID and client secret from Azure Key Vault
            client_id = UtilityManager.get_secret_value(key_vault_name, client_id, linked_service)
            client_secret = UtilityManager.get_secret_value(key_vault_name, client_secret, linked_service)

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
    
    def trigger_semantic_model_refresh(self, workspace_id, semantic_model_id, refresh_token):
        """
        Triggers a refresh of a Power BI dataset.

        :param workspace_id: The ID of the Power BI workspace containing the dataset.
        :param semantic_model_id: The ID of the dataset to refresh.
        :param refresh_token: The refresh token for authentication.
        :return: True if the refresh was successful, False otherwise.
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
    
    def get_semantic_model_refresh_status(self, workspace_id, semantic_model_id, refresh_token):
        """
        Retrieves the refresh status of a Power BI dataset.

        :param workspace_id: The ID of the Power BI workspace containing the dataset.
        :param semantic_model_id: The ID of the dataset to refresh.
        :param refresh_token: The refresh token for authentication.
        :return: The refresh status of the dataset.
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
                    time.sleep(15)
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
                StructField("error", StringType(), True),
                StructField("message", StringType(), True)
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
                "error": None,
                "message": None
            }

            # Parse serviceExceptionJson for error details
            if "serviceExceptionJson" in refreshes[0]:
                service_exception_json = json.loads(refreshes[0]["serviceExceptionJson"])
                refresh_state["error"] = service_exception_json["error"]
                refresh_state["message"] = service_exception_json["message"]

            # Create a PySpark DataFrame with the defined schema
            df_refresh_state = self.spark.createDataFrame([Row(**refresh_state)], schema=schema)

            return df_refresh_state
        except Exception as e:
            self.logger.error(f"An unexpected error occurred in get_semantic_model_refresh_status: {e}")
            raise    

    def log_semantic_model_refresh_activity(self, refresh_state):
        """
        Logs the refresh activity of a Power BI dataset.

        :param refresh_state: The refresh state of the dataset.
        """
        try:
            # Set match_keys for the log DataFrame using workspace_id and semantic_model_id from the refresh_state dataframe
            match_keys = refresh_state.select(concat(col("workspace_id"), col("semantic_model_id")))

            # Set target table
            target_table = 'Control.semantic_model_refresh_log'
            
            # Add key to the log DataFrame using transform manager
            df = self.transform_manager.stage_dataframe_with_keys(target_table, refresh_state, refresh_state.columns, 'semantic_model_refresh_key', 'semantic_model_refresh_natural_key', match_keys)
            
            # Write the refresh state to delta table
            df.write.format("delta").mode("append").saveAsTable(target_table)
            self.logger.info("Refresh state logged successfully.")
        except Exception as e:
            self.logger.error(f"An unexpected error occurred in log_semantic_model_refresh_activity: {e}")
            raise