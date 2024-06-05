import logging
from typing import List, Dict, Optional, Tuple, Callable
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from .azure_resource_manager import AzureResourceManager

class Control:
    """
    Provides data engineering and orchestration capabilities for the Lucid Control Framework.

    :param spark: The SparkSession object.
    """

    def __init__(self, tenant_id=None, client_id=None, client_secret=None):
        """
        Initializes the Control class.

        :param spark: The SparkSession object.
        """

        # Initialize the SparkSession object
        self.spark = SparkSession.builder.getOrCreate()
        
        # Set up logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

        # Initialize the Lucid Support Modules
        if tenant_id and client_id and client_secret:
            self.arm_ops = AzureResourceManager(tenant_id, client_id, client_secret)
        else:
            self.arm_ops = None

    def get_subscriptions_dataframe(self) -> DataFrame:
        """
        Retrieves a DataFrame containing information about the Azure subscriptions available to the user.

        :return: A DataFrame containing information about the Azure subscriptions available to the user.
        """
        self.logger.info("Retrieving Azure subscriptions")
        return self.arm_ops.get_subscriptions_dataframe()

    def get_secret_value_as_user(self, key_vault_name: str, secret_name: str) -> str:
        """
        Retrieves a secret value from Azure Key Vault.

        :param key_vault_name: The name of the key vault.
        :param secret_name: The name of the secret within the key vault.
        :return: The value of the secret retrieved from Azure Key Vault.
        """
        self.logger.info(f"Getting secret value for {key_vault_name}/{secret_name}")
        return self.utility_manager.get_secret_value_as_user(key_vault_name, secret_name)

    