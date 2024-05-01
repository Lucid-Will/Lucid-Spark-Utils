import logging
from typing import List, Dict, Optional
from .utility_manager import UtilityManager
from .upsert_strategy.upsert_handler import UpsertHandler

class LucidUtils:
    def __init__(self):
        # Set up logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

        self.utility_manager = UtilityManager()
        self.upsert_handler = UpsertHandler(self.logger)

    def get_secret_value(self, key_vault_name: str, secret_name: str) -> str:
        """
        Retrieves a secret value from Azure Key Vault.

        :param key_vault_name: The name of the key vault.
        :param secret_name: The name of the secret within the key vault.
        :return: The value of the secret retrieved from Azure Key Vault.
        """
        self.logger.info(f"Getting secret value for {key_vault_name}/{secret_name}")
        return self.utility_manager.get_secret_value(key_vault_name, secret_name)

    def upsert_data_concurrently(self, table_configs: List[Dict], storage_container_endpoint: Optional[str] = None, write_method: str = 'default') -> None:
        """
        Performs upsert operations concurrently on multiple tables based on the provided configurations.

        :param table_configs: A list of table configurations.
        :param storage_container_endpoint: The storage container endpoint (optional).
        :param write_method: The write method (default is 'default').
        """
        self.logger.info(f"Upserting data concurrently")
        return self.upsert_handler.upsert_data_concurrently(table_configs, storage_container_endpoint, write_method)