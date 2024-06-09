from notebookutils import mssparkutils
from typing import Optional
import logging

class UtilityManager:
    @staticmethod
    def flatten_properties(azure_object, logger=None):
        """
        Flattens properties of an Azure object to a dictionary for DataFrame compatibility.

        :param azure_object: The Azure object instance to be flattened. This should be an instance of a class from the `azure` package.
        :param logger: The logger to use. If not provided, a default logger will be used.
        :return: A dictionary containing the flattened properties of the Azure object.
        """
        logger = logger if logger else logging.getLogger(__name__)

        try:
            # Initialize an empty dictionary to store the flattened object
            flat_object = {}

            # Iterate over all attributes of the azure_object
            for attr in dir(azure_object):
                # Ignore private attributes and methods
                if not attr.startswith("_") and not callable(getattr(azure_object, attr)):
                    value = getattr(azure_object, attr)

                    # Only add hashable values to the dictionary
                    try:
                        hash(value)
                        flat_object[attr] = value
                    except TypeError:
                        continue

            return flat_object

        except AttributeError as e:
            # Log any attribute errors that occur during the flattening process
            logger.error(f"An unexpected error occurred in flatten_properties: {e}")
            raise

    @staticmethod
    def get_secret_value_as_user(
        key_vault_name, 
        secret_name, 
        logger=None
    ):
        """
        Retrieves a secret value from Azure Key Vault.

        :param key_vault_name: The name of the key vault.
        :param secret_name: The name of the secret within the key vault.
        :param logger: The logger to use. If not provided, a default logger will be used.
        :return: The value of the secret retrieved from Azure Key Vault.

        Example:
        key_vault_name = "my-key-vault
        secret_name = "my-secret"
        secret_value = UtilityManager.get_secret_value_as_user(key_vault_name, secret_name)
        """
        
        logger = logger if logger else logging.getLogger(__name__)

        # Construct the vault URL using the key_vault_name
        key_vault_url = f"https://{key_vault_name}.vault.azure.net/"

        try:
            # Retrieve the secret from the key vault
            key_vault_secret = mssparkutils.credentials.getSecret(key_vault_url, secret_name)
        except Exception as e:
            # Log any errors that occur during the secret retrieval process
            logger.error(f"Failed to retrieve secret from Key Vault: {e}")
            raise

        return key_vault_secret
    
    @staticmethod
    def get_secret_value_as_managed_identity(
        key_vault_name: str, 
        secret_name: str, 
        managed_identity_name: str, 
        logger: Optional[logging.Logger] = None
    ) -> str:
        """
        Retrieves a secret value from Azure Key Vault.

        :param key_vault_name: The name of the key vault.
        :param secret_name: The name of the secret within the key vault.
        :param managed_identity_name: The name of the managed identity resource to use for secret retrieval.
        :param logger: The logger to use. If not provided, a default logger will be used.
        :return: The value of the secret retrieved from Azure Key Vault.

        Example:
        key_vault_name = "my-key-vault
        secret_name = "my-secret"
        managed_identity_name = "my-managed-identity"
        secret_value = UtilityManager.get_secret_value_as_managed_identity(key_vault_name, secret_name, managed_identity_name)
        """
        logger = logger if logger else logging.getLogger(__name__)

        # Construct the vault URL using the key_vault_name
        key_vault_url = f"https://{key_vault_name}.vault.azure.net/"

        try:
            # Retrieve the secret from the key vault
            key_vault_secret = mssparkutils.credentials.getSecret(key_vault_url, secret_name, managed_identity_name)
        except Exception as e:
            # Log any errors that occur during the secret retrieval process
            logger.error(f"Failed to retrieve secret from Key Vault: {e}")
            raise

        return key_vault_secret