import logging
from notebookutils import mssparkutils

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
    def get_secret_value(key_vault_name, secret_name, linked_service=None, logger=None):
        """
        Retrieves a secret value from Azure Key Vault.

        :param key_vault_name: The name of the key vault.
        :param secret_name: The name of the secret within the key vault.
        :param linked_service: The name of the linked service to use for secret retrieval.
        :param logger: The logger to use. If not provided, a default logger will be used.
        :return: The value of the secret retrieved from Azure Key Vault.
        """
        
        logger = logger if logger else logging.getLogger(__name__)

        try:
            # Retrieve the secret from the key vault
            key_vault_secret = mssparkutils.credentials.getSecret(key_vault_name, secret_name, linked_service)
        except mssparkutils.credentials.SecretRetrievalError as e:
            # Log any errors that occur during the secret retrieval process
            logger.error(f"Failed to retrieve secret from Key Vault: {e}")
            raise

        return key_vault_secret