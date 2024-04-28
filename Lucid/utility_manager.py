#!/usr/bin/env python
# coding: utf-8

# ## UtilityManager
# 
# New notebook

# In[ ]:


import logging


# In[ ]:


class UtilityManager:
    # Initialize a logger for this module
    logger = logging.getLogger(__name__)

    @staticmethod
    def flatten_properties(azure_object):
        """
        Flattens properties of an Azure object to a dictionary for DataFrame compatibility.
        Args:
            azure_object (object): The Azure object instance to be flattened. This should be an instance of a class from the `azure` package.
        Returns:
            dict: A dictionary containing the flattened properties of the Azure object.
        """
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
            UtilityManager.logger.error(f"An unexpected error occurred in flatten_properties: {e}")
            raise

    @staticmethod
    def get_secret_value(key_vault_name, secret_name):
        """
        Retrieves a secret value from Azure Key Vault.
        Args:
            key_vault_name (str): The name of the key vault.
            secret_name (str): The name of the secret within the key vault.
        Returns:
            str: The value of the secret retrieved from Azure Key Vault.
        """
        
        # Construct the vault URL using the key_vault_name
        key_vault_url = f"https://{key_vault_name}.vault.azure.net/"

        try:
            # Retrieve the secret from the key vault
            key_vault_secret = mssparkutils.credentials.getSecret(key_vault_url, secret_name)
        except mssparkutils.credentials.SecretRetrievalError as e:
            # Log any errors that occur during the secret retrieval process
            UtilityManager.logger.error(f"Failed to retrieve secret from Key Vault: {e}")
            raise

        return key_vault_secret

