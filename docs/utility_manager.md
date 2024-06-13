## UtilityManager Documentation

### Overview

The `UtilityManager` class provides static methods for handling Azure objects and secrets. It includes methods for flattening Azure object properties, retrieving secret values from Azure Key Vault as a user, and retrieving secret values from Azure Key Vault using a managed identity.

### Module Structure

#### Classes

1. **UtilityManager**

---

### Class: UtilityManager

#### Description

This class is used for handling Azure objects and secrets. It includes methods for flattening Azure object properties, retrieving secret values from Azure Key Vault as a user, and retrieving secret values from Azure Key Vault using a managed identity.

#### Methods

- `flatten_properties(azure_object, logger=None)`
    - Flattens properties of an Azure object to a dictionary for DataFrame compatibility.
    - **Parameters:**
        - `azure_object`: The Azure object instance to be flattened. This should be an instance of a class from the `azure` package.
        - `logger`: The logger to use. If not provided, a default logger will be used.
    - **Returns:** A dictionary containing the flattened properties of the Azure object.
    - **Raises:** 
        - `AttributeError`: If there's a problem accessing the attributes of the Azure object.
    - **Example:**
        ```python
        azure_object = AzureBlobStorageObject(...)
        logger = logging.getLogger(__name__)
        flat_object = UtilityManager.flatten_properties(azure_object, logger)
        ```

- `get_secret_value_as_user(key_vault_name, secret_name, logger=None)`
    - Retrieves a secret value from Azure Key Vault as a user.
    - **Parameters:**
        - `key_vault_name`: The name of the key vault.
        - `secret_name`: The name of the secret within the key vault.
        - `logger`: The logger to use. If not provided, a default logger will be used.
    - **Returns:** The value of the secret retrieved from Azure Key Vault.
    - **Raises:** 
        - `Exception`: If there's a problem retrieving the secret from Azure Key Vault.
    - **Example:**
        ```python
        key_vault_name = "my-key-vault"
        secret_name = "my-secret"
        secret_value = UtilityManager.get_secret_value_as_user(key_vault_name, secret_name)
        ```

- `get_secret_value_as_managed_identity(key_vault_name: str, secret_name: str, managed_identity_name: str, logger: Optional[logging.Logger] = None) -> str`
    - Retrieves a secret value from Azure Key Vault using a managed identity.
    - **Parameters:**
        - `key_vault_name`: The name of the key vault.
        - `secret_name`: The name of the secret within the key vault.
        - `managed_identity_name`: The name of the managed identity resource to use for secret retrieval.
        - `logger`: The logger to use. If not provided, a default logger will be used.
    - **Returns:** The value of the secret retrieved from Azure Key Vault.
    - **Raises:** 
        - `Exception`: If there's a problem retrieving the secret from Azure Key Vault.
    - **Example:**
        ```python
        key_vault_name = "my-key-vault"
        secret_name = "my-secret"
        managed_identity_name = "my-managed-identity"
        secret_value = UtilityManager.get_secret_value_as_managed_identity(key_vault_name, secret_name, managed_identity_name)
        ```