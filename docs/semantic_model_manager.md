## Semantic Model Manager Documentation

### Overview

The `SemanticModelManager` class provides methods for managing Power BI semantic models. It includes methods for loading semantic model configurations, retrieving access tokens, triggering model refreshes, checking refresh statuses, and logging refresh activities.

### Requirements

To leverage this module, the following prerequisites must be met:

1. **Service Principal Creation**:
    - A service principal must be created in Azure Active Directory.

2. **Azure Key Vault Integration**:
    - The service principal's client ID and client secret must be stored in Azure Key Vault.
    - Ensure that the Key Vault has appropriate policies set to allow access to these secrets.

3. **Power BI Workspace Integration**:
    - The service principal must be added to the Power BI workspace with appropriate permissions (at least Contributor role).

### Module Structure

#### Classes

1. **SemanticModelManager**

---

### Class: SemanticModelManager

#### Description

This class is used for managing Power BI semantic models. It includes methods for loading configurations into Delta tables, retrieving service principal tokens, triggering model refreshes, checking refresh statuses, and logging refresh activities.

#### Methods

- `__init__(self, spark, logger=None)`
    - Initialize the SemanticModelManager object.
    - **Parameters:**
        - `spark`: The SparkSession object.
        - `logger`: The logger object. If not provided, a default logger will be used.

- `load_semantic_model_config(self, control_table_name: str, semantic_model_config: list, write_method: str = 'catalog', control_storage_container_endpoint: Optional[str] = None)`
    - Load the semantic model configuration into a DataFrame and save it as a Delta table.
    - **Parameters:**
        - `control_table_name`: The name of the control table.
        - `semantic_model_config`: A list of dictionaries representing the semantic model configuration.
        - `write_method`: The method to use for writing the table. Can be either 'path' or 'catalog'.
        - `control_storage_container_endpoint`: The endpoint of the storage container where the semantic model configuration is stored. Required if write_method is 'path'.
    - **Raises:**
        - `Exception`: If there's a problem loading the semantic model configuration.
        - `ValueError`: If an invalid write method is provided.
    - **Example:**
        ```python
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
        semantic_model_manager.load_semantic_model_config('control_table', semantic_model_config)
        ```

- `get_service_principal_pbi_scope_token(self, tenant_id: str, key_vault_name: str, client_id: str, client_secret: str, secret_retrieval_method: str = 'user', managed_identity: Optional[str] = None) -> str`
    - Retrieves an access token for a service principal using the Microsoft Authentication Library (MSAL).
    - **Parameters:**
        - `tenant_id`: The Azure Active Directory tenant GUID.
        - `key_vault_name`: The name of the Azure Key Vault containing the client ID and client secret.
        - `client_id`: The name of the secret containing the client ID in Azure Key Vault.
        - `client_secret`: The name of the secret containing the client secret in Azure Key Vault.
        - `secret_retrieval_method`: The method to use for secret retrieval. Can be either 'user' or 'managed_identity'. Default is 'user'.
        - `managed_identity`: The name of the linked service to use for secret retrieval if using 'managed_identity' method.
    - **Returns:** The access token for the service principal.
    - **Raises:**
        - `NameError`: If `managed_identity` is not provided when `secret_retrieval_method` is 'managed_identity'.
        - `Exception`: If there's a problem retrieving the access token.
    - **Example:**
        ```python
        token = semantic_model_manager.get_service_principal_pbi_scope_token(
            tenant_id="my-tenant-id",
            key_vault_name="my-key-vault",
            client_id="my-client-id",
            client_secret="my-client-secret",
            secret_retrieval_method="user"
        )
        ```

- `trigger_semantic_model_refresh(self, workspace_id: str, semantic_model_id: str, refresh_token: str) -> None`
    - Triggers a refresh of a Power BI dataset.
    - **Parameters:**
        - `workspace_id`: The ID of the Power BI workspace containing the dataset.
        - `semantic_model_id`: The ID of the dataset to refresh.
        - `refresh_token`: The refresh token for authentication.
    - **Returns:** None. The function logs the success or failure of the refresh operation.
    - **Raises:** `Exception`: If there's a problem triggering the dataset refresh.
    - **Example:**
        ```python
        semantic_model_manager.trigger_semantic_model_refresh("my-workspace-id", "my-dataset-id", "my-refresh-token")
        ```

- `get_semantic_model_refresh_status(self, workspace_id: str, semantic_model_id: str, refresh_token: str) -> DataFrame`
    - Retrieves the refresh status of a Power BI dataset.
    - **Parameters:**
        - `workspace_id`: The ID of the Power BI workspace containing the dataset.
        - `semantic_model_id`: The ID of the dataset to refresh.
        - `refresh_token`: The refresh token for authentication.
    - **Returns:** The refresh status of the dataset as a PySpark DataFrame.
    - **Raises:** `Exception`: If there's a problem retrieving the refresh status.
    - **Example:**
        ```python
        df_refresh_status = semantic_model_manager.get_semantic_model_refresh_status("my-workspace-id", "my-dataset-id", "my-refresh-token")
        ```

- `log_semantic_model_refresh_activity(self, log_table_name: str, refresh_state: DataFrame, write_method: str = 'catalog', log_table_storage_container_endpoint: Optional[str] = None) -> None`
    - Logs the refresh activity of a Power BI dataset.
    - **Parameters:**
        - `log_table_name`: The name of the log table in the Azure Storage container.
        - `refresh_state`: The refresh state of the dataset as a DataFrame.
        - `write_method`: The method to use for writing the log table. Can be either 'path' or 'catalog'. Default is 'catalog'.
        - `log_table_storage_container_endpoint`: The endpoint of the Azure Storage container where the log table is stored. Required if write_method is 'path'.
    - **Raises:**
        - `Exception`: If there's a problem logging the refresh activity.
        - `ValueError`: If an invalid write method is provided.
    - **Example:**
        ```python
        semantic_model_manager.log_semantic_model_refresh_activity("log_table", df_refresh_state, "catalog", "log_table_storage_container_endpoint")
        ```
