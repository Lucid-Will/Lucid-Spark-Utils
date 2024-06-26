#!/usr/bin/env python
# coding: utf-8

# ## Utility Manager
# 
# New notebook

# ## UtilityManager Documentation
# 
# ### Overview
# 
# The `UtilityManager` class provides static methods for handling Azure objects and secrets. It includes methods for flattening Azure object properties, retrieving secret values from Azure Key Vault as a user, and retrieving secret values from Azure Key Vault using a managed identity.
# 
# ### Module Structure
# 
# #### Classes
# 
# 1. **UtilityManager**
# 
# ---
# 
# ### Class: UtilityManager
# 
# #### Description
# 
# This class is used for handling Azure objects and secrets. It includes methods for flattening Azure object properties, retrieving secret values from Azure Key Vault as a user, and retrieving secret values from Azure Key Vault using a managed identity.
# 
# #### Methods
# 
# - `get_secret_value_as_user(key_vault_name, secret_name, logger=None)`
#     - Retrieves a secret value from Azure Key Vault as a user.
#     - **Parameters:**
#         - `key_vault_name`: The name of the key vault.
#         - `secret_name`: The name of the secret within the key vault.
#         - `logger`: The logger to use. If not provided, a default logger will be used.
#     - **Returns:** The value of the secret retrieved from Azure Key Vault.
#     - **Raises:** 
#         - `Exception`: If there's a problem retrieving the secret from Azure Key Vault.
#     - **Example:**
#         ```python
#         key_vault_name = "my-key-vault"
#         secret_name = "my-secret"
#         secret_value = utils.get_secret_value_as_user(key_vault_name, secret_name)
#         ```
# 
# - `get_secret_value_as_managed_identity(key_vault_name: str, secret_name: str, managed_identity_name: str, logger: Optional[logging.Logger] = None) -> str`
#     - Retrieves a secret value from Azure Key Vault using a managed identity.
#     - **Parameters:**
#         - `key_vault_name`: The name of the key vault.
#         - `secret_name`: The name of the secret within the key vault.
#         - `managed_identity_name`: The name of the managed identity resource to use for secret retrieval.
#         - `logger`: The logger to use. If not provided, a default logger will be used.
#     - **Returns:** The value of the secret retrieved from Azure Key Vault.
#     - **Raises:** 
#         - `Exception`: If there's a problem retrieving the secret from Azure Key Vault.
#     - **Example:**
#         ```python
#         key_vault_name = "my-key-vault"
#         secret_name = "my-secret"
#         managed_identity_name = "my-managed-identity"
#         secret_value = utils.get_secret_value_as_managed_identity(key_vault_name, secret_name, managed_identity_name)
#         ```

# In[1]:


# Install and initialize lucid-spark-utils
get_ipython().system('pip install "https://raw.githubusercontent.com/Lucid-Will/Lucid-Spark-Utils/main/dist/lucidsparkutils-1.0-py3-none-any.whl" --quiet 2>/dev/null')

import lucid_control_framework as lucid
utils = lucid.LucidUtils()


# In[ ]:


# Set variables
key_vault_name = "my-key-vault"
secret_name = "my-secret"

# Get secret as user
secret_value = utils.get_secret_value_as_user(key_vault_name, secret_name)


# In[ ]:


# Set variables
key_vault_name = "my-key-vault"
secret_name = "my-secret"
managed_identity_name = "my-managed-identity"

# Get secret as managed identity
secret_value = utils.get_secret_value_as_managed_identity(key_vault_name, secret_name, managed_identity_name)

