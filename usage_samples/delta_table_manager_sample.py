#!/usr/bin/env python
# coding: utf-8

# ## Delta Table Manager
# 
# New notebook

# ## Delta Table Manager Documentation
# 
# ### Overview
# 
# The `DeltaTableManager` class provides methods for reading and writing Delta tables in Spark. It supports concurrent reading of multiple tables and offers configurable options for different read and write methods.
# 
# ### Module Structure
# 
# #### Classes
# 
# 1. **DeltaTableManager**
# 
# ---
# 
# ### Class: DeltaTableManager
# 
# #### Description
# 
# This class is used for managing Delta tables in Spark. It includes methods for reading and writing Delta tables, as well as for concurrent reading of multiple tables.
# 
# #### Methods
# 
# - `__init__(self, spark, logger=None)`
#     - Initialize the DeltaTableManager object.
#     - **Parameters:**
#         - `spark`: The SparkSession object.
#         - `logger`: The logger object. If not provided, a default logger will be used.
# 
# - `read_delta_table(self, table_name: str, storage_container_endpoint: Optional[str] = None, read_method: str = 'catalog') -> DataFrame`
#     - Reads a Delta table into a DataFrame.
#     - **Parameters:**
#         - `table_name`: The name of the Delta table.
#         - `storage_container_endpoint`: The storage container endpoint. Required if read_method is 'path'.
#         - `read_method`: The method to use for reading the table. Can be either 'path' or 'catalog'.
#     - **Returns:** The DataFrame representing the Delta table.
#     - **Raises:** 
#         - `Exception`: If there's a problem reading the Delta table.
#         - `ValueError`: If an invalid read_method is provided.
#     - **Example:**
#         ```python
#         table_name = 'my_table'
#         workspace_id = 'my_workspace_id'
#         lakehouse_id = 'my_lakehouse_id'
#         storage_container_endpoint = f'abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}'
#         storage_container_endpoint = 'abfss://workspaceid.dfs.core.windows.net/data'
#         read_method = 'path'
#         df = utils.read_delta_table(table_name, storage_container_endpoint, read_method)
#         ```
# 
# - `read_delta_tables_concurrently(self, table_names: List[str], storage_container_endpoint: Optional[str] = None, read_method: str = 'catalog') -> Dict[str, DataFrame]`
#     - Reads multiple Delta tables into DataFrames concurrently.
#     - **Parameters:**
#         - `table_names`: A list of Delta table names.
#         - `storage_container_endpoint`: The storage container endpoint. Required if read_method is 'path'.
#         - `read_method`: The method to use for reading the table. Can be either 'path' or 'catalog'.
#     - **Returns:** A dictionary mapping table names to DataFrames.
#     - **Raises:** 
#         - `Exception`: If there's a problem reading the Delta tables.
#     - **Example:**
#         ```python
#         table_names = ['table1', 'table2']
#         storage_container_endpoint = 'abfss://workspaceid@onelake.dfs.fabric.microsoft.com/lakehouseid'
#         storage_container_endpoint = 'abfss://workspaceid.dfs.core.windows.net/data'
#         read_method = 'path'
#         df_tables = utils.read_delta_tables_concurrently(table_names, storage_container_endpoint, read_method)
#         ```
# 
# - `write_delta_table(self, dataframe: DataFrame, table_name: str, storage_container_endpoint: Optional[str] = None, write_method: str = 'catalog', write_mode: str = 'overwrite', merge_schema: str = 'true') -> None`
#     - Writes a DataFrame to a Delta table.
#     - **Parameters:**
#         - `dataframe`: The DataFrame to write.
#         - `table_name`: The name of the Delta table.
#         - `storage_container_endpoint`: The storage container endpoint. Required if write_method is 'path'.
#         - `write_method`: The method to use for writing the table. Can be either 'path' or 'catalog'.
#         - `write_mode`: The mode to use for writing the table. Can be 'overwrite', 'append', 'ignore', 'error', or 'overwritePartitions'. Default is 'overwrite'.
#         - `merge_schema`: Whether to merge the schema of the DataFrame with the schema of the Delta table. Default is 'true'.
#     - **Raises:** 
#         - `Exception`: If there's a problem writing the Delta table.
#         - `ValueError`: If an invalid write_method is provided.
#     - **Example:**
#         ```python
#         dataframe = df_data
#         table_name = 'my_table'
#         storage_container_endpoint = 'abfss://workspaceid@onelake.dfs.fabric.microsoft.com/lakehouseid'
#         write_method = 'path'
#         write_mode = 'overwrite'
#         merge_schema = 'true'
#         utils.write_delta_table(dataframe, table_name, storage_container_endpoint, write_method)
#         ```

# In[1]:


# Install and initialize lucid-spark-utils
get_ipython().system('pip install "https://raw.githubusercontent.com/Lucid-Will/Lucid-Spark-Utils/main/dist/lucidsparkutils-1.0-py3-none-any.whl" --quiet 2>/dev/null')

import lucid_control_framework as lucid
utils = lucid.LucidUtils()


# In[ ]:


# Set variables
table_name = 'my_table'
workspace_id = 'my_workspace_id'
lakehouse_id = 'my_lakehouse_id'
storage_container_endpoint = f'abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}'
read_method = 'default'

# Read delta table to dataframe
df = utils.read_delta_table(table_name, storage_container_endpoint, read_method)


# In[ ]:


# Set variables
table_names = ['table1', 'table2']
storage_container_endpoint = 'abfss://workspaceid@onelake.dfs.fabric.microsoft.com/lakehouseid'
storage_container_endpoint = 'abfss://workspaceid.dfs.core.windows.net/data'
read_method = 'path'

# Read delta table to dataframe
df_tables = utils.read_delta_tables_concurrently(table_names, storage_container_endpoint, read_method)


# In[ ]:


# Set variables
dataframe = df_data
table_name = 'my_table'
storage_container_endpoint = 'abfss://workspaceid@onelake.dfs.fabric.microsoft.com/lakehouseid'
write_method = 'path'
write_mode = 'overwrite'
merge_schema = 'true'

# Write delta table from dataframe
utils.write_delta_table(dataframe, table_name, storage_container_endpoint, write_method)

