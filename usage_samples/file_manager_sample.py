#!/usr/bin/env python
# coding: utf-8

# ## File Manager
# 
# New notebook

# ## File Manager Documentation
# 
# ### Overview
# 
# The `FileManager` class provides methods for reading and writing files in various formats using Spark. It supports concurrent reading and writing of multiple files and offers configurable options for different file formats.
# 
# ### Module Structure
# 
# #### Classes
# 
# 1. **FileManager**
# 
# ---
# 
# ### Class: FileManager
# 
# #### Description
# 
# This class is used for managing files in a storage account using Spark. It includes methods for reading and writing files, as well as for concurrent operations on multiple files.
# 
# #### Methods
# 
# - `__init__(self, spark, logger=None)`
#     - Initialize the FileManager object.
#     - **Parameters:**
#         - `spark`: The SparkSession object.
#         - `logger`: The logger object. If not provided, a default logger will be used.
# 
# - `read_file(self, file_path: str, file_format: str = 'csv') -> DataFrame`
#     - Reads a file from the storage account.
#     - **Parameters:**
#         - `file_path`: The path of the file.
#         - `file_format`: The format of the file. Can be 'csv', 'json', 'parquet', etc.
#     - **Returns:** The DataFrame read from the file.
#     - **Raises:** 
#         - `ValueError`: If an invalid file format is provided.
#         - `Exception`: If any error occurs during file reading.
#     - **Example:**
#         ```python
#         file_path = 'abfss://workspaceid.dfs.core.windows.net/data/Files/file1.csv'
#         file_format = 'csv'
#         df = utils.read_file(file_path, file_format)
#         ```
# 
# - `read_files_concurrently(self, file_paths: List[str], file_format: str = 'csv') -> List[DataFrame]`
#     - Reads multiple files from the storage account concurrently.
#     - **Parameters:**
#         - `file_paths`: A list of file paths.
#         - `file_format`: The format of the files. Can be 'csv', 'json', 'parquet', etc.
#     - **Returns:** A list of DataFrames read from the files.
#     - **Raises:** 
#         - `Exception`: If any error occurs during file reading.
#     - **Example:**
#         ```python
#         file_paths = ['abfss://workspaceid.dfs.core.windows.net/data/Files/file1.csv', 'abfss://workspaceid.dfs.core.windows.net/data/Files/file2.csv']
#         file_format = 'csv'
#         dataframes = utils.read_files_concurrently(file_paths, file_format)
#         ```
# 
# - `write_file(self, dataframe: DataFrame, file_name: str, storage_container_endpoint: str, file_format: str = 'parquet') -> None`
#     - Writes a DataFrame to a file in the storage account.
#     - **Parameters:**
#         - `dataframe`: The DataFrame to write.
#         - `file_name`: The name of the file to write.
#         - `storage_container_endpoint`: The endpoint of the storage container.
#         - `file_format`: The format of the file. Can be 'csv', 'json', 'parquet', etc.
#     - **Raises:** 
#         - `Exception`: If any error occurs during file writing.
#     - **Example:**
#         ```python
#         dataframe = df_data
#         file_name = 'file1.parquet'
#         storage_container_endpoint = 'abfss://workspaceid.dfs.core.windows.net/data'
#         file_format = 'parquet'
#         utils.write_file(dataframe, file_name, storage_container_endpoint, file_format)
#         ```
# 
# - `write_files_concurrently(self, dataframe_list: List[DataFrame], file_names: List[str], storage_container_endpoint: str, file_format: str = 'parquet') -> None`
#     - Writes multiple DataFrames to files in the storage account concurrently.
#     - **Parameters:**
#         - `dataframe_list`: A list of DataFrames to write.
#         - `file_names`: A list of file names.
#         - `storage_container_endpoint`: The storage container endpoint.
#         - `file_format`: The format of the files. Can be 'csv', 'json', 'parquet', etc.
#     - **Returns:** None. The function writes the DataFrames to files.
#     - **Raises:** 
#         - `ValueError`: If the lengths of `dataframe_list` and `file_names` do not match.
#         - `Exception`: If any error occurs during file writing.
#     - **Example:**
#         ```python
#         dataframe_list = [
#             spark.createDataFrame([(1, "John", "Doe"), (2, "Jane", "Doe")], ["ID", "First Name", "Last Name"]),
#             spark.createDataFrame([(3, "Jim", "Smith"), (4, "Jill", "Smith")], ["ID", "First Name", "Last Name"])
#         ]
#         file_names = ['file1.parquet', 'file2.parquet']
#         storage_container_endpoint = 'abfss://workspaceid.dfs.core.windows.net/data'
#         file_format = 'parquet'
#         utils.write_files_concurrently(dataframe_list, file_names, storage_container_endpoint, file_format)
#         ```
# 

# In[ ]:


# Install and initialize lucid-spark-utils
get_ipython().system('pip install "https://raw.githubusercontent.com/Lucid-Will/Lucid-Spark-Utils/main/dist/lucidsparkutils-1.0-py3-none-any.whl" --quiet 2>/dev/null')

import lucid_spark_utils as lucid
utils = lucid.LucidUtils()


# In[ ]:


# Set variables
file_path = 'abfss://workspaceid.dfs.core.windows.net/data/Files/file1.csv'
file_format = 'csv'

# Read file to dataframe
df = utils.read_file(file_path, file_format)


# In[ ]:


# Set variables
file_paths = ['abfss://workspaceid.dfs.core.windows.net/data/Files/file1.csv', 'abfss://workspaceid.dfs.core.windows.net/data/Files/file2.csv']
file_format = 'csv'

# Read file to dataframe
dataframes = utils.read_files_concurrently(file_paths, file_format)


# In[ ]:


# Set variables
dataframe = df_data
file_name = 'file1.parquet'
storage_container_endpoint = 'abfss://workspaceid.dfs.core.windows.net/data'
file_format = 'parquet'

# Write file from dataframe
utils.write_file(dataframe, file_name, storage_container_endpoint, file_format)


# In[ ]:


# Set variables
dataframe_list = [
    spark.createDataFrame([(1, "John", "Doe"), (2, "Jane", "Doe")], ["ID", "First Name", "Last Name"]),
    spark.createDataFrame([(3, "Jim", "Smith"), (4, "Jill", "Smith")], ["ID", "First Name", "Last Name"])
]
file_names = ['file1.parquet', 'file2.parquet']
storage_container_endpoint = 'abfss://workspaceid.dfs.core.windows.net/data'
file_format = 'parquet'

# Write file from dataframe
utils.write_files_concurrently(dataframe_list, file_names, storage_container_endpoint, file_format)

