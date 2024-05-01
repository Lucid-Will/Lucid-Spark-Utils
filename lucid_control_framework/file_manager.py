#!/usr/bin/env python
# coding: utf-8

# ## Lucid_FileManager
# 
# 
# 

# In[ ]:


import logging
from concurrent.futures import ThreadPoolExecutor, as_completed


# In[ ]:


class FileManager:
    def __init__(self, spark):
        self.logger = logging.getLogger(__name__)
        self.spark = spark

    def read_file(self, file_path, file_format='csv'):
        """
        Reads a file from the storage account.

        Args:
            file_path (str): The path of the file.
            file_format (str): The format of the file. Can be 'csv', 'json', 'parquet', etc.
        Returns:
            DataFrame: The DataFrame read from the file.
        """
        try:
            if file_format.lower() == 'csv':
                return self.spark.read.csv(file_path, header=True, inferSchema=True)
            elif file_format.lower() == 'json':
                return self.spark.read.json(file_path)
            elif file_format.lower() == 'parquet':
                return self.spark.read.parquet(file_path)
            else:
                raise ValueError(f"Invalid file format: {file_format}")
        except Exception as e:
            self.logger.error(f"An error occurred in read_file: {e}")
            raise

    def read_files_concurrently(self, file_paths, file_format='csv'):
        """
        Reads multiple files from the storage account concurrently.

        Args:
            file_paths (list): A list of file paths.
            file_format (str): The format of the files. Can be 'csv', 'json', 'parquet', etc.
        Returns:
            list: A list of DataFrames read from the files.
        """
        # Function to be executed in parallel
        def read_file(file_path):
            return self.read_file(file_path, file_format=file_format)

        # Use ThreadPoolExecutor to read files concurrently
        with ThreadPoolExecutor(max_workers=min(len(file_paths), 4)) as executor:
            # Submit tasks to the executor
            future_to_file = {executor.submit(read_file, file_path): file_path for file_path in file_paths}

            # Process results as they complete
            dataframes = []
            for future in as_completed(future_to_file):
                try:
                    # Get result from Future
                    dataframes.append(future.result())
                except Exception as e:
                    self.logger.error(f"An error occurred while reading file {future_to_file[future]}: {e}")
            return dataframes
    
    def write_file(self, dataframe, file_name, storage_container_endpoint, storage_container_name, file_format='parquet'):
        """
        Writes a DataFrame to a file in the storage account.

        Args:
            df (DataFrame): The DataFrame to write.
            file_name (str): The name of the file.
            storage_container_endpoint (str): The storage container endpoint.
            file_format (str): The format of the file. Can be 'csv', 'json', 'parquet', etc.
        """

        try:
            if file_format.lower() == 'csv':
                dataframe.write.csv(f"{storage_container_endpoint}/{storage_container_name}/{file_name}")
            elif file_format.lower() == 'json':
                dataframe.write.json(f"{storage_container_endpoint}/{file_name}")
            elif file_format.lower() == 'parquet':
                dataframe.write.parquet(f"{storage_container_endpoint}/{file_name}")
            else:
                raise ValueError(f"Invalid file format: {file_format}")
        except Exception as e:
            self.logger.error(f"An error occurred in write_file_to_storage: {e}")
            raise

    def write_files_concurrently(self, dataframe_list, file_names, storage_container_endpoint, storage_container_name, file_format='parquet'):
        """
        Writes multiple DataFrames to files in the storage account concurrently.

        Args:
            dfs (list): A list of DataFrames to write.
            file_names (list): A list of file names.
            storage_container_endpoint (str): The storage container endpoint.
            storage_container_name (str): The storage container name.
            file_format (str): The format of the files. Can be 'csv', 'json', 'parquet', etc.
        """

        # Function to be executed in parallel
        def write_file(dataframe, file_name):
            self.write_file(dataframe, file_name, storage_container_endpoint, storage_container_name, file_format=file_format)

        # Use ThreadPoolExecutor to write files concurrently
        with ThreadPoolExecutor(max_workers=min(len(dataframe_list), 4)) as executor:
            # Submit tasks to the executor
            future_to_file = {executor.submit(write_file, dataframe, file_name): file_name for dataframe, file_name in zip(dataframe_list, file_names)}

            # Process results as they complete
            for future in as_completed(future_to_file):
                try:
                    # Get result from Future
                    future.result()
                except Exception as e:
                    self.logger.error(f"An error occurred while writing file {future_to_file[future]}: {e}")

