from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import DataFrame
from typing import List
import logging
import os

class FileManager:
    def __init__(self, spark, logger=None):
        """
        Initialize FileManager with a Spark session.

        :param spark: The Spark session.
        """
        self.logger = logger if logger else logging.getLogger(__name__)
        self.spark = spark

    def read_file(self, file_path: str, file_format: str = 'csv') -> DataFrame:
        """
        Reads a file from the storage account.

        :param file_path: The path of the file.
        :param file_format: The format of the file. Can be 'csv', 'json', 'parquet', etc.
        :return: The DataFrame read from the file.

        Raises:
            ValueError: If an invalid file format is provided.
            Exception: If any error occurs during file reading.
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

    def read_files_concurrently(self, file_paths: list, file_format: str = 'csv') -> list:
        """
        Reads multiple files from the storage account concurrently.

        :param file_paths: A list of file paths.
        :param file_format: The format of the files. Can be 'csv', 'json', 'parquet', etc.
        :return: A list of DataFrames read from the files.

        Raises:
            Exception: If any error occurs during file reading.
        """
        # Function to be executed in parallel
        def read_file(file_path):
            return self.read_file(file_path, file_format=file_format)

        # Set thread pool size based on the number of tables and available CPUs
        max_workers = min(len(file_paths), (os.cpu_count() or 1) * 5)
        
        # Use ThreadPoolExecutor to read files concurrently
        with ThreadPoolExecutor(max_workers) as executor:
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

    def write_file(self, dataframe: DataFrame, file_name: str, storage_container_endpoint: str, storage_container_name: str, file_format: str = 'parquet'):
        """
        Writes a DataFrame to a file in the storage account.

        :param dataframe: The DataFrame to write.
        :param file_name: The name of the file to write.
        :param storage_container_endpoint: The endpoint of the storage container.
        :param storage_container_name: The name of the storage container.
        :param file_format: The format of the file. Can be 'csv', 'json', 'parquet', etc.

        Raises:
            Exception: If any error occurs during file writing.
        """
        try:
            # Write the DataFrame to the specified file format
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

    def write_files_concurrently(self, dataframe_list: List[DataFrame], file_names: List[str], storage_container_endpoint: str, storage_container_name: str, file_format: str = 'parquet'):
        """
        Writes multiple DataFrames to files in the storage account concurrently.

        :param dataframe_list: A list of DataFrames to write.
        :param file_names: A list of file names.
        :param storage_container_endpoint: The storage container endpoint.
        :param storage_container_name: The storage container name.
        :param file_format: The format of the files. Can be 'csv', 'json', 'parquet', etc.

        :return: None. The function writes the DataFrames to files.

        Raises:
            ValueError: If the lengths of dataframe_list and file_names do not match.
            Exception: If any error occurs during file writing.

        Example:
            dataframe_list = [spark.createDataFrame([(1, "John", "Doe"), (2, "Jane", "Doe")], ["ID", "First Name", "Last Name"]),
                            spark.createDataFrame([(3, "Jim", "Smith"), (4, "Jill", "Smith")], ["ID", "First Name", "Last Name"])]
            file_names = ['file1.parquet', 'file2.parquet']
            storage_container_endpoint = 'https://storageaccount.blob.core.windows.net'
            storage_container_name = 'container'
            file_format = 'parquet'
            write_files_concurrently(dataframe_list, file_names, storage_container_endpoint, storage_container_name, file_format)
        """
        # Function to be executed in parallel
        def write_file(dataframe, file_name):
            self.write_file(dataframe, file_name, storage_container_endpoint, storage_container_name, file_format=file_format)

        # Set thread pool size based on the number of tables and available CPUs
        max_workers = min(len(dataframe_list), (os.cpu_count() or 1) * 5)
        
        # Use ThreadPoolExecutor to write files concurrently
        with ThreadPoolExecutor(max_workers) as executor:
            # Submit tasks to the executor
            future_to_file = {executor.submit(write_file, dataframe, file_name): file_name for dataframe, file_name in zip(dataframe_list, file_names)}

            # Process results as they complete
            for future in as_completed(future_to_file):
                try:
                    # Get result from Future
                    future.result()
                except Exception as e:
                    self.logger.error(f"An error occurred while writing file {future_to_file[future]}: {e}")