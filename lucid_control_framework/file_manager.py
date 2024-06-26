from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import DataFrame
from typing import List
from pyspark.sql.types import TimestampType
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

    def read_file(
        self, 
        file_path: str, 
        file_format: str = 'csv'
    ) -> DataFrame:
        """
        Reads a file from the storage account.

        :param file_path: The path of the file.
        :param file_format: The format of the file. Can be 'csv', 'json', 'parquet', etc.

        :return: The DataFrame read from the file.

        Raises:
            ValueError: If an invalid file format is provided.
            Exception: If any error occurs during file reading.

        This function performs the following steps:
        1. Checks the file format and reads the file accordingly:
            - For CSV format, reads the file with headers and infers the schema.
            - For JSON format, reads the file as a JSON file.
            - For Parquet format, reads the file as a Parquet file.
        2. Raises a ValueError if an invalid file format is provided.
        3. Logs and raises any exceptions that occur during file reading.

        Example:
            file_path = 'abfss://workspaceid.dfs.core.windows.net/data/Files/file1.csv'
            file_format = 'csv'
            dataframe = read_file(file_path, file_format)
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

    def read_files_concurrently(
        self, 
        file_paths: list, 
        file_format: str = 'csv'
    ) -> list:
        """
        Reads multiple files from the storage account concurrently.

        :param file_paths: A list of file paths.
        :param file_format: The format of the files. Can be 'csv', 'json', 'parquet', etc.

        :return: A list of DataFrames read from the files.

        Raises:
            Exception: If any error occurs during file reading.

        This function performs the following steps:
        1. Defines a nested function to read a single file.
        2. Determines the maximum number of worker threads based on the number of file paths and available CPUs.
        3. Uses ThreadPoolExecutor to read files concurrently.
        4. Submits tasks to the executor for each file path.
        5. Processes the results as they complete.
        6. Collects the DataFrames from the completed tasks.
        7. Logs any errors that occur during file reading.

        Example:
            file_paths = [
                'abfss://workspaceid.dfs.core.windows.net/data/Files/file1.csv',
                'abfss://workspaceid.dfs.core.windows.net/data/Files/file2.csv'
            ]
            file_format = 'csv'
            dataframes = read_files_concurrently(file_paths, file_format)
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

    def write_file(
        self, 
        dataframe: DataFrame, 
        file_name: str, 
        storage_container_endpoint: str, 
        file_format: str = 'parquet'
    ):
        """
        Writes a DataFrame to a file in the storage account.

        :param dataframe: The DataFrame to write.
        :param file_name: The name of the file to write.
        :param storage_container_endpoint: The endpoint of the storage container.
        :param file_format: The format of the file. Can be 'csv', 'json', 'parquet', etc.

        Raises:
            ValueError: If an invalid file format is provided.
            Exception: If any error occurs during file writing.

        This function performs the following steps:
        1. Checks the file format and prepares the DataFrame accordingly:
            - For CSV and JSON formats, converts timestamp fields to strings to avoid issues during writing.
        2. Temporarily disables Arrow optimization for writing to CSV or JSON formats.
        3. Writes the DataFrame to the specified file format:
            - Converts the Spark DataFrame to a Pandas DataFrame for CSV and JSON formats and writes the file.
            - Directly writes the Spark DataFrame for Parquet format.
        4. Re-enables Arrow optimization after writing the file.
        5. Logs and raises any exceptions that occur during the file writing process.

        Example:
            dataframe = df_data
            file_name = 'file1.parquet'
            storage_container_endpoint = 'abfss://workspaceid@onelake.dfs.fabric.microsoft.com/lakehouseid'
            file_format = 'parquet'
            write_file(dataframe, file_name, storage_container_endpoint, file_format)
        """
        try:
            # Write the DataFrame to the specified file format
            if file_format.lower() == 'csv':
                # Convert timestamp fields to string to avoid issues with writing to CSV
                for field in dataframe.schema.fields:
                    if isinstance(field.dataType, TimestampType):
                        dataframe = dataframe.withColumn(field.name, dataframe[field.name].cast('string'))
                
                # Convert spark dataframe to pandas dataframe and write to CSV
                dataframe.toPandas().to_csv(f"{storage_container_endpoint}/Files/{file_name}.csv", index=False)

            elif file_format.lower() == 'json':
                # Convert timestamp fields to string to avoid issues with writing to JSON
                for field in dataframe.schema.fields:
                    if isinstance(field.dataType, TimestampType):
                        dataframe = dataframe.withColumn(field.name, dataframe[field.name].cast('string'))

                # Convert spark dataframe to pandas dataframe and write to JSON
                dataframe.toPandas().to_json(f"{storage_container_endpoint}/Files/{file_name}.json", orient='records', lines=True)

            elif file_format.lower() == 'parquet':
                # Convert spark dataframe to pandas dataframe and write to parquet
                dataframe.write.parquet(f"{storage_container_endpoint}/Files/{file_name}.parquet")

            else:
                raise ValueError(f"Invalid file format: {file_format}")
        except Exception as e:
            self.logger.error(f"An error occurred in write_file_to_storage: {e}")
            raise

    def write_files_concurrently(
        self, 
        dataframe_list: List[DataFrame], 
        file_names: List[str], 
        storage_container_endpoint: str, 
        file_format: str = 'parquet'
    ):
        """
        Writes multiple DataFrames to files in the storage account concurrently.

        :param dataframe_list: A list of DataFrames to write.
        :param file_names: A list of file names.
        :param storage_container_endpoint: The storage container endpoint.
        :param file_format: The format of the files. Can be 'csv', 'json', 'parquet', etc.

        :return: None. The function writes the DataFrames to files.

        Raises:
            ValueError: If the lengths of dataframe_list and file_names do not match.
            Exception: If any error occurs during file writing.

        This function performs the following steps:
        1. Defines a nested function to write a single DataFrame to a file.
        2. Determines the maximum number of worker threads based on the number of DataFrames and available CPUs.
        3. Uses ThreadPoolExecutor to write files concurrently.
        4. Submits tasks to the executor for each DataFrame and file name pair.
        5. Processes the results as they complete.
        6. Logs any errors that occur during file writing.

        Example:
            dataframe_list = [
                spark.createDataFrame([(1, "John", "Doe"), (2, "Jane", "Doe")], ["ID", "First Name", "Last Name"]),
                spark.createDataFrame([(3, "Jim", "Smith"), (4, "Jill", "Smith")], ["ID", "First Name", "Last Name"])
            ]
            file_names = ['file1.parquet', 'file2.parquet']
            storage_container_endpoint = 'abfss://workspaceid@onelake.dfs.fabric.microsoft.com/lakehouseid'
            file_format = 'parquet'
            write_files_concurrently(dataframe_list, file_names, storage_container_endpoint, file_format)
        """
        # Function to be executed in parallel
        def write_file(dataframe, file_name):
            self.write_file(dataframe, file_name, storage_container_endpoint, file_format)

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