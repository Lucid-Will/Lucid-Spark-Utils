from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import DataFrame
from typing import Dict, List, Optional
from pyspark.sql.types import TimestampType
import pandas as pd
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

        Example:
            file_path = 'abfss://workspaceid.dfs.core.windows.net/data/Files/file1.csv'
            file_format = 'csv'
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

        Example:
            file_paths = ['abfss://workspaceid.dfs.core.windows.net/data/Files/file1.csv', 'abfss://workspaceid.dfs.core.windows.net/data/Files/file2.csv']
            file_format = 'csv'
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

    def write_file(self, dataframe: DataFrame, file_name: str, storage_container_endpoint: str, file_format: str = 'parquet'):
        """
        Writes a DataFrame to a file in the storage account.

        :param dataframe: The DataFrame to write.
        :param file_name: The name of the file to write.
        :param storage_container_endpoint: The endpoint of the storage container.
        :param file_format: The format of the file. Can be 'csv', 'json', 'parquet', etc.

        Raises:
            Exception: If any error occurs during file writing.
        
        Example:
            dataframe = df_data
            file_names = 'file1.parquet'
            storage_container_endpoint = 'abfss://workspaceid@onelake.dfs.fabric.microsoft.com/lakehouseid'
            storage_container_endpoint = 'abfss://workspaceid.dfs.core.windows.net/data'
            file_format = 'parquet'
        """
        try:
            # Write the DataFrame to the specified file format
            if file_format.lower() == 'csv':
                # Convert timestamp fields to string to avoid issues with writing to CSV
                for field in dataframe.schema.fields:
                    if isinstance(field.dataType, TimestampType):
                        dataframe = dataframe.withColumn(field.name, dataframe[field.name].cast('string'))
                
                # Temporarily disble Arrow for writing to CSV
                self.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
                
                # Convert spark dataframe to pandas dataframe and write to CSV
                dataframe.toPandas().to_csv(f"{storage_container_endpoint}/Files/{file_name}.csv", index=False)

                # Temporarily disble Arrow for writing to CSV
                self.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
            elif file_format.lower() == 'json':
                # Convert timestamp fields to string to avoid issues with writing to JSON
                for field in dataframe.schema.fields:
                    if isinstance(field.dataType, TimestampType):
                        dataframe = dataframe.withColumn(field.name, dataframe[field.name].cast('string'))

                # Temporarily disble Arrow for writing to CSV
                self.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
                
                # Convert spark dataframe to pandas dataframe and write to JSON
                dataframe.toPandas().to_json(f"{storage_container_endpoint}/Files/{file_name}.json", orient='records', lines=True)

                # Temporarily disble Arrow for writing to CSV
                self.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
            elif file_format.lower() == 'parquet':
                # Temporarily disble Arrow for writing to CSV
                self.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
                
                dataframe.write.parquet(f"{storage_container_endpoint}/Files/{file_name}.parquet")

                # Temporarily disble Arrow for writing to CSV
                self.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
            else:
                raise ValueError(f"Invalid file format: {file_format}")
        except Exception as e:
            self.logger.error(f"An error occurred in write_file_to_storage: {e}")
            raise

    def write_files_concurrently(self, dataframe_list: List[DataFrame], file_names: List[str], storage_container_endpoint: str, file_format: str = 'parquet'):
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

        Example:
            dataframe_list = [spark.createDataFrame([(1, "John", "Doe"), (2, "Jane", "Doe")], ["ID", "First Name", "Last Name"]),
                            spark.createDataFrame([(3, "Jim", "Smith"), (4, "Jill", "Smith")], ["ID", "First Name", "Last Name"])]
            file_names = ['file1.parquet', 'file2.parquet']
            storage_container_endpoint = 'abfss://workspaceid@onelake.dfs.fabric.microsoft.com/lakehouseid'
            storage_container_endpoint = 'abfss://workspaceid.dfs.core.windows.net/data'
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

    def read_delta_table(self, table_name: str, storage_container_endpoint: Optional[str] = None, read_method: str = 'path') -> DataFrame:
        """
        Reads a Delta table into a DataFrame.

        :param table_name: The name of the Delta table.
        :param storage_container_endpoint: The storage container endpoint. Required if read_method is 'path'.
        :param read_method: The method to use for reading the table. Can be either 'path' or 'catalog'.
        :return: The DataFrame representing the Delta table.

        :raises Exception: If there's a problem reading the Delta table.
        :raises ValueError: If an invalid read_method is provided.

        Example:
            table_name = 'my_table'
            storage_container_endpoint = 'abfss://workspaceid@onelake.dfs.fabric.microsoft.com/lakehouseid'
            storage_container_endpoint = 'abfss://workspaceid.dfs.core.windows.net/data'
            read_method = 'default'
            df = delta_table_reader.read_delta_table(table_name, storage_container_endpoint, read_method)
        """
        try:
            # Read Delta table using the specified method
            if read_method == 'catalog':
                try:
                    return self.spark.read.format('delta').table(f"{table_name}")
                except Exception as e:
                    self.logger.error(f"An error occurred in read_delta_table: {e}")
                    raise
            elif read_method == 'path':
                try:
                    return self.spark.read.format('delta').load(f'{storage_container_endpoint}/Tables/{table_name}')
                except Exception as e:
                    self.logger.error(f"An error occurred in read_delta_table: {e}")
                    raise
            else:
                raise ValueError(f"Invalid method: {read_method}")
        except Exception as e:
            self.logger.error(f"An error occurred in read_delta_table: {e}")
            raise

    def read_delta_tables_concurrently(self, table_names: List[str], storage_container_endpoint: Optional[str] = None, read_method: str = 'path') -> Dict[str, DataFrame]:
        """
        Reads multiple Delta tables into DataFrames concurrently.

        :param table_names: A list of Delta table names.
        :param storage_container_endpoint: The storage container endpoint. Required if read_method is 'path'.
        :param read_method: The method to use for reading the table. Can be either 'path' or 'catalog'.
        :return: A dictionary mapping table names to DataFrames.

        :raises Exception: If there's a problem reading the Delta tables.

        Example:
            table_names = ['table1', 'table2']
            storage_container_endpoint = 'abfss://workspaceid@onelake.dfs.fabric.microsoft.com/lakehouseid'
            storage_container_endpoint = 'abfss://workspaceid.dfs.core.windows.net/data'
            read_method = 'path'
            df_tables = delta_table_reader.read_delta_tables_concurrently(table_names, storage_container_endpoint, read_method)
        """
        # Function to be executed in parallel
        def read_table(table_name):
            return table_name, self.read_delta_table(table_name, storage_container_endpoint=storage_container_endpoint, read_method=read_method)

        # Set thread pool size based on the number of tables and available CPUs
        max_workers = min(len(table_names), (os.cpu_count() or 1) * 5)
        
        # Use ThreadPoolExecutor to read tables concurrently
        with ThreadPoolExecutor(max_workers) as executor:
            # Submit tasks to the executor
            future_to_table = {executor.submit(read_table, table_name): table_name for table_name in table_names}

            # Process results as they complete
            df_tables = {}
            for future in as_completed(future_to_table):
                table_name = future_to_table[future]
                try:
                    # Get result (DataFrame) from Future
                    table_name, df = future.result()
                    # Store DataFrame in the result dictionary
                    df_tables[table_name] = df
                except Exception as e:
                    self.logger.error(f"An error occurred while reading table {table_name}: {e}")

        return df_tables