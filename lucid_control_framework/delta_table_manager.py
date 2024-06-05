from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import DataFrame
from typing import Dict, List, Optional
import logging
import os

class DeltaTableReader:
    def __init__(self, spark, logger=None):
        self.logger = logger if logger else logging.getLogger(__name__)
        self.spark = spark

    def read_delta_table(self, table_name: str, storage_container_endpoint: Optional[str] = None, read_method: str = 'default') -> DataFrame:
        """
        Reads a Delta table into a DataFrame.

        :param table_name: The name of the Delta table.
        :param storage_container_endpoint: The storage container endpoint. Required if read_method is 'abfss'.
        :param read_method: The method to use for reading the table. Can be either 'default' or 'abfss'.
        :return: The DataFrame representing the Delta table.

        :raises Exception: If there's a problem reading the Delta table.
        :raises ValueError: If an invalid read_method is provided.

        Example:
            table_name = 'my_table'
            storage_container_endpoint = 'https://storageaccount.blob.core.windows.net/container'
            read_method = 'default'
            df = delta_table_reader.read_delta_table(table_name, storage_container_endpoint, read_method)
        """
        
        try:
            # Read Delta table using the specified method
            if read_method == 'default':
                try:
                    return self.spark.read.format('delta').table(f"{table_name}")
                except Exception as e:
                    self.logger.error(f"An error occurred in read_delta_table: {e}")
                    raise
            elif read_method == 'abfss':
                try:
                    return self.spark.read.format('delta').load(f'{storage_container_endpoint}/{table_name}')
                except Exception as e:
                    self.logger.error(f"An error occurred in read_delta_table: {e}")
                    raise
            else:
                raise ValueError(f"Invalid method: {read_method}")
        except Exception as e:
            self.logger.error(f"An error occurred in read_delta_table: {e}")
            raise

    def read_delta_tables_concurrently(self, table_names: List[str], storage_container_endpoint: Optional[str] = None, read_method: str = 'default') -> Dict[str, DataFrame]:
        """
        Reads multiple Delta tables into DataFrames concurrently.

        :param table_names: A list of Delta table names.
        :param storage_container_endpoint: The storage container endpoint. Required if read_method is 'abfss'.
        :param read_method: The method to use for reading the table. Can be either 'default' or 'abfss'.
        :return: A dictionary mapping table names to DataFrames.

        :raises Exception: If there's a problem reading the Delta tables.

        Example:
            table_names = ['table1', 'table2']
            storage_container_endpoint = 'https://storageaccount.blob.core.windows.net/container'
            read_method = 'default'
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

