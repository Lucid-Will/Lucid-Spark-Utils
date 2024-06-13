from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import DataFrame
from typing import Dict, List, Optional
import logging
import os

class DeltaTableManager:
    def __init__(self, spark, logger=None):
        self.logger = logger if logger else logging.getLogger(__name__)
        self.spark = spark

    def read_delta_table(
        self, 
        table_name: str, 
        storage_container_endpoint: Optional[str] = None, 
        read_method: str = 'catalog'
    ) -> DataFrame:
        """
        Reads a Delta table into a DataFrame.

        :param table_name: The name of the Delta table.
        :param storage_container_endpoint: The storage container endpoint. Required if read_method is 'path'.
        :param read_method: The method to use for reading the table. Can be either 'path' or 'catalog'.

        :return: The DataFrame representing the Delta table.

        :raises Exception: If there's a problem reading the Delta table.
        :raises ValueError: If an invalid read_method is provided.

        This function performs the following steps:
        1. Checks the read method and reads the Delta table accordingly:
            - For 'catalog' read method, reads the table using the catalog method.
            - For 'path' read method, reads the table from the specified storage container endpoint.
        2. Raises a ValueError if an invalid read method is provided.
        3. Logs and raises any exceptions that occur during the Delta table reading process.

        Example:
            table_name = 'my_table'
            storage_container_endpoint = 'abfss://workspaceid@onelake.dfs.fabric.microsoft.com/lakehouseid'
            read_method = 'path'
            df = delta_table_reader.read_delta_table(table_name, storage_container_endpoint, read_method)
        """
        try:
            # Read Delta table using the specified method
            if read_method == 'catalog':
                try:
                    return self.spark.read.format('delta').table(f"{table_name}")
                except Exception as e:
                    self.logger.error(f'An error occurred in read_delta_table: {e}')
                    raise
            elif read_method == 'path':
                try:
                    return self.spark.read.format('delta').load(f'{storage_container_endpoint}/Tables/{table_name}')
                except Exception as e:
                    self.logger.error(f'An error occurred in read_delta_table: {e}')
                    raise
            else:
                raise ValueError(f'Invalid method: {read_method}')
        except Exception as e:
            self.logger.error(f'An error occurred in read_delta_table: {e}')
            raise

    def read_delta_tables_concurrently(
        self, 
        table_names: List[str], 
        storage_container_endpoint: Optional[str] = None, 
        read_method: str = 'catalog'
    ) -> Dict[str, DataFrame]:
        """
        Reads multiple Delta tables into DataFrames concurrently.

        :param table_names: A list of Delta table names.
        :param storage_container_endpoint: The storage container endpoint. Required if read_method is 'path'.
        :param read_method: The method to use for reading the table. Can be either 'path' or 'catalog'.

        :return: A dictionary mapping table names to DataFrames.

        :raises Exception: If there's a problem reading the Delta tables.

        This function performs the following steps:
        1. Defines a nested function to read a single Delta table.
        2. Determines the maximum number of worker threads based on the number of table names and available CPUs.
        3. Uses ThreadPoolExecutor to read tables concurrently.
        4. Submits tasks to the executor for each table name.
        5. Processes the results as they complete.
        6. Collects the DataFrames from the completed tasks and stores them in a dictionary.
        7. Logs any errors that occur during table reading.

        Example:
            table_names = ['table1', 'table2']
            storage_container_endpoint = 'abfss://workspaceid@onelake.dfs.fabric.microsoft.com/lakehouseid'
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
    
    def write_delta_table(
        self, 
        dataframe: DataFrame, 
        table_name: str, 
        storage_container_endpoint: Optional[str] = None, 
        write_method: str = 'catalog', 
        write_mode: str = 'overwrite', 
        merge_schema: str = 'true'
    ) -> None:
        """
        Writes a DataFrame to a Delta table.

        :param dataframe: The DataFrame to write.
        :param table_name: The name of the Delta table.
        :param storage_container_endpoint: The storage container endpoint. Required if write_method is 'path'.
        :param write_method: The method to use for writing the table. Can be either 'path' or 'catalog'.
        :param write_mode: The mode to use for writing the table. Can be 'overwrite', 'append', 'ignore', 'error', or 'overwritePartitions'. Default is 'overwrite'.
        :param merge_schema: Whether to merge the schema of the DataFrame with the schema of the Delta table. Default is 'true'.

        :raises Exception: If there's a problem writing the Delta table.
        :raises ValueError: If an invalid write_method is provided or if storage_container_endpoint is not provided when using 'path' write method.

        This function performs the following steps:
        1. Checks the write method and writes the DataFrame to the Delta table accordingly:
            - For 'catalog' write method, writes the table using the catalog method.
            - For 'path' write method, writes the table to the specified storage container endpoint.
        2. Raises a ValueError if the storage_container_endpoint is not provided when using the 'path' write method.
        3. Sets the write mode and merge schema options for the Delta table write operation.
        4. Logs and raises any exceptions that occur during the Delta table writing process.

        Example:
            dataframe = df_data
            table_name = 'my_table'
            storage_container_endpoint = 'abfss://workspaceid.dfs.core.windows.net/data'
            write_method = 'path'
            write_mode = 'overwrite'
            merge_schema = 'true'
            delta_table_writer.write_delta_table(dataframe, table_name, storage_container_endpoint, write_method)
        """
        try:
            # Write Delta table using the specified method
            if write_method == 'catalog':
                try:
                    dataframe.write.format('delta').mode(write_mode).option('mergeSchema', merge_schema.lower() == 'true').saveAsTable(f"{table_name}")
                except Exception as e:
                    self.logger.error(f"An error occurred in write_delta_table: {e}")
                    raise
            elif write_method == 'path':
                if not storage_container_endpoint:
                        raise ValueError("Storage container endpoint must be provided when using 'path' write method.")
                try:
                    dataframe.write.format('delta').mode(write_mode).option('mergeSchema', merge_schema.lower() == 'true').save(f'{storage_container_endpoint}/Tables/{table_name}')
                except Exception as e:
                    self.logger.error(f"An error occurred in write_delta_table: {e}")
                    raise
            else:
                raise ValueError(f"Invalid method: {write_method}")
        except Exception as e:
            self.logger.error(f"An error occurred in write_delta_table: {e}")
            raise