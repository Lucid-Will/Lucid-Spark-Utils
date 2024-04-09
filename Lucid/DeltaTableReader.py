#!/usr/bin/env python
# coding: utf-8

# ## Lucid_DeltaTableReader
# 
# 
# 

# In[ ]:


import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed


# In[ ]:


class DeltaTableReader:
    def __init__(self, spark):
        self.logger = logging.getLogger(__name__)
        self.spark = spark

    def read_delta_table(self, table_name, storage_container_endpoint=None, read_method='default'):
        """
        Reads a Delta table into a DataFrame.

        Args:
            table_name (str): The name of the Delta table.
            method (str): The method to use for reading the table. Can be either 'default' or 'abfss'.

        Returns:
            DataFrame: The DataFrame representing the Delta table.
        """

        if read_method == 'default':
            try:
                return self.spark.read.format("delta").table(f"{table_name}")
            except Exception as e:
                self.logger.error(f"An error occurred in read_delta_table: {e}")
                raise
        elif read_method == 'abfss':
            try:
                return self.spark.read.format("delta").load(f"{storage_container_endpoint}/{table_name}")
            except Exception as e:
                self.logger.error(f"An error occurred in read_delta_table: {e}")
                raise
        else:
            raise ValueError(f"Invalid method: {read_method}")

    def read_delta_tables_concurrently(self, table_names, storage_container_endpoint=None, read_method='default'):
        """
        Reads multiple Delta tables into DataFrames concurrently.

        Args:
            table_names (list): A list of Delta table names.
            storage_container_endpoint (str): The storage container endpoint. Required if read_method is 'abfss'.
            read_method (str): The method to use for reading the table. Can be either 'default' or 'abfss'.

        Returns:
            dict: A dictionary mapping table names to DataFrames.
        """

        # Function to be executed in parallel
        def read_table(table_name):
            return table_name, self.read_delta_table(table_name, storage_container_endpoint=storage_container_endpoint, read_method=read_method)

        # Use ThreadPoolExecutor to read tables concurrently
        with ThreadPoolExecutor(max_workers=min(len(table_names), (os.cpu_count() or 1) * 5)) as executor:
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

