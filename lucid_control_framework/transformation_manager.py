from lucid_control_framework.delta_table_manager import DeltaTableManager
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql.functions import concat_ws, abs, hash, row_number, lit
from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from pyspark.sql.window import Window
from typing import List, Optional, Callable, Tuple
import logging
import os

class TransformationManager:
    def __init__(self, spark, logger=None):
        
        self.logger = logger if logger else logging.getLogger(__name__)
        self.spark = spark
        self.table_manager = DeltaTableManager(spark, logger)

    def stage_dataframe_with_keys(
        self, 
        target_table: str, 
        dataframe: DataFrame, 
        primary_key_column: Optional[str] = None, 
        composite_key_column: Optional[str] = None, 
        match_key_columns: Optional[List[str]] = None, 
        read_method: str = 'catalog',
        target_table_storage_container_endpoint: Optional[str] = None 
    ) -> Optional[DataFrame]:
        """
        Transforms a DataFrame by adding a new column with an integer hash based on specified key columns.
        It also adds a surrogate key column with values starting from the maximum key in the target table plus one.

        :param target_table: The target table to check for the maximum key.
        :param dataframe: The source DataFrame.
        :param columns: List of column names to include in the transformation.
        :param primary_key_column: The name of the new surrogate key column to be added.
        :param composite_key_column: The name of the new natural key column to be added.
        :param match_key_columns: List of columns to use for hash generation.
        :param read_method: The method to use for reading the target table. Can be either 'path' or 'catalog'.
        :param target_table_storage_container_endpoint: The storage container endpoint for the target table.

        :return: Transformed DataFrame with the new columns added, if specified.

        Example:
            df = spark.createDataFrame([(1, "John", "Doe"), (2, "Jane", "Doe")], ["ID", "First Name", "Last Name"])
            df_transformed = stage_dataframe_with_keys("target_table", df, ["ID", "First Name"], "skey", "nkey", ["ID", "First Name"])
        """
        try:
            # Get the column names from the dataframe
            columns = dataframe.columns
            
            # If a new column is not specified, return the dataframe
            if not primary_key_column:
                return dataframe.select(*columns)
            
            # Initialize max_skey to 0
            max_key = 0

            # Select the specified columns
            df_transformed = dataframe.select(*columns)

            if match_key_columns:
                # Generate an integer hash based on the match_key_columns and add it as the new column
                concat_cols = concat_ws('_', *[df_transformed[col] for col in match_key_columns])
                df_transformed = df_transformed.withColumn(composite_key_column, abs(hash(concat_cols))).select(composite_key_column, *columns)
            
            # Attempt to access the Delta table
            try:
                if read_method == 'catalog':
                    DeltaTable.forName(self.spark, target_table)
                elif read_method == 'path':
                    target_table_path = f"{target_table_storage_container_endpoint}/Tables/{target_table}"
                    DeltaTable.forPath(self.spark, target_table_path)
                else:
                    raise ValueError(f'Invalid method: {read_method}')

                # If table exists retrieve max key
                if read_method == 'catalog':
                    max_key = self.spark.sql(f"SELECT MAX({primary_key_column}) FROM {target_table}").first()[0]
                elif read_method == 'path':
                    max_key = self.spark.sql(f"SELECT MAX({primary_key_column}) FROM delta.`{target_table_path}`").first()[0]
                else:
                    raise ValueError(f'Invalid method: {read_method}')

                # If table isn't empty use max key value
                if max_key is not None:
                    max_key = max_key

            except Exception as e:
                self.logger.info('Table does not exist or is empty, starting key from 0.')

            # Create a window specification
            window_spec = Window.orderBy(lit(1))

            # Increment max key by 1 and add it as the surrogate key column
            df_transformed = df_transformed.withColumn(primary_key_column, (row_number().over(window_spec) + max_key)).select(primary_key_column, *columns)

            # Return the transformed dataframe
            return df_transformed
        except Exception as e:
            # Log the error message and return None if an error occurs
            self.logger.error(f"An error occurred while transforming the dataframe with columns {columns}, match_key_columns {match_key_columns}, and new column {primary_key_column}: {e}")
            return None

    def execute_transformations_concurrently(
            self,
            transformations: List[Tuple[Callable, Tuple]]
    ) -> List:
        """
        Executes multiple DataFrame transformation tasks concurrently, improving performance on multi-core systems.

        :param transformations: A list of tuples, where each tuple contains a transformation function
                                and its corresponding arguments.

        :return: A list of results from each transformation task, executed concurrently.

        Example:
        transformations = [
            (self.stage_dataframe_with_keys, (target_table_storage_container_endpoint1, target_table1, df1, None, None, ["ID", "First Name"])),
            (self.stage_dataframe_with_keys, (target_table_storage_container_endpoint2, target_table2, df2, None, None, ["ID", "First Name"]))
        ]
        results = self.execute_transformations_concurrently(transformations)
        """
        try:
            # Determine the number of workers based on the number of tasks and available CPU cores.
            max_workers = min(len(transformations), (os.cpu_count() or 1) * 5)

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all transformation tasks to the executor.
                futures = [executor.submit(func, *args) for func, args in transformations]

                # Wait for all futures to complete and gather results.
                results = [future.result() for future in as_completed(futures)]

            return results
        except Exception as e:
            # Log any exceptions that occur during concurrent execution and re-raise.
            self.logger.error(f"Failed to execute concurrent transformations: {e}")
            raise