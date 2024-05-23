from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql.functions import concat_ws, abs, hash
from pyspark.sql import DataFrame
from typing import List, Optional, Callable, Tuple
import logging
import os

class DataFrameTransformationManager:
    def __init__(self, spark, logger=None):
        
        self.logger = logger if logger else logging.getLogger(__name__)
        self.spark = spark

    def stage_dataframe_with_keys(self, target_table: str, dataframe: DataFrame, columns: List[str], skey_column: Optional[str] = None, nkey_column: Optional[str] = None, match_key_columns: Optional[List[str]] = None) -> Optional[DataFrame]:
        """
        Transforms a DataFrame by adding a new column with an integer hash based on specified key columns.
        It also adds a surrogate key column with values starting from the maximum key in the target table plus one.

        :param target_table: The target table to check for the maximum key.
        :param dataframe: The source DataFrame.
        :param columns: List of column names to include in the transformation.
        :param skey_column: The name of the new surrogate key column to be added.
        :param nkey_column: The name of the new natural key column to be added.
        :param match_key_columns: List of columns to use for hash generation.

        :return: Transformed DataFrame with the new columns added, if specified.

        Example:
            df = spark.createDataFrame([(1, "John", "Doe"), (2, "Jane", "Doe")], ["ID", "First Name", "Last Name"])
            df_transformed = stage_dataframe_with_keys("target_table", df, ["ID", "First Name"], "skey", "nkey", ["ID", "First Name"])
        """
        try:
            # Select the specified columns
            df_transformed = dataframe.select(*columns)
            
            # If table exists retrieve max key
            max_key = self.spark.sql(f"""
                    SELECT IF(
                        EXISTS (SELECT 1 FROM {target_table}),
                        (SELECT MAX({skey_column}) FROM {target_table}),
                        0
                    )
                """).collect()[0][0]
            
            if match_key_columns:
                # Generate an integer hash based on the match_key_columns and add it as the natural key column
                concat_cols = concat_ws('_', *[df_transformed[col] for col in match_key_columns])
                df_transformed = df_transformed.withColumn(nkey_column, abs(hash(concat_cols))).select(nkey_column, *columns)

            # Increment max key by 1 and add it as the surrogate key column
            df_transformed = df_transformed.withColumn(skey_column, max_key + 1).select(skey_column, *columns)

            # Return the transformed dataframe
            return df_transformed
        except Exception as e:
            # Log the error message and return None if an error occurs
            self.logger.error(f"An error occurred while transforming the dataframe with columns {columns}, match_key_columns {match_key_columns}, and new column {new_column}: {e}")
            return None

    def execute_transformations_concurrently(self, transformations: List[Tuple[Callable, Tuple]]) -> List:
        """
        Executes multiple DataFrame transformation tasks concurrently, improving performance on multi-core systems.

        :param transformations: A list of tuples, where each tuple contains a transformation function
                                and its corresponding arguments.

        :return: A list of results from each transformation task, executed concurrently.

        Example:
            df1 = spark.createDataFrame([(1, "John", "Doe"), (2, "Jane", "Doe")], ["ID", "First Name", "Last Name"])
            df2 = spark.createDataFrame([(3, "Jim", "Smith"), (4, "Jill", "Smith")], ["ID", "First Name", "Last Name"])
            transformations = [(self.stage_dataframe_with_key, (df1, ["ID", "First Name"], "hash_key1", ["ID", "First Name"], False)),
                               (self.stage_dataframe_with_key, (df2, ["ID", "First Name"], "hash_key2", ["ID", "First Name"], True))]
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