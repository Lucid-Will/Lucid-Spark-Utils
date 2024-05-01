from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql.functions import col, concat_ws, abs, hash, current_timestamp
from pyspark.sql import DataFrame
import logging
import os


class DataFrameTransformationManager:
    def __init__(self, spark):
        
        self.logger = logging
        self.spark = spark

    def stage_dataframe_with_key(self, dataframe: DataFrame, columns: list, new_column: str = None, match_key_columns: list = None, include_timestamp: bool = False) -> DataFrame:
        """
        Transforms a DataFrame by adding a new column with an integer hash based on specified key columns.
        Optionally includes the current timestamp in the hash calculation to ensure uniqueness over time.

        :param dataframe: The source DataFrame.
        :param columns: List of column names to include in the transformation.
        :param new_column: The name of the new column to be added with the hash.
        :param match_key_columns: List of columns to use for hash generation.
        :param include_timestamp: Flag to include a timestamp in the hash generation.

        :return: Transformed DataFrame with the new column added, if specified.

        Example:
            df = spark.createDataFrame([(1, "John", "Doe"), (2, "Jane", "Doe")], ["ID", "First Name", "Last Name"])
            df_transformed = stage_dataframe_with_key(df, ["ID", "First Name"], "hash_key", ["ID", "First Name"], True)
        """
        try:
            # Select specified columns and remove duplicate rows.
            df_transformed = dataframe.select(*columns).distinct()

            if new_column and match_key_columns:
                # Concatenate key columns into a single string, separated by '_'.
                concat_cols = concat_ws('_', *[col(k) for k in match_key_columns])

                # If requested, add the current timestamp to the hash input to ensure uniqueness.
                if include_timestamp:
                    concat_cols = concat_ws('_', concat_cols, current_timestamp())

                # Create a new column with a hash of the concatenated key columns.
                df_transformed = df_transformed.withColumn(new_column, abs(hash(concat_cols)))

            return df_transformed
        except Exception as e:
            # Log the error and re-raise to allow for external handling.
            self.logger.error(f"Failed to transform DataFrame with new key column {new_column}: {e}")
            raise

    def execute_transformations_concurrently(self, transformations: list) -> list:
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