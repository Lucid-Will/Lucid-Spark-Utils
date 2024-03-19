#!/usr/bin/env python
# coding: utf-8

# ## Lucid_DataFrameTransformer
# 
# 
# 

# In[ ]:


import logging
from pyspark.sql.functions import col, concat_ws, abs, hash, current_timestamp
from concurrent.futures import ThreadPoolExecutor, as_completed


# In[ ]:


class DataFrameTransformer:
    def __init__(self, spark):
        self.logger = logging.getLogger(__name__)
        self.spark = spark

    def stage_dataframe_with_natural_key(self, dataframe, columns, new_column=None, match_key_columns=None):
        """
        This method transforms a dataframe by selecting distinct rows based on the given columns.
        If a new column is specified, it adds the new column with an integer hash based on the match_key_columns.
        """
        try:
            # If a new column is not specified, just return the dataframe with distinct rows
            if not new_column or not match_key_columns:
                return dataframe.select(*columns).distinct()

            # Select the specified columns, remove duplicate rows
            df_transformed = dataframe.select(*columns).distinct()

            # Generate an integer hash based on the match_key_columns and add it as the new column
            concat_cols = concat_ws('_', *[df_transformed[col] for col in match_key_columns])
            df_transformed = df_transformed.withColumn(new_column, abs(hash(concat_cols))).select(new_column, *columns)

            # Return the transformed dataframe
            return df_transformed
        except Exception as e:
            # Print the error message and return None if an error occurs
            self.logger.error(f"An error occurred while transforming the dataframe with columns {columns}, match_key_columns {match_key_columns}, and new column {new_column}: {e}")
            return None
        
    def stage_dataframe_with_surrogate_key(self, dataframe, columns, new_column=None, match_key_columns=None):
        """
        This method transforms a dataframe by selecting distinct rows based on the given columns.
        If a new column is specified, it adds the new column with an integer hash based on the match_key_columns.
        """
        try:
            # If a new column is not specified, just return the dataframe with distinct rows
            if not new_column or not match_key_columns:
                return dataframe.select(*columns).distinct()

            # Select the specified columns, remove duplicate rows
            df_transformed = dataframe.select(*columns).distinct()

            # Add a timestamp column
            df_transformed = df_transformed.withColumn("timestamp", current_timestamp())

            # Generate an integer hash based on the match_key_columns and the timestamp and add it as the new column
            concat_cols = concat_ws('_', *[df_transformed[col] for col in match_key_columns], df_transformed["timestamp"])
            df_transformed = df_transformed.withColumn(new_column, abs(hash(concat_cols))).select(new_column, *columns)

            # Return the transformed dataframe
            return df_transformed
        except Exception as e:
            # Print the error message and return None if an error occurs
            self.logger.error(f"An error occurred while transforming the dataframe with columns {columns}, match_key_columns {match_key_columns}, and new column {new_column}: {e}")
            return None

    def execute_transformations_concurrently(self, transformations):
        """
        Executes a list of DataFrame transformation tasks concurrently. Each task should be 
        defined as a tuple containing a transformation function and its required arguments.

        Args:
            transformations (list): A list of tuples, where each tuple represents a transformation task.
            Example of `transformations` list: 
                [
                    (stage_dataframe_with_surrogate_key, (dataframe, ['col1', 'col2'], 'new_col', ['col1'])),
                    (another_transformation_method, (dataframe, ['col3', 'col4'], 'new_col2', ['col3']))
                ]

        Returns:
            List: A list of results from each transformation task executed.
        """
        try:
            # Create a ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=min(len(table_configs), (os.cpu_count() or 1) * 5)) as executor:
                # Start the transformations and store the futures in a list
                futures = [executor.submit(function, *args) for function, args in transformations]
                
                # Process results as they complete
                for future in as_completed(futures):
                    future.result()

            # Return the results
            return [future.result() for future in futures]

        except Exception as e:
            # Print the error message and return None if an error occurs
            self.logger.error(f"An error occurred while executing the transformations: {e}")
            return None

