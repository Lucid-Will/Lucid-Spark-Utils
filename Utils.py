#!/usr/bin/env python
# coding: utf-8

# ## Lucid_Utils
# 
# 
# 

# In[ ]:


# Run other notebooks to access their defined classes and functions
get_ipython().run_line_magic('run', 'Lucid_SparkSessionManager')
get_ipython().run_line_magic('run', 'Lucid_DeltaTableReader')
get_ipython().run_line_magic('run', 'Lucid_UpsertStrategy')
get_ipython().run_line_magic('run', 'Lucid_DataFrameTransformer')


# In[ ]:


def instantiate_spark_session():
    """
    Function to instantiate spark session.
    """
    spark_manager = SparkSessionManager()


# In[ ]:


def read_delta_tables_concurrently_util(table_names, storage_endpoint=None, read_method='default'):
    """
    Function to read multiple Delta tables concurrently.

    Args:
        table_names (list): A list specifying the names of the Delta tables to read.
        storage_endpoint (str, optional): The storage container endpoint, necessary if using 'abfss' reading method.
        read_method (str, optional): Specifies the reading method, either 'default' or 'abfss'. Defaults to 'default'.
    
    Returns:
        dict: A dictionary mapping each table name to its corresponding DataFrame.
    """
    delta_reader = DeltaTableReader(spark_manager.spark)
    return delta_reader.read_delta_tables_concurrently(table_names, storage_endpoint, read_method)


# In[ ]:


def stage_dataframe_with_surrogate_key_util(dataframe, columns, new_column=None, match_key_columns=None):
    """
    Facade method to apply a surrogate key transformation to a DataFrame.
    
    Args:
        dataframe: The DataFrame to be transformed.
        columns: List of column names to include in the transformation.
        new_column: The name of the new column to be added.
        match_key_columns: Columns used to generate the surrogate key.
    
    Returns:
        Transformed DataFrame with a new surrogate key column.
    """
    transformer = DataFrameTransformer(spark_manager.spark)
    return transformer.stage_dataframe_with_surrogate_key(dataframe, columns, new_column, match_key_columns)

def execute_transformations_concurrently_util(transformations):
    """
    Facade method to execute DataFrame transformations concurrently.
    
    Args:
        transformations: A list of transformation tasks structured as tuples.
    
    Returns:
        List of results from the executed transformation tasks.
    """
    transformer = DataFrameTransformer(spark_manager.spark)
    return transformer.execute_transformations_concurrently(transformations)


# In[ ]:


def upsert_fact_table_util(config, storage_endpoint=None, write_method='default', delete_unmatched=False):
    """
    Facade method for upserting data into a Delta fact table.
    """
    fact_upserter = UpsertToFactTable(spark_manager.spark, logging.getLogger())
    fact_upserter.upsert_to_table(config, storage_endpoint, write_method, delete_unmatched)

def upsert_dimension_table_util(config, storage_endpoint=None, write_method='default', delete_unmatched=False):
    """
    Facade method for upserting data into a Delta dimension table supporting SCD2.
    """
    dimension_upserter = UpsertToDimensionTable(spark_manager.spark, logging.getLogger())
    dimension_upserter.upsert_to_table(config, storage_endpoint, write_method, delete_unmatched)

def upsert_generic_util(config, storage_endpoint=None, write_method='default', delete_unmatched=False):
    """
    Facade method for generic upserting data into a Delta table.
    """
    generic_upserter = UpsertGeneric(spark_manager.spark, logging.getLogger())
    generic_upserter.upsert_to_table(config, storage_endpoint, write_method, delete_unmatched)

def upsert_data_concurrently_util(table_configs, storage_endpoint=None, write_method='default', delete_unmatched=False):
    """
    Facade method for concurrently upserting data into Delta tables.
    """
    concurrent_handler = ConcurrentUpsertHandler(spark_manager.spark, logging.getLogger())
    concurrent_handler.upsert_data_concurrently(table_configs, storage_endpoint, write_method, delete_unmatched)

