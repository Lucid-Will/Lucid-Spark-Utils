#!/usr/bin/env python
# coding: utf-8

# ## Lucid_LoadValidationManager
# 
# 
# 

# In[ ]:


import logging
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import datetime

class Validation:
    """
    This class is used for validating data in Spark DataFrames.
    It includes methods for logging table validation results.
    """

    def __init__(self, spark, logger, transform_manager = None):
        """
        Initialize the Validation object.

        :param transform_manager: TransformManager object
        """
        self.spark = spark
        self.logger = logger
        self.transformer = transform_manager

    def log_table_validation(self, df_stage, target_table, target_filter=None):
        """
        Log the validation results for a table.

        :param df_stage: The staging DataFrame
        :param target_table: The name of the target table
        :param target_filter: The filter condition for the target table
        """
        try:
            # Get current timestamp
            current_ts = lit(datetime.datetime.now())

            # Count rows in staging DataFrame and target table
            staging_count = df_stage.count()
            target_count = self.spark.table(target_table).filter(target_filter).count() if target_filter else self.spark.table(target_table).count()

            # Define logging schema
            schema = StructType([
                StructField("table_name", StringType(), True),
                StructField("staging_count", IntegerType(), True),
                StructField("target_count", IntegerType(), True)
            ])

            # Create DataFrame for logging
            df_validation = self.spark.createDataFrame([(target_table, staging_count, target_count)], schema)

            # Add timestamp column
            df_validation = df_validation.withColumn("log_timestamp", current_ts)

            # Define target table name for logging
            log_target_table = "Control.dimensional_model_table_validation"

            # Define match keys
            log_match_key_columns = ['log_timestamp', 'table_name']

            # Define key column
            nkey_column = "validation_key"

            # Transform table
            df_transformed = self.transformer.stage_dataframe_with_natural_key(df_validation, df_validation.columns, nkey_column, log_match_key_columns)

            # Write to log table
            df_transformed.write.format("delta").mode("append").saveAsTable(log_target_table)

            self.logger.info(f"Validation log for table {target_table} has been successfully created.")
        except Exception as e:
            self.logger.error(f"Failed to create validation log for table {target_table}. Error: {str(e)}")

