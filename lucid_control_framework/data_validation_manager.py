from lucid_control_framework.dataframe_transformation_manager import DataFrameTransformationManager
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import current_timestamp
from pyspark.sql import DataFrame
from typing import Optional
import logging

class Validation:
    """
    This class is used for validating data in Spark DataFrames.
    It includes methods for logging table validation results.
    """

    def __init__(self, spark, logger=None):
        """
        Initialize the Validation object.

        :param transform_manager: TransformManager object
        """
        self.spark = spark
        self.logger = logger if logger else logging.getLogger(__name__)
        self.transform_manager = DataFrameTransformationManager(self.spark)

    def log_table_validation(self, df_stage: DataFrame, target_table: str, target_filter: Optional[str] = None) -> None:
        """
        Log the validation results for a table.

        :param df_stage: The staging DataFrame
        :param target_table: The name of the target table
        :param target_filter: The filter condition for the target table
        """
        try:
            # Get current timestamp
            current_ts = current_timestamp()

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
            log_target_table = "Control.record_count_validation"

            # Add key to the log DataFrame using transform manager
            df_transformed = self.transform_manager.stage_dataframe_with_keys(log_target_table, df_validation, df_validation.columns, 'record_validation_key')

            # Write to log table
            df_transformed.write.format("delta").mode("append").saveAsTable(log_target_table)

            self.logger.info(f"Validation log for table {target_table} has been successfully created.")
        except Exception as e:
            self.logger.error(f"Failed to create validation log for table {target_table}. Error: {str(e)}")

